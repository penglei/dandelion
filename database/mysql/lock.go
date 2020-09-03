package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"sync"
	"time"
)

type mysqlLockTimer struct {
	Key       string
	LastSeen  time.Time
	AgentName string
}

func getConnId(conn *sql.Conn) (int, error) {
	ctx := context.Background()
	var connId int
	err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID() as conn_id").Scan(&connId)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return connId, err
}

//notice: Acquire/Release is not thread-safety
type mysqlLockImpl struct {
	agentName    string
	lg           *zap.Logger
	db           *sql.DB
	conn         *sql.Conn
	lockerConnId int
	hbInterval   time.Duration
	waitInterval time.Duration // int64(HbInterval) * 1500 * 1000 * 1000)
	lockersMutex sync.RWMutex
	lockers      map[string]struct{}
}

func (m *mysqlLockImpl) cacheLocker(ctx context.Context, key string) {
	m.lockersMutex.Lock()
	m.lockers[key] = struct{}{}
	m.lockersMutex.Unlock()
}

func (m *mysqlLockImpl) registerLock(ctx context.Context, key string) error {
	_, err := m.db.ExecContext(ctx, "INSERT lock_timer (`key`, `agent_name`, `last_seen`) VALUES(?, ?, NOW())", key, m.agentName)
	if IsKeyDuplicationError(err) {
		return err
	}
	m.cacheLocker(ctx, key)
	return nil
}

func (m *mysqlLockImpl) getLockTimerRecord(ctx context.Context, key string) (*mysqlLockTimer, error) {
	lockTimer := &mysqlLockTimer{Key: key}
	querySql := "SELECT agent_name, last_seen FROM lock_timer WHERE `key` = ?"
	err := m.db.QueryRowContext(ctx, querySql, key).Scan(&lockTimer.AgentName, &lockTimer.LastSeen)
	if hasErr, err := CheckNoRowsError(err); hasErr {
		return nil, err
	}
	return lockTimer, nil
}

func (m *mysqlLockImpl) takeoverLock(ctx context.Context, key string) error {
	upsertSql := "INSERT INTO lock_timer (`key`, agent_name, last_seen) VALUES (?, ?, NOW()) ON DUPLICATE KEY UPDATE last_seen = NOW(), agent_name = ?"
	_, err := m.db.ExecContext(ctx, upsertSql, key, m.agentName, m.agentName)
	if err != nil {
		return err
	}

	m.cacheLocker(ctx, key)
	return err
}

func (m *mysqlLockImpl) releaseSchemaLock(ctx context.Context, key string) error {
	var r sql.NullInt32
	err := m.conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?) as conn_id", key).Scan(&r)
	if err != nil && !IsNoRowsError(err) {
		return err
	}

	m.lockersMutex.Lock()
	if _, ok := m.lockers[key]; ok {
		delete(m.lockers, key)
	}
	m.lockersMutex.Unlock()
	return nil
}

func (m *mysqlLockImpl) ReleaseLock(ctx context.Context, key string) error {
	if err := m.releaseSchemaLock(ctx, key); err != nil {
		return err
	}

	_, err := m.db.ExecContext(ctx, "DELETE FROM lock_timer where `key` = ? and agent_name = ?", key, m.agentName)
	if err != nil {
		return err
	}
	return nil
}

func (m *mysqlLockImpl) checkSchemeLockWhetherIsOwned(ctx context.Context, key string) (bool, error) {
	var connId sql.NullInt32
	err := m.db.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?) as conn_id", key).Scan(&connId)
	if err != nil && !IsNoRowsError(err) {
		return false, err
	}

	// not null, lock is used
	if connId.Valid {
		if int(connId.Int32) == m.lockerConnId {
			return true, nil
		}
	}

	return false, nil
}

func (m *mysqlLockImpl) doLockRequest(ctx context.Context, key string) (bool, error) {
	var flag sql.NullInt32
	err := m.conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0) as flag", key).Scan(&flag)
	if err != nil && !IsNoRowsError(err) {
		return false, err
	}

	if flag.Valid {
		if flag.Int32 == 1 {
			return true, nil
		}
	}
	return false, nil
}

//acquire lock for the key.
//the method will block the request for a while, caller running in a new goroutine is better.
func (m *mysqlLockImpl) AcquireLock(ctx context.Context, key string) (bool, error) {

	isOwned, err := m.checkSchemeLockWhetherIsOwned(ctx, key)
	if err != nil {
		return false, err
	}

	if isOwned {
		return true, nil
	}
	// if lock is free or owned by others, try get the lock
	locked, err := m.doLockRequest(ctx, key)
	if err != nil {
		return false, err
	}

	if !locked {
		return false, nil
	}

	// ---------- acquire the lock success, register it in lock table ------

	defer func() {
		if err != nil {
			releaseErr := m.releaseSchemaLock(ctx, key)
			if releaseErr != nil {
				m.lg.Error("release lock failed", zap.String("key", key), zap.Error(releaseErr))
			}
		}
	}()

	lockTimer, err := m.getLockTimerRecord(ctx, key)
	if err != nil {
		return false, err
	}

	if lockTimer == nil {
		//we are creating the lock, register it.
		if err = m.registerLock(ctx, key); err != nil {
			return false, err
		}
		return true, nil
	}

	//Maybe we got the lock not too long ago, but connection has lost accidentally.
	//Now we are coming back!
	//It's like reentrant lock
	if lockTimer.AgentName == m.agentName {
		//m.takeoverLock(ctx, key)
		m.cacheLocker(ctx, key)
		return true, nil
	}
	// else

	//It's maybe another executor went offline by accident, we should to wait for a while
	time.Sleep(m.waitInterval)

	lockTimerLater, err := m.getLockTimerRecord(ctx, key)
	if err != nil {
		m.lg.Debug("get lock meta error", zap.String("key", key), zap.Error(err))
		return false, err
	}
	//lock timer wouldn't be null

	expired := lockTimerLater.LastSeen.Equal(lockTimer.LastSeen)
	if expired {
		// another executor has gone, we can take over the queue
		if err = m.takeoverLock(ctx, key); err != nil {
			m.lg.Debug("failed to takeover lock", zap.String("key", key), zap.Error(err))
			return false, err
		}
		return true, nil
	} else {
		// another executor is apparent death, we should take over the lock meta.
		return false, nil
	}
}

func (m *mysqlLockImpl) checkLockConnAndDoHeartbeat(ctx context.Context) error {
	ticker := time.NewTicker(m.hbInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			if err := m.db.PingContext(ctx); err != nil {
				return err
			}
			m.lockersMutex.RLock()
			if len(m.lockers) == 0 {
				m.lockersMutex.RUnlock()
				continue
			}
			lockerKeys := make([]string, 0)
			for key := range m.lockers {
				lockerKeys = append(lockerKeys, key)
			}
			m.lockersMutex.RUnlock()

			return func() error {
				//XXX maybe we should use another db connection
				hbSql := "UPDATE lock_timer SET last_seen=NOW() WHERE `key` = ?"
				stmt, err := m.db.PrepareContext(ctx, hbSql)
				if err != nil {
					return err
				}
				defer stmt.Close()

				//TODO batch processing
				for _, key := range lockerKeys {
					_, err := stmt.ExecContext(ctx, key)
					if err != nil {
						m.lg.Debug("renew lock last_seen", zap.String("key", key), zap.Error(err))
						return err
					}
				}
				return nil
			}()

		case <-ctx.Done():
			return nil
		}
	}
}

func (m *mysqlLockImpl) checkRunningAgain(ctx context.Context) (bool, error) {
	agentInstanceKey := fmt.Sprintf("__flow_agent__:%s", m.agentName)
	locked, err := m.doLockRequest(ctx, agentInstanceKey)
	return !locked, err
}

func (m *mysqlLockImpl) Bootstrap(ctx context.Context, connErrCallback func(err error)) error {

	yes, err := m.checkRunningAgain(ctx)
	if err != nil {
		return err
	}
	if yes {
		return fmt.Errorf("a unique name is required to bootstrap runtime([%s] is in use)", m.agentName)
	}

	go func() {
		err := m.checkLockConnAndDoHeartbeat(ctx)

		if err != nil {
			if err := m.conn.Close(); err != nil {
				m.lg.Error("close lock agent connection error", zap.Error(err))
			}
			m.lg.Warn("locker checking exit accidentally", zap.Error(err))
			connErrCallback(err)
		}
	}()
	return nil
}

func BuildMySQLLockAgent(db *sql.DB, lg *zap.Logger, agentName string, hbInterval time.Duration) (*mysqlLockImpl, error) {

	//XXX how to rebuilt if lock connection closed ?

	ctx := context.Background()
	db.SetConnMaxLifetime(0) //keep connection
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	var lockerConnId int
	if lockerConnId, err = getConnId(conn); err != nil {
		return nil, err
	}

	return &mysqlLockImpl{
		agentName:    agentName,
		lg:           lg,
		db:           db,
		conn:         conn,
		lockerConnId: lockerConnId,
		hbInterval:   hbInterval,
		//report status by saving current time to database every 3 seconds
		//so every lock acquiring should wait more than 3 seconds(is 1.5 times better?) for
		//preventing another executor to be in a state of suspended
		waitInterval: time.Duration(int64(hbInterval * 15 / 10)), //hbInterval * 1.5
		lockersMutex: sync.RWMutex{},
		lockers:      make(map[string]struct{}),
	}, nil
}
