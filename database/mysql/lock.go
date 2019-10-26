package mysql

import (
	"context"
	"database/sql"
	"github.com/pkg/errors"
	"log"
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

type mysqlLockManipulator struct {
	agentName     string
	db            *sql.DB
	conn          *sql.Conn
	lockerConnId  int
	hbInterval    time.Duration
	waitInterval  time.Duration // int64(HbInterval) * 1500 * 1000 * 1000)
	lockersMutex  sync.RWMutex
	lockers       map[string]struct{}
	chordCallback func(err error)
}

func (m *mysqlLockManipulator) checkConnectionIsAlive(ctx context.Context) bool {
	err := m.conn.PingContext(ctx)
	if err != nil {
		m.chordCallback(errors.WithStack(err))
		return false
	}
	return true
}

func (m *mysqlLockManipulator) cacheLocker(ctx context.Context, key string) {
	m.lockersMutex.Lock()
	m.lockers[key] = struct{}{}
	m.lockersMutex.Unlock()
}

func (m *mysqlLockManipulator) registerLock(ctx context.Context, key string) error {
	_, err := m.conn.ExecContext(ctx, "INSERT lock_timer (`key`, `agent_name`, `last_seen`) VALUES(?, ?, NOW())", key, m.agentName)
	if IsKeyDuplicationError(err) {
		log.Printf("unreachable error:%v", err)
		return err
	}
	m.cacheLocker(ctx, key)
	return nil
}

func (m *mysqlLockManipulator) getLockTimerRecord(ctx context.Context, key string) (*mysqlLockTimer, error) {
	lockTimer := &mysqlLockTimer{Key: key}
	querySql := "SELECT agent_name, last_seen FROM lock_timer WHERE `key` = ?"
	err := m.conn.QueryRowContext(ctx, querySql, key).Scan(&lockTimer.AgentName, &lockTimer.LastSeen)
	if hasErr, err := CheckNoRowsError(err); hasErr {
		return nil, err
	}
	return lockTimer, nil
}

func (m *mysqlLockManipulator) takeoverLock(ctx context.Context, key string) error {
	upsertSql := "INSERT INTO lock_timer (`key`, agent_name, last_seen) VALUES (?, ?, NOW()) ON DUPLICATE KEY UPDATE last_seen = NOW(), agent_name = ?"
	_, err := m.conn.ExecContext(ctx, upsertSql, key, m.agentName, m.agentName)
	if err != nil {
		return err
	}

	m.cacheLocker(ctx, key)
	return err
}

func (m *mysqlLockManipulator) releaseSchemaLock(ctx context.Context, key string) error {
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

func (m *mysqlLockManipulator) ReleaseLock(ctx context.Context, key string) error {
	//TODO log, retry
	if err := m.releaseSchemaLock(ctx, key); err != nil {
		return err
	}

	_, err := m.conn.ExecContext(ctx, "DELETE FROM lock_timer where `key` = ? and agent_name = ?", key, m.agentName)
	if err != nil {
		return err
	}
	return nil
}

func (m *mysqlLockManipulator) checkSchemeLockWhetherIsOwned(ctx context.Context, key string) (bool, error) {
	var connId sql.NullInt32
	err := m.conn.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?) as conn_id", key).Scan(&connId)
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

func (m *mysqlLockManipulator) doLockRequest(ctx context.Context, key string) (bool, error) {
	var flag sql.NullInt32
	log.Printf("db get_lock(%s, 0)\n", key)
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
func (m *mysqlLockManipulator) AcquireLock(ctx context.Context, key string) (bool, error) {

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
				log.Printf("release lock(%s) failed: %v", key, releaseErr)
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
			log.Printf("register lock(%s) error: %v", key, err)
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
		log.Printf("get lock meta(%s) error: %v", key, err)
		return false, err
	}
	//lock timer wouldn't be null

	expired := lockTimerLater.LastSeen.Equal(lockTimer.LastSeen)
	if expired {
		// another executor has gone, we can take over the queue
		if err = m.takeoverLock(ctx, key); err != nil {
			log.Printf("takeover lock(%s) error: %v", key, err)
			return false, err
		}
		return true, nil
	} else {
		// another executor is apparent death, we should take over the lock meta.
		return false, nil
	}
}

func (m *mysqlLockManipulator) checkLockerConnAndHeartbeat(ctx context.Context) error {
	ticker := time.NewTicker(m.hbInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !m.checkConnectionIsAlive(ctx) {
				return nil
			}

			m.lockersMutex.RLock()
			lockerKeys := make([]string, 0)
			for key := range m.lockers {
				lockerKeys = append(lockerKeys, key)
			}
			m.lockersMutex.RUnlock()

			//XXX maybe we should use another db connection
			hbSql := "UPDATE lock_timer SET last_seen=NOW() WHERE `key` = ?"
			stmt, err := m.conn.PrepareContext(ctx, hbSql)
			if err != nil {
				return err
			}
			//TODO batch processing
			for _, key := range lockerKeys {
				_, err := stmt.ExecContext(ctx, key)
				if err != nil {
					log.Printf("renew last_seen for:%s error: %v", key, err)
				}
			}

		case <-ctx.Done():
			return nil
		}
	}
}

func (m *mysqlLockManipulator) Bootstrap(ctx context.Context, chordCallback func(err error)) error {
	m.chordCallback = chordCallback
	go func() {
		err := m.checkLockerConnAndHeartbeat(ctx)
		if err != nil {
			log.Printf("locker checking exit accidentally! error:%v", err)
		}
	}()
	return nil
}

func BuildMySQLLockManipulator(db *sql.DB, agentName string, hbInterval time.Duration) (*mysqlLockManipulator, error) {

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

	return &mysqlLockManipulator{
		agentName:    agentName,
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
