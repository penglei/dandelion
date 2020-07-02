package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/penglei/dandelion/database"
	"github.com/penglei/dandelion/util"
)

type mysqlStore struct {
	db *sql.DB
}

func (ms *mysqlStore) LoadUncommittedFlowMeta(ctx context.Context) ([]*database.FlowMetaObject, error) {
	querySql := "SELECT `id`, `uuid`, `userid`, `class`, `data` FROM flow_meta WHERE `deleted_at` is NULL ORDER BY `id`"
	rows, err := ms.db.QueryContext(ctx, querySql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]*database.FlowMetaObject, 0)
	for rows.Next() {
		obj := &database.FlowMetaObject{}
		if err := rows.Scan(&obj.ID, &obj.UUID, &obj.UserID, &obj.Class, &obj.Data); err != nil {
			return nil, err
		}
		results = append(results, obj)
	}

	return results, nil
}

func (ms *mysqlStore) GetOrCreateFlow(ctx context.Context, data database.FlowDataPartial) (obj database.FlowDataObject, err error) {
	tx, err := ms.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	obj.FlowDataPartial = data

	querySql := "SELECT id, storage, status, state, running_cnt FROM flow WHERE uuid = ?"
	err = tx.QueryRowContext(ctx, querySql, data.EventUUID).Scan(&obj.ID, &obj.Storage, &obj.Status, &obj.State, &obj.RunningCnt)
	if IsNoRowsError(err) {
		var id int64
		id, err = createPendingFlow(ctx, tx, &data)
		if err != nil {
			return
		}
		obj.ID = id
	}

	err = tx.Commit()
	return
}

func createPendingFlow(ctx context.Context, tx *sql.Tx, data *database.FlowDataPartial) (int64, error) {
	//XXX state is empty now, executor will initialize it.
	createSql := "INSERT INTO flow (uuid, userid, class, status, storage, state) VALUES (?, ?, ?, ?, ?, ?)"
	result, err := tx.ExecContext(ctx, createSql, data.EventUUID, data.UserID, data.Class, data.Status, data.Storage, data.State)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (ms *mysqlStore) CreatePendingFlow(
	ctx context.Context,
	data database.FlowDataPartial,
) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = createPendingFlow(ctx, tx, &data)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (ms *mysqlStore) UpdateFlow(ctx context.Context, obj database.FlowDataObject, agentName string, mask util.BitMask) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	updateFieldsSql := "`status` = ?, `state` = ?, `storage` = ?, `agent_name` = ?"
	sqlArgs := []interface{}{obj.Status, obj.State, obj.Storage, agentName}

	if mask.Has(util.FlowSetStartStat) {
		updateFieldsSql += ", `started_at` = NOW()"
	}

	if mask.Has(util.FlowUpdateRunningCnt) {
		updateFieldsSql += ", `running_cnt` = ?"
		sqlArgs = append(sqlArgs, obj.RunningCnt)
	}

	if mask.Has(util.FlowSetCompleteStat) {
		updateFieldsSql += ", `ended_at` = NOW()"
	}

	updateSql := fmt.Sprintf("UPDATE flow SET %s WHERE id = ?", updateFieldsSql)
	sqlArgs = append(sqlArgs, obj.ID)

	_, err = tx.ExecContext(ctx, updateSql, sqlArgs...)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (ms *mysqlStore) SaveFlowStorage(ctx context.Context, flowId int64, data []byte) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	updateSql := "UPDATE flow SET storage=? WHERE id = ?"
	_, err = tx.ExecContext(ctx, updateSql, data, flowId)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// flowId int64, taskName string, status TypeStatusRaw)
func (ms *mysqlStore) UpsertFlowTask(ctx context.Context, data database.TaskDataObject, opts util.BitMask) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	//{insert
	insertFieldsSql := "`flow_id`, `name`, `status`"
	insertFieldsSqlPlaceHolder := "?, ?, ?"
	sqlArgs := []interface{}{data.FlowID, data.Name, data.Status}

	if opts.Has(util.TaskSetError) {
		insertFieldsSql += ", `error`"
		insertFieldsSqlPlaceHolder += ", ?"
		sqlArgs = append(sqlArgs, data.ErrorMsg)
	}
	//}

	//{update
	updateFieldsSql := "`status`=? "
	sqlArgs = append(sqlArgs, data.Status)

	if opts.Has(util.TaskSetExecuted) {
		updateFieldsSql += ", `started_at` = NOW(), `executed` = 1"
	}

	if opts.Has(util.TaskSetError) {
		updateFieldsSql += ", `error` = ?"
		sqlArgs = append(sqlArgs, data.ErrorMsg)
	}

	if opts.Has(util.TaskSetFinishStat) {
		updateFieldsSql += ", `ended_at` = NOW()"
	}
	//}

	upsertSql := fmt.Sprintf(
		"INSERT INTO flow_task (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		insertFieldsSql,
		insertFieldsSqlPlaceHolder,
		updateFieldsSql,
	)

	_, err = tx.ExecContext(ctx, upsertSql, sqlArgs...)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (ms *mysqlStore) CreateFlowMeta(ctx context.Context, meta *database.FlowMetaObject) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	insertSql := "INSERT INTO flow_meta (uuid, userid, class, data) VALUES (?, ?, ?, ?)"

	result, err := tx.ExecContext(ctx, insertSql, meta.UUID, meta.UserID, meta.Class, meta.Data)
	if err != nil {
		return err
	}

	flowId, err := result.LastInsertId()
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	meta.ID = flowId
	return nil
}

func (ms *mysqlStore) DeleteFlowMeta(ctx context.Context, uuid string) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	deleteSql := "DELETE FROM flow_meta WHERE uuid = ?"

	result, err := tx.ExecContext(ctx, deleteSql, uuid)
	if err != nil {
		return err
	}
	cnt, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if cnt != 1 {
		return fmt.Errorf("delete event but it doesn't exit: %s", uuid)
	}
	return tx.Commit()
}

func (ms *mysqlStore) LoadFlowTasks(ctx context.Context, flowId int64) ([]*database.TaskDataObject, error) {
	querySql := "SELECT `name`, `status`, `executed` FROM flow_task WHERE flow_id = ?"
	rows, err := ms.db.QueryContext(ctx, querySql, flowId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]*database.TaskDataObject, 0)
	for rows.Next() {
		td := &database.TaskDataObject{
			FlowID: flowId,
		}
		var executed int
		if err := rows.Scan(&td.Name, &td.Status, &executed); err != nil {
			return nil, err
		}
		td.Executed = executed == 1
		results = append(results, td)
	}

	return results, nil
}

var _ database.RuntimeStore = &mysqlStore{}

func BuildRuntimeStore(db *sql.DB) *mysqlStore {
	store := &mysqlStore{db: db}
	return store
}
