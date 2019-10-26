package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"git.code.oa.com/tke/theflow/database"
	"git.code.oa.com/tke/theflow/util"
)

type mysqlStore struct {
	db *sql.DB
}

func (ms *mysqlStore) LoadUncommittedJobEvents(ctx context.Context) ([]*database.JobMetaObject, error) {
	querySql := "SELECT `id`, `uuid`, `userid`, `class`, `data` FROM job_event WHERE `deleted_at` is NULL"
	rows, err := ms.db.QueryContext(ctx, querySql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]*database.JobMetaObject, 0)
	for rows.Next() {
		obj := &database.JobMetaObject{}
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

	querySql := "SELECT id, storage, status, state, running_cnt FROM flow WHERE event_uuid = ?"
	err = tx.QueryRowContext(ctx, querySql, data.EventUUID).Scan(&obj.ID, &obj.Storage, &obj.Status, &obj.State, &obj.RunningCnt)
	if IsNoRowsError(err) {
		var id int64
		id, err = createPendingJobFlow(ctx, tx, &data)
		if err != nil {
			return
		}
		obj.ID = id
	}

	err = tx.Commit()
	return
}

func createPendingJobFlow(ctx context.Context, tx *sql.Tx, data *database.FlowDataPartial) (int64, error) {
	//XXX state is empty now, executor will initialize it.
	createSql := "INSERT INTO flow (event_uuid, userid, class, status, storage, state) VALUES (?, ?, ?, ?, ?, ?)"
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
	_, err = createPendingJobFlow(ctx, tx, &data)
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

func (ms *mysqlStore) CreateJobEvent(ctx context.Context, meta *database.JobMetaObject) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	insertSql := "INSERT INTO job_event (uuid, userid, class, data) VALUES (?, ?, ?, ?)"

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

func (ms *mysqlStore) DeleteJobEvent(ctx context.Context, uuid string) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	deleteSql := "DELETE FROM job_event WHERE uuid = ?"

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

func (ms *mysqlStore) SetFlowStartTime(ctx context.Context, flowId int64) error {
	updateSql := "update flow set `started_at` = now where"
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, updateSql, flowId)
	if err != nil {
		return err
	}
	return tx.Commit()
}

var _ database.RuntimeStore = &mysqlStore{}

func BuildRuntimeStore(db *sql.DB) *mysqlStore {
	store := &mysqlStore{db: db}
	return store
}
