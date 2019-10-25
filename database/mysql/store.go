package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"git.code.oa.com/tke/theflow/database"
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

	querySql := "SELECT id, storage, status, state FROM flow WHERE event_uuid = ?"
	err = tx.QueryRowContext(ctx, querySql, data.EventUUID).Scan(&obj.ID, &obj.Storage, &obj.Status, &obj.State)
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
	createSql := "INSERT INTO flow (event_uuid, userid, class, status, storage, state) VALUES (?, ?, ?, ?, ?, '')"
	result, err := tx.ExecContext(ctx, createSql, data.EventUUID, data.UserID, data.Class, data.Status, data.Storage)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (ms *mysqlStore) CreatePendingFlow(ctx context.Context, dbJobMeta database.JobMetaObject, status database.TypeStatusRaw) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	data := database.FlowDataPartial{
		EventUUID: dbJobMeta.UUID,
		UserID:    dbJobMeta.UserID,
		Class:     dbJobMeta.Class,
		Status:    status,
		Storage:   dbJobMeta.Data,
	}
	_, err = createPendingJobFlow(ctx, tx, &data)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (ms *mysqlStore) UpdateFlow(ctx context.Context, obj database.FlowDataObject, agentName string) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	updateSql := "UPDATE flow SET `status` = ?, `state` = ?, `storage` = ?, `agent_name` = ? WHERE id = ?"
	_, err = tx.ExecContext(ctx, updateSql, obj.Status, obj.State, obj.Storage, agentName, obj.ID)
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

func (ms *mysqlStore) SaveFlowTask(ctx context.Context, flowId int64, taskName string, status database.TypeStatusRaw) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	//TODO update status, error_msg, ended_at
	upsertSql := "INSERT INTO flow_task (flow_id, name, status) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE status=? "
	_, err = tx.ExecContext(ctx, upsertSql, flowId, taskName, status, status)
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

var _ database.RuntimeStore = &mysqlStore{}

func BuildRuntimeStore(db *sql.DB) *mysqlStore {
	store := &mysqlStore{db: db}
	return store
}
