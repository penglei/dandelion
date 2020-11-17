package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/penglei/dandelion/database"
	"github.com/penglei/dandelion/util"
)

type mysqlDatabase struct {
	db *sql.DB
}

func (ms *mysqlDatabase) LoadTriggers(ctx context.Context) ([]*database.ProcessTriggerObject, error) {
	querySQL := "SELECT `id`, `uuid`, `user`, `class`, `data`, `event`" +
		" FROM process_trigger WHERE `deleted_flag` = 0 ORDER BY `id`"
	rows, err := ms.db.QueryContext(ctx, querySQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]*database.ProcessTriggerObject, 0)
	for rows.Next() {
		obj := &database.ProcessTriggerObject{}
		if err := rows.Scan(&obj.ID, &obj.UUID, &obj.User, &obj.Class, &obj.Data, &obj.Event); err != nil {
			return nil, err
		}
		results = append(results, obj)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

//case 1: system crashed accidentally, some processes should recovery
//case 2 : system shutdown gracefully, maybe some task haven't finished and should be resumed.
func (ms *mysqlDatabase) LoadUnfinishedProcesses() ([]*database.ProcessDataObject, error) {
	tx, err := ms.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	querySQL := "SELECT `status`, `state`, `user`, `class`, `uuid` FROM process" +
		" WHERE stage_committed = 0 OR `status` IN ('Interrupted', 'RInterrupted')"
	rows, err := tx.Query(querySQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]*database.ProcessDataObject, 0)
	for rows.Next() {
		obj := &database.ProcessDataObject{}
		if err := rows.Scan(&obj.Status, &obj.State, &obj.User, &obj.Class, &obj.UUID); err != nil {
			return nil, err
		}
		results = append(results, obj)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (ms *mysqlDatabase) GetProcess(ctx context.Context, processUUID string) (*database.ProcessDataObject, error) {
	tx, err := ms.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	querySQL := "SELECT `storage`, `status`, `state`, `user`, `class`, `uuid` FROM process WHERE uuid = ?"
	obj := &database.ProcessDataObject{}
	err = tx.QueryRowContext(ctx, querySQL, processUUID).Scan(
		&obj.Storage,
		&obj.Status,
		&obj.State,
		&obj.User,
		&obj.Class,
		&obj.UUID,
	)
	if IsNoRowsError(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return obj, nil

}

func (ms *mysqlDatabase) UpdateProcessContext(ctx context.Context, data database.ProcessDataObject) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	//{ update
	updateFieldsSQL := "`status`=?, `storage`=?, `state`=?"
	sqlArgs := []interface{}{data.Status, data.Storage, data.State}
	//}

	upsertSQL := fmt.Sprintf("UPDATE process SET %s WHERE `uuid` = ?", updateFieldsSQL)
	sqlArgs = append(sqlArgs, data.UUID)
	_, err = tx.ExecContext(ctx, upsertSQL, sqlArgs...)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (ms *mysqlDatabase) InitProcessInstanceOnce(
	ctx context.Context, data database.ProcessDataObject) (created bool, err error) {
	tx, err := ms.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var id int64
	querySQL := "SELECT `id` FROM process WHERE uuid = ? FOR UPDATE" //or: LOCK IN SHARE MODE
	err = tx.QueryRowContext(ctx, querySQL, data.UUID).Scan(&id)
	if IsNoRowsError(err) {
		const fields = "(`uuid`, `user`, `class`, `latest_event`," +
			"`stage_committed`, `started_at`, `agent_name`, `storage`, `state`)"
		createSQL := "INSERT INTO process " + fields + "VALUES (?, ?, ?, ?, 0, NOW(), ?, '', '')"
		_, err = tx.ExecContext(ctx, createSQL, data.UUID, data.User, data.Class, data.Event, data.AgentName)
		if err != nil {
			return false, err
		}
		return true, tx.Commit()
	}
	if err != nil {
		return false, err
	}

	updateSQL := "UPDATE process SET `latest_event` = ?, `stage_committed`=0, `agent_name` = ? WHERE uuid = ? "
	_, err = tx.ExecContext(ctx, updateSQL, data.Event, data.AgentName, data.UUID)
	if err != nil {
		return false, err
	}

	err = tx.Commit()
	return false, err
}

func (ms *mysqlDatabase) UpdateProcessStat(ctx context.Context, processUUID string, mask util.BitMask) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	updateFieldsSQL := "`stage_committed`=1"
	sqlArgs := make([]interface{}, 0)

	if mask.Has(util.ProcessSetStartStat) {
		updateFieldsSQL += ", `started_at` = NOW()"
	}

	if mask.Has(util.ProcessSetCompleteStat) {
		updateFieldsSQL += ", `ended_at` = NOW()"
	}

	updateSQL := fmt.Sprintf("UPDATE process SET %s WHERE uuid = ?", updateFieldsSQL)
	sqlArgs = append(sqlArgs, processUUID)

	_, err = tx.ExecContext(ctx, updateSQL, sqlArgs...)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (ms *mysqlDatabase) CreateOrUpdateTaskDetail(
	ctx context.Context, data database.TaskDataObject, opts ...util.BitMask) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	querySQL := "select id from process_task where `process_uuid` = ? and `name` = ?"

	var id int64
	err = tx.QueryRowContext(ctx, querySQL, data.ProcessUUID, data.Name).Scan(&id)
	if IsNoRowsError(err) {
		fields := "`process_uuid`, `name`, `status`, `err_code`, `err_msg`"
		placeHolders := "?,?,?,?,?"
		sqlArgs := []interface{}{data.ProcessUUID, data.Name, data.Status, data.ErrorCode, data.ErrorMsg}
		opt := util.CombineBitMasks(opts...)
		if opt.Has(util.TaskSetStartStat) {
			fields += ", `started_at`"
			placeHolders += ", NOW()"
		}

		createSQL := fmt.Sprintf("INSERT INTO process_task (%s) VALUES (%s)", fields, placeHolders)
		_, err = tx.ExecContext(ctx, createSQL, sqlArgs...)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	opt := util.CombineBitMasks(opts...)

	updateFieldsSQL := "`status` = ?, `err_code`=?, `err_msg` = ?"
	sqlArgs := []interface{}{data.Status, data.ErrorCode, data.ErrorMsg}

	if opt.Has(util.TaskSetStartStat) {
		updateFieldsSQL += ", `started_at` = NOW()"
	}

	if opt.Has(util.TaskSetEndStat) {
		updateFieldsSQL += ", `ended_at` = NOW()"
	}

	updateSQL := fmt.Sprintf("UPDATE process_task SET %s WHERE process_uuid = ? and `name` = ?", updateFieldsSQL)
	sqlArgs = append(sqlArgs, data.ProcessUUID, data.Name)

	_, err = tx.ExecContext(ctx, updateSQL, sqlArgs...)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (ms *mysqlDatabase) CreateProcessTrigger(ctx context.Context, trigger *database.ProcessTriggerObject) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	insertSQL := "INSERT INTO process_trigger (`uuid`, `user`, `class`, `data`, `event`) VALUES (?, ?, ?, ?, ?)"

	result, err := tx.ExecContext(ctx, insertSQL, trigger.UUID, trigger.User, trigger.Class, trigger.Data, trigger.Event)
	if err != nil {
		return err
	}

	insertID, err := result.LastInsertId()
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	trigger.ID = insertID
	return nil
}

func (ms *mysqlDatabase) DeleteProcessTrigger(ctx context.Context, uuid string) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	softDeletionSQL := "UPDATE process_trigger SET deleted_at = now(), deleted_flag = id WHERE uuid = ?"

	_, err = tx.ExecContext(ctx, softDeletionSQL, uuid)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (ms *mysqlDatabase) GetProcessTasks(ctx context.Context, processUUID string) ([]*database.TaskDataObject, error) {
	querySQL := "SELECT a.`name`, a.`status`, a.`err_code`, a.`err_msg`, a.`started_at`, a.`ended_at` FROM" +
		" process_task as a LEFT JOIN process as b ON a.process_uuid = b.uuid WHERE uuid = ? ORDER BY a.id"
	rows, err := ms.db.QueryContext(ctx, querySQL, processUUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]*database.TaskDataObject, 0)
	for rows.Next() {
		td := &database.TaskDataObject{}
		if err := rows.Scan(&td.Name, &td.Status, &td.ErrorCode, &td.ErrorMsg, &td.StartedAt, &td.EndedAt); err != nil {
			return nil, err
		}
		results = append(results, td)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

var _ database.Database = &mysqlDatabase{}

func BuildDatabase(db *sql.DB) database.Database {
	store := &mysqlDatabase{db: db}
	return store
}
