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

func (ms *mysqlDatabase) LoadUncommittedTrigger(ctx context.Context) ([]*database.ProcessTriggerObject, error) {
	querySql := "SELECT `id`, `uuid`, `user`, `class`, `data`, `event` FROM process_trigger WHERE `deleted_flag` = 0 ORDER BY `id`"
	rows, err := ms.db.QueryContext(ctx, querySql)
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

	return results, nil
}

func (ms *mysqlDatabase) GetProcess(ctx context.Context, processUuid string) (*database.ProcessDataObject, error) {
	tx, err := ms.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	querySql := "SELECT `storage`, `status`, `state`, `user`, `class`, `uuid` FROM process WHERE uuid = ?"
	obj := &database.ProcessDataObject{}
	err = tx.QueryRowContext(ctx, querySql, processUuid).Scan(
		&obj.Storage,
		&obj.Status,
		&obj.State,
		&obj.User,
		&obj.Class,
		&obj.Uuid,
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
	defer tx.Rollback()

	//{ update
	updateFieldsSql := "`status`=?, `storage`=?, `state`=?"
	sqlArgs := []interface{}{data.Status, data.Storage, data.State}
	//}

	upsertSql := fmt.Sprintf("UPDATE process SET %s", updateFieldsSql)
	_, err = tx.ExecContext(ctx, upsertSql, sqlArgs...)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (ms *mysqlDatabase) InitProcessInstanceOnce(ctx context.Context, data database.ProcessDataObject) (created bool, err error) {
	tx, err := ms.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var id int64
	querySql := "SELECT `id` FROM process WHERE uuid = ? FOR UPDATE" //or: LOCK IN SHARE MODE
	err = tx.QueryRowContext(ctx, querySql, data.Uuid).Scan(&id)
	if IsNoRowsError(err) {
		createSql := "INSERT INTO process(`uuid`, `user`, `class`, `latest_event`, `stage_committed`, `started_at`, `agent_name`, `storage`, `state`) VALUES (?, ?, ?, ?, 0, NOW(), ?, '', '')"
		_, err = tx.ExecContext(ctx, createSql, data.Uuid, data.User, data.Class, data.Event, data.AgentName)
		if err != nil {
			return false, err
		}
		return true, tx.Commit()
	}
	if err != nil {
		return false, err
	}

	updateSql := "UPDATE process SET `latest_event` = ?, `stage_committed`=0, `agent_name` = ? WHERE uuid = ? "
	_, err = tx.ExecContext(ctx, updateSql, data.Event, data.Uuid, data.AgentName)
	if err != nil {
		return false, err
	}

	err = tx.Commit()
	return false, err
}

func (ms *mysqlDatabase) UpdateProcessStat(ctx context.Context, processUuid string, mask util.BitMask) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	updateFieldsSql := "`stage_committed`=1"
	sqlArgs := make([]interface{}, 0)

	if mask.Has(util.ProcessSetStartStat) {
		updateFieldsSql += ", `started_at` = NOW()"
	}

	if mask.Has(util.ProcessSetCompleteStat) {
		updateFieldsSql += ", `ended_at` = NOW()"
	}

	updateSql := fmt.Sprintf("UPDATE process SET %s WHERE uuid = ?", updateFieldsSql)
	sqlArgs = append(sqlArgs, processUuid)

	_, err = tx.ExecContext(ctx, updateSql, sqlArgs...)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (ms *mysqlDatabase) SaveProcessStorage(ctx context.Context, id int64, data []byte) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	updateSql := "UPDATE process SET storage=? WHERE id = ?"
	_, err = tx.ExecContext(ctx, updateSql, data, id)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (ms *mysqlDatabase) CreateOrUpdateTaskDetail(ctx context.Context, data database.TaskDataObject, opts ...util.BitMask) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	querySql := "select id from process_task where `process_uuid` = ? and `name` = ?"

	var id int64
	err = tx.QueryRowContext(ctx, querySql, data.ProcessUuid, data.Name).Scan(&id)
	if IsNoRowsError(err) {
		fields := "`process_uuid`, `name`, `status`, `err_code`, `err_msg`"
		placeHolders := "?,?,?,?,?"
		sqlArgs := []interface{}{data.ProcessUuid, data.Name, data.Status, data.ErrorCode, data.ErrorMsg}
		opt := util.CombineBitMasks(opts...)
		if opt.Has(util.TaskSetStartStat) {
			fields += ", `started_at`"
			placeHolders += ", NOW()"
		}

		createSql := fmt.Sprintf("INSERT INTO process_task (%s) VALUES (%s)", fields, placeHolders)
		_, err = tx.ExecContext(ctx, createSql, sqlArgs...)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	opt := util.CombineBitMasks(opts...)

	updateFieldsSql := "`status` = ?, `err_code`=?, `err_msg` = ?"
	sqlArgs := []interface{}{data.Status, data.ErrorCode, data.ErrorMsg}

	if opt.Has(util.TaskSetStartStat) {
		updateFieldsSql += ", `started_at` = NOW()"
	}

	if opt.Has(util.TaskSetEndStat) {
		updateFieldsSql += ", `ended_at` = NOW()"
	}

	updateSql := fmt.Sprintf("UPDATE process_task SET %s WHERE process_uuid = ? and `name` = ?", updateFieldsSql)
	sqlArgs = append(sqlArgs, data.ProcessUuid, data.Name)

	_, err = tx.ExecContext(ctx, updateSql, sqlArgs...)
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
	defer tx.Rollback()

	insertSql := "INSERT INTO process_trigger (`uuid`, `user`, `class`, `data`, `event`) VALUES (?, ?, ?, ?, ?)"

	result, err := tx.ExecContext(ctx, insertSql, trigger.UUID, trigger.User, trigger.Class, trigger.Data, trigger.Event)
	if err != nil {
		return err
	}

	insertId, err := result.LastInsertId()
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	trigger.ID = insertId
	return nil
}

func (ms *mysqlDatabase) DeleteProcessTrigger(ctx context.Context, uuid string) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	deleteSql := "DELETE FROM process_trigger WHERE uuid = ?"

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

func (ms *mysqlDatabase) GetProcessTasks(ctx context.Context, processUuid string) ([]*database.TaskDataObject, error) {
	querySql := "SELECT a.`name`, a.`status`, a.`err_code`, a.`err_msg`, a.`started_at`, a.`ended_at` FROM process_task as a LEFT JOIN process as b ON a.process_uuid = b.uuid WHERE uuid = ? ORDER BY a.id"
	rows, err := ms.db.QueryContext(ctx, querySql, processUuid)
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

	return results, nil
}

var _ database.Database = &mysqlDatabase{}

func BuildDatabase(db *sql.DB) *mysqlDatabase {
	store := &mysqlDatabase{db: db}
	return store
}
