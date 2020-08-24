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

func (ms *mysqlDatabase) GetInstance(ctx context.Context, uuid string) (*database.ProcessDataObject, error) {
	tx, err := ms.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	querySql := "SELECT id, storage, status, `state`, `user`, `class`, `uuid` FROM process WHERE uuid = ?"
	obj := &database.ProcessDataObject{}
	err = tx.QueryRowContext(ctx, querySql, uuid).Scan(
		&obj.ID,
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

func (ms *mysqlDatabase) UpsertProcess(ctx context.Context, data database.ProcessDataObject) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	//{ insert
	insertFieldsSql := "`uuid`, `user`, `class`, `status`, `storage`, `state`"
	insertFieldsSqlPlaceHolder := "?, ?, ?, ?, ?, ?"
	sqlArgs := []interface{}{data.Uuid, data.User, data.Class, data.Status, data.Storage, data.State}
	//}

	//{ update
	updateFieldsSql := "`status`=?, `storage`=?, `state`=?"
	sqlArgs = append(sqlArgs, data.Status, data.Storage, data.State)
	//}

	upsertSql := fmt.Sprintf(
		"INSERT INTO process (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
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

func (ms *mysqlDatabase) GetOrCreateInstance(ctx context.Context, data database.ProcessDataPartial) (obj database.ProcessDataObject, err error) {
	tx, err := ms.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	obj.ProcessDataPartial = data

	querySql := "SELECT id, storage, status, `state` FROM process WHERE uuid = ?"
	err = tx.QueryRowContext(ctx, querySql, data.Uuid).Scan(&obj.ID, &obj.Storage, &obj.Status, &obj.State)
	if IsNoRowsError(err) {
		var id int64
		id, err = createPendingProcess(ctx, tx, &data)
		if err != nil {
			return
		}
		obj.ID = id
	}

	err = tx.Commit()
	return
}

func createPendingProcess(ctx context.Context, tx *sql.Tx, data *database.ProcessDataPartial) (int64, error) {
	//XXX state is empty now, executor will initialize it.
	createSql := "INSERT INTO process (uuid, user, class, status, storage, `state`) VALUES (?, ?, ?, ?, ?, ?)"
	result, err := tx.ExecContext(ctx, createSql, data.Uuid, data.User, data.Class, data.Status, data.Storage, data.State)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (ms *mysqlDatabase) CreatePendingInstance(
	ctx context.Context,
	data database.ProcessDataPartial,
) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = createPendingProcess(ctx, tx, &data)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (ms *mysqlDatabase) UpdateProcess(ctx context.Context, obj database.ProcessDataObject, agentName string, mask util.BitMask) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	updateFieldsSql := "`status` = ?, `state` = ?, `storage` = ?, `agent_name` = ?"
	sqlArgs := []interface{}{obj.Status, obj.State, obj.Storage, agentName}

	if mask.Has(util.ProcessSetStartStat) {
		updateFieldsSql += ", `started_at` = NOW()"
	}

	if mask.Has(util.ProcessSetCompleteStat) {
		updateFieldsSql += ", `ended_at` = NOW()"
	}

	updateSql := fmt.Sprintf("UPDATE process SET %s WHERE id = ?", updateFieldsSql)
	sqlArgs = append(sqlArgs, obj.ID)

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

func (ms *mysqlDatabase) UpsertTask(ctx context.Context, data database.TaskDataObject, opts util.BitMask) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	//{insert
	insertFieldsSql := "`process_id`, `name`, `status`"
	insertFieldsSqlPlaceHolder := "?, ?, ?"
	sqlArgs := []interface{}{data.ProcessID, data.Name, data.Status}

	if opts.Has(util.TaskSetError) {
		insertFieldsSql += ", `err_code`"
		insertFieldsSqlPlaceHolder += ", ?"
		sqlArgs = append(sqlArgs, data.ErrorCode)

		insertFieldsSql += ", `err_msg`"
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
		updateFieldsSql += ", `err_msg` = ?, `err_code` = ?"
		sqlArgs = append(sqlArgs, data.ErrorMsg, data.ErrorCode)
	}

	if opts.Has(util.TaskSetFinishStat) {
		updateFieldsSql += ", `ended_at` = NOW()"
	}
	//}

	upsertSql := fmt.Sprintf(
		"INSERT INTO process_task (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
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

func (ms *mysqlDatabase) CreateProcessTrigger(ctx context.Context, trigger *database.ProcessTriggerObject) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	insertSql := "INSERT INTO process_trigger (uuid, `user`, `class`, `data`, `event`) VALUES (?, ?, ?, ?, ?)"

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

func (ms *mysqlDatabase) CreateResumeProcessTrigger(ctx context.Context, user, class, uuid string) (int64, error) {
	tx, err := ms.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	insertSql := "INSERT INTO process_trigger (uuid, `user`, class, `data`) VALUES (?, ?, ?, '')"

	result, err := tx.ExecContext(ctx, insertSql, uuid, user, class)
	if err != nil {
		return 0, err
	}
	insertId, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	err = tx.Commit()
	if err != nil {
		return 0, err
	}
	return insertId, nil
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

func (ms *mysqlDatabase) LoadTasks(ctx context.Context, id int64) ([]*database.TaskDataObject, error) {
	querySql := "SELECT `name`, `status`, `executed` FROM process_task WHERE process_id = ?"
	rows, err := ms.db.QueryContext(ctx, querySql, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]*database.TaskDataObject, 0)
	for rows.Next() {
		td := &database.TaskDataObject{
			ProcessID: id,
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

func (ms *mysqlDatabase) GetProcessTasks(ctx context.Context, uuid string) ([]*database.TaskDataObject, error) {
	querySql := "SELECT a.`name`, a.`status`, a.`err_code`, a.`err_msg`, a.`started_at`, a.`ended_at` FROM process_task as a LEFT JOIN process as b ON a.process_id = b.id WHERE uuid = ?"
	rows, err := ms.db.QueryContext(ctx, querySql, uuid)
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
