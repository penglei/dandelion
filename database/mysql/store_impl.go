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

func (ms *mysqlStore) LoadUncommittedMeta(ctx context.Context) ([]*database.ProcessMetaObject, error) {
	querySql := "SELECT `id`, `uuid`, `user`, `class`, `data` FROM process_meta WHERE `deleted_at` is NULL ORDER BY `id`"
	rows, err := ms.db.QueryContext(ctx, querySql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]*database.ProcessMetaObject, 0)
	for rows.Next() {
		obj := &database.ProcessMetaObject{}
		if err := rows.Scan(&obj.ID, &obj.UUID, &obj.User, &obj.Class, &obj.Data); err != nil {
			return nil, err
		}
		results = append(results, obj)
	}

	return results, nil
}

func (ms *mysqlStore) GetInstance(ctx context.Context, uuid string) (*database.ProcessDataObject, error) {
	tx, err := ms.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	querySql := "SELECT id, storage, status, plan_state, running_cnt FROM process WHERE uuid = ?"
	obj := &database.ProcessDataObject{}
	err = tx.QueryRowContext(ctx, querySql, uuid).Scan(&obj.ID, &obj.Storage, &obj.Status, &obj.PlanState, &obj.RunningCnt)
	if IsNoRowsError(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return obj, nil

}

func (ms *mysqlStore) GetOrCreateInstance(ctx context.Context, data database.ProcessDataPartial) (obj database.ProcessDataObject, err error) {
	tx, err := ms.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	obj.ProcessDataPartial = data

	querySql := "SELECT id, storage, status, plan_state, running_cnt FROM process WHERE uuid = ?"
	err = tx.QueryRowContext(ctx, querySql, data.Uuid).Scan(&obj.ID, &obj.Storage, &obj.Status, &obj.PlanState, &obj.RunningCnt)
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
	//XXX plan_state is empty now, executor will initialize it.
	createSql := "INSERT INTO process (uuid, user, class, status, storage, plan_state) VALUES (?, ?, ?, ?, ?, ?)"
	result, err := tx.ExecContext(ctx, createSql, data.Uuid, data.User, data.Class, data.Status, data.Storage, data.PlanState)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (ms *mysqlStore) CreatePendingInstance(
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

func (ms *mysqlStore) UpdateProcess(ctx context.Context, obj database.ProcessDataObject, agentName string, mask util.BitMask) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	updateFieldsSql := "`status` = ?, `plan_state` = ?, `storage` = ?, `agent_name` = ?"
	sqlArgs := []interface{}{obj.Status, obj.PlanState, obj.Storage, agentName}

	if mask.Has(util.ProcessSetStartStat) {
		updateFieldsSql += ", `started_at` = NOW()"
	}

	if mask.Has(util.ProcessUpdateRunningCnt) {
		updateFieldsSql += ", `running_cnt` = ?"
		sqlArgs = append(sqlArgs, obj.RunningCnt)
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

func (ms *mysqlStore) SaveProcessStorage(ctx context.Context, id int64, data []byte) error {
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

func (ms *mysqlStore) UpsertTask(ctx context.Context, data database.TaskDataObject, opts util.BitMask) error {
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

func (ms *mysqlStore) CreateProcessMeta(ctx context.Context, meta *database.ProcessMetaObject) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	insertSql := "INSERT INTO process_meta (uuid, `user`, class, `data`) VALUES (?, ?, ?, ?)"

	result, err := tx.ExecContext(ctx, insertSql, meta.UUID, meta.User, meta.Class, meta.Data)
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
	meta.ID = insertId
	return nil
}

func (ms *mysqlStore) CreateRerunProcessMeta(ctx context.Context, user, class, uuid string) (int64, error) {
	tx, err := ms.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	insertSql := "INSERT INTO process_meta (uuid, `user`, class, rerun, `data`) VALUES (?, ?, ?, 1, '')"

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

func (ms *mysqlStore) DeleteProcessMeta(ctx context.Context, uuid string) error {
	tx, err := ms.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	deleteSql := "DELETE FROM process_meta WHERE uuid = ?"

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

func (ms *mysqlStore) LoadTasks(ctx context.Context, id int64) ([]*database.TaskDataObject, error) {
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

var _ database.RuntimeStore = &mysqlStore{}

func BuildRuntimeStore(db *sql.DB) *mysqlStore {
	store := &mysqlStore{db: db}
	return store
}
