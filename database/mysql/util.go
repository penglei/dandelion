package mysql

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
)

func IsKeyDuplicationError(err error) bool {
	var myErr, ok = err.(*mysql.MySQLError)
	return ok && myErr.Number == 1062
}

func IsNoRowsError(err error) bool {
	return err == sql.ErrNoRows
}

func CheckNoRowsError(err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if IsNoRowsError(err) {
		return true, nil
	}
	return true, err
}
