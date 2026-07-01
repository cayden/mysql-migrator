package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// MySQLWriter 使用多值批量 INSERT 写入 MySQL。
type MySQLWriter struct {
	db          *sql.DB
	insertBatch int // 单条 SQL 包含的最大行数（实际由调用方控制批次）
}

func NewMySQLWriter(db *sql.DB, insertBatch int) *MySQLWriter {
	if insertBatch <= 0 {
		insertBatch = 500
	}
	return &MySQLWriter{db: db, insertBatch: insertBatch}
}

func (w *MySQLWriter) prepare(ctx context.Context) error {
	_, err := w.db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0")
	return err
}

func (w *MySQLWriter) close() error {
	_, err := w.db.Exec("SET FOREIGN_KEY_CHECKS=1")
	return err
}

// flushBatch 构建多值 INSERT 并执行。返回 RowsAffected。
func (w *MySQLWriter) flushBatch(ctx context.Context, table string, columns []string, rows [][]interface{}) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	nCols := len(columns)
	rowPH := "(" + strings.Repeat("?,", nCols)
	rowPH = rowPH[:len(rowPH)-1] + ")"

	phs := make([]string, len(rows))
	args := make([]interface{}, 0, len(rows)*nCols)
	for i, row := range rows {
		phs[i] = rowPH
		args = append(args, row...)
	}

	colList := strings.Join(columns, ",")
	sql := fmt.Sprintf("INSERT IGNORE INTO `%s` (%s) VALUES %s", table, colList, strings.Join(phs, ","))

	result, err := w.db.ExecContext(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("批量 INSERT: %w", err)
	}
	return result.RowsAffected()
}
