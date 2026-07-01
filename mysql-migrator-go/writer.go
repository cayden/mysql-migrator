package main

import "context"

// targetWriter 抽象目标写入路径，支持 MySQL INSERT 和 Doris Stream Load 两种实现。
type targetWriter interface {
	// flushBatch 写入一批行数据到目标表，返回成功存储的行数。
	flushBatch(ctx context.Context, table string, columns []string, rows [][]interface{}) (stored int64, err error)

	// prepare 写入前的一次性准备工作（如 MySQL 关闭外键检查）。
	prepare(ctx context.Context) error

	// close 写入后的清理工作。
	close() error
}
