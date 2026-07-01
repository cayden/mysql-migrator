package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v3"
)

// ============================================================
//  配置结构体（与 config.yaml 一一对应）
// ============================================================

type Config struct {
	SourceDB    DBConfig    `yaml:"source_db"`
	TargetDB    DBConfig    `yaml:"target_db"`
	BatchSize   int         `yaml:"batch_size"`   // 每批 SELECT 行数
	InsertBatch int         `yaml:"insert_batch"` // 每批 INSERT 行数，默认 500
	DryRun      bool        `yaml:"dry_run"`
	Tables      []TableCfg  `yaml:"tables"`
}

type DBConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Charset  string `yaml:"charset"`
}

type TableCfg struct {
	Source      string      `yaml:"source"`
	Target      string      `yaml:"target"`
	UniqueKey   string      `yaml:"unique_key"`             // B 库主键，用于跳过空值和游标分页
	InsertBatch int         `yaml:"insert_batch,omitempty"` // 覆盖全局 insert_batch
	Description string      `yaml:"description"`
	Columns     []ColumnCfg `yaml:"columns"`
}

type ColumnCfg struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
	Type   string `yaml:"type"`
}

// ============================================================
//  类型转换
// ============================================================

func toInt(ns sql.NullString) int {
	if !ns.Valid || ns.String == "" {
		return 0
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(ns.String), 64)
	if err != nil {
		return 0
	}
	return int(v)
}

func toString(ns sql.NullString) string {
	if !ns.Valid {
		return ""
	}
	return ns.String
}

// ============================================================
//  数据库连接
// ============================================================

func connect(cfg DBConfig) (*sql.DB, error) {
	charset := cfg.Charset
	if charset == "" {
		charset = "utf8mb4"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=true",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, charset)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	return db, nil
}

// ============================================================
//  加载配置
// ============================================================

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析 YAML 失败: %w", err)
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5000
	}
	if cfg.InsertBatch <= 0 {
		cfg.InsertBatch = 500
	}
	return &cfg, nil
}

// ============================================================
//  批量 INSERT
// ============================================================

// buildBatchInsert 构建多值 INSERT 语句
// 例: INSERT IGNORE INTO t (a,b) VALUES (?,?),(?,?),(?,?)
func buildBatchInsert(table string, columns []string, rows [][]interface{}) (string, []interface{}) {
	nCols := len(columns)

	// 单行占位符: (?, ?, ?)
	rowPH := "(" + strings.Repeat("?,", nCols)
	rowPH = rowPH[:len(rowPH)-1] + ")"

	phs := make([]string, len(rows))
	args := make([]interface{}, 0, len(rows)*nCols)
	for i, row := range rows {
		phs[i] = rowPH
		args = append(args, row...)
	}

	sql := fmt.Sprintf("INSERT IGNORE INTO `%s` (%s) VALUES %s",
		table, strings.Join(columns, ","), strings.Join(phs, ","))
	return sql, args
}

// ============================================================
//  迁移单张表
// ============================================================

func migrateTable(srcDB, dstDB *sql.DB, tc TableCfg, batchSize, insertBatch int, dryRun bool) (success, skipped, failed int, err error) {
	desc := tc.Description
	if desc == "" {
		desc = fmt.Sprintf("%s → %s", tc.Source, tc.Target)
	}

	// ----- 构建 SELECT 列 -----
	var srcCols, tgtCols []string
	for _, c := range tc.Columns {
		srcCols = append(srcCols, fmt.Sprintf("`%s`", c.Source))
		tgtCols = append(tgtCols, fmt.Sprintf("`%s`", c.Target))
	}
	tgtColList := strings.Join(tgtCols, ",")

	selectSQL := fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(srcCols, ", "), tc.Source)
	singleInsertSQL := fmt.Sprintf(
		"INSERT IGNORE INTO `%s` (%s) VALUES (%s)",
		tc.Target, tgtColList, strings.Repeat("?,", len(tgtCols))[:len(tgtCols)*2-1],
	)

	// ----- 获取总数 -----
	var total int
	if err := srcDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tc.Source)).Scan(&total); err != nil {
		return 0, 0, 0, fmt.Errorf("查询总数失败: %w", err)
	}

	// ----- 预检 unique_key 空值 -----
	nullKeyCount := 0
	keyIdx := -1
	if tc.UniqueKey != "" {
		checkSQL := fmt.Sprintf(
			"SELECT COUNT(*) FROM `%s` WHERE `%s` IS NULL OR `%s` = ''",
			tc.Source, tc.UniqueKey, tc.UniqueKey,
		)
		if err := srcDB.QueryRow(checkSQL).Scan(&nullKeyCount); err != nil {
			return 0, 0, 0, fmt.Errorf("预检失败: %w", err)
		}
		if nullKeyCount > 0 {
			log.Printf("[%s] ⚠ %d 行 %s 为空, 将被跳过", desc, nullKeyCount, tc.UniqueKey)
		}
		// 找到 unique_key 在 columns 中的位置
		for i, c := range tc.Columns {
			if c.Source == tc.UniqueKey {
				keyIdx = i
				break
			}
		}
	}

	if dryRun {
		log.Printf("[%s] DRY-RUN — 共 %d 行, 预估跳过 %d, 预估写入 %d", desc, total, nullKeyCount, total-nullKeyCount)
		log.Printf("[%s] INSERT SQL: %s", desc, singleInsertSQL)
		return 0, 0, 0, nil
	}

	// ===== 游标分批迁移 =====
	var lastKey string
	cursorUsed := tc.UniqueKey != "" && keyIdx >= 0
	batch := make([][]interface{}, 0, insertBatch)
	batchTotal := 0

	// 进度日志间隔：至少每 50000 行或每 5%
	reportInterval := 50000
	if pctInterval := total / 20; pctInterval > reportInterval {
		reportInterval = pctInterval
	}

	for {
		// 构建分页查询
		var query string
		var args []interface{}
		if cursorUsed && lastKey != "" {
			query = fmt.Sprintf("%s WHERE `%s` > ? ORDER BY `%s` LIMIT %d",
				selectSQL, tc.UniqueKey, tc.UniqueKey, batchSize)
			args = append(args, lastKey)
		} else if cursorUsed {
			query = fmt.Sprintf("%s ORDER BY `%s` LIMIT %d",
				selectSQL, tc.UniqueKey, batchSize)
		} else {
			query = fmt.Sprintf("%s LIMIT %d OFFSET %d",
				selectSQL, batchSize, batchTotal+success+skipped+failed)
		}

		rows, err := srcDB.Query(query, args...)
		if err != nil {
			return success, skipped, failed, fmt.Errorf("查询失败: %w", err)
		}

		rowCount := 0
		for rows.Next() {
			scanDests := make([]sql.NullString, len(tc.Columns))
			scanPtrs := make([]interface{}, len(tc.Columns))
			for i := range scanDests {
				scanPtrs[i] = &scanDests[i]
			}

			if err := rows.Scan(scanPtrs...); err != nil {
				log.Printf("[%s] 扫描行失败: %v", desc, err)
				failed++
				rowCount++
				continue
			}

			// 检查 unique_key 是否为空
			if cursorUsed {
				ns := scanDests[keyIdx]
				if !ns.Valid || strings.TrimSpace(ns.String) == "" {
					skipped++
					rowCount++
					continue
				}
				// 记录游标
				lastKey = ns.String
			}

			// 逐列类型转换
			values := make([]interface{}, len(tc.Columns))
			for i, c := range tc.Columns {
				switch c.Type {
				case "int":
					values[i] = toInt(scanDests[i])
				default:
					values[i] = toString(scanDests[i])
				}
			}

			batch = append(batch, values)
			rowCount++

			// 攒够一批就 flush
			if len(batch) >= insertBatch {
				s, f := flushBatch(dstDB, tc.Target, tgtColList, batch)
				success += s
				failed += f
				batch = batch[:0]
			}
		}
		rows.Close()

		// flush 剩余
		if len(batch) > 0 {
			s, f := flushBatch(dstDB, tc.Target, tgtColList, batch)
			success += s
			failed += f
			batch = batch[:0]
		}

		batchTotal += rowCount
		processed := success + skipped + failed

		// 进度日志
		pct := processed * 100 / max(total, 1)
		if processed%reportInterval < batchSize || rowCount < batchSize || pct%5 == 0 {
			log.Printf("[%s] %d/%d (%d%%)  %d/s",
				desc, processed, total, pct,
				formatRate(success, time.Now()))
		}

		// 最后一页
		if rowCount < batchSize {
			break
		}
	}

	return success, skipped, failed, nil
}

// flushBatch 执行一次批量 INSERT
func flushBatch(dstDB *sql.DB, table, tgtColList string, batch [][]interface{}) (success, failed int) {
	if len(batch) == 0 {
		return 0, 0
	}

	// 多值 INSERT: VALUES (?,?),(?,?),(?,?) ...
	nCols := len(batch[0])
	rowPH := "(" + strings.Repeat("?,", nCols)
	rowPH = rowPH[:len(rowPH)-1] + ")"

	phs := make([]string, len(batch))
	args := make([]interface{}, 0, len(batch)*nCols)
	for i, row := range batch {
		phs[i] = rowPH
		args = append(args, row...)
	}

	sql := fmt.Sprintf("INSERT IGNORE INTO `%s` (%s) VALUES %s",
		table, tgtColList, strings.Join(phs, ","))

	result, err := dstDB.Exec(sql, args...)
	if err != nil {
		log.Printf("批量 INSERT 失败: %v", err)
		return 0, len(batch)
	}

	n, _ := result.RowsAffected()
	return int(n), len(batch) - int(n)
}

// ============================================================
//  主流程
// ============================================================

var startTime time.Time

func main() {
	startTime = time.Now()

	configPath := flag.String("c", "config.yaml", "配置文件路径")
	configPathLong := flag.String("config", "config.yaml", "配置文件路径")
	dryRun := flag.Bool("dry-run", false, "仅验证不写入")
	tableFilter := flag.String("table", "", "只迁移指定表: 索引(0,1..) 或 目标表名 或 描述关键词")
	flag.Parse()

	cfgFile := *configPath
	if cfgFile == "config.yaml" && *configPathLong != "config.yaml" {
		cfgFile = *configPathLong
	}

	log.SetFlags(log.Ltime)
	log.SetPrefix("")

	cfg, err := loadConfig(cfgFile)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	if len(cfg.Tables) == 0 {
		log.Fatal("config.yaml 中 tables 为空, 请先配置表映射")
	}

	isDryRun := *dryRun || cfg.DryRun
	batchSize := cfg.BatchSize
	insertBatch := cfg.InsertBatch

	tables := filterTables(cfg.Tables, *tableFilter)
	if len(tables) == 0 {
		log.Fatalf("未匹配到表: %s", *tableFilter)
	}
	if *tableFilter != "" {
		log.Printf("仅迁移 %d 张表", len(tables))
	}

	if isDryRun {
		log.Println("========== DRY-RUN 模式 (不写入) ==========")
	}

	log.Printf("连接源库 %s:%s", cfg.SourceDB.Host, cfg.SourceDB.Database)
	srcDB, err := connect(cfg.SourceDB)
	if err != nil {
		log.Fatalf("连接源库失败: %v", err)
	}
	defer srcDB.Close()

	log.Printf("连接目标库 %s:%s", cfg.TargetDB.Host, cfg.TargetDB.Database)
	dstDB, err := connect(cfg.TargetDB)
	if err != nil {
		log.Fatalf("连接目标库失败: %v", err)
	}
	defer dstDB.Close()

	if !isDryRun {
		if _, err := dstDB.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
			log.Fatalf("关闭外键检查失败: %v", err)
		}
	}

	totalSuccess, totalSkipped, totalFailed := 0, 0, 0

	for _, tc := range tables {
		desc := tc.Description
		if desc == "" {
			desc = tc.Target
		}

		// 表级 insert_batch 优先
		ib := insertBatch
		if tc.InsertBatch > 0 {
			ib = tc.InsertBatch
		}
		log.Printf("开始迁移: %s (SELECT %d行/批, INSERT %d行/批)", desc, batchSize, ib)

		s, sk, f, err := migrateTable(srcDB, dstDB, tc, batchSize, ib, isDryRun)
		if err != nil {
			log.Printf("[%s] 迁移出错: %v", desc, err)
			continue
		}
		totalSuccess += s
		totalSkipped += sk
		totalFailed += f
	}

	if !isDryRun {
		if _, err := dstDB.Exec("SET FOREIGN_KEY_CHECKS=1"); err != nil {
			log.Printf("恢复外键检查失败: %v", err)
		}
	}

	elapsed := time.Since(startTime).Seconds()
	prefix := ""
	if isDryRun {
		prefix = "DRY-RUN "
	}
	log.Printf("===== %s完成 (%.1fs) =====", prefix, elapsed)
	if !isDryRun {
		log.Printf("成功: %d  跳过: %d  失败: %d", totalSuccess, totalSkipped, totalFailed)
		if totalSuccess > 0 {
			log.Printf("速率: %s 行/秒", formatRate(totalSuccess, startTime))
		}
	}
}

// ============================================================
//  辅助函数
// ============================================================

func filterTables(tables []TableCfg, filter string) []TableCfg {
	if filter == "" {
		return tables
	}
	filter = strings.ToLower(filter)
	var result []TableCfg
	for i, t := range tables {
		if filter == fmt.Sprintf("%d", i) ||
			strings.ToLower(t.Target) == filter ||
			strings.Contains(strings.ToLower(t.Description), filter) {
			result = append(result, t)
		}
	}
	return result
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// formatRate 计算速率
func formatRate(count int, since time.Time) string {
	elapsed := time.Since(since).Seconds()
	if elapsed < 1 {
		return "-"
	}
	rate := float64(count) / elapsed
	if rate >= 10000 {
		return fmt.Sprintf("%.1f万", rate/10000)
	}
	return fmt.Sprintf("%.0f", rate)
}
