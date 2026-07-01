package main

import (
	"context"
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
//  配置结构体
// ============================================================

type Config struct {
	SourceDB   DBConfig    `yaml:"source_db"`
	TargetDB   DBConfig    `yaml:"target_db"`            // target_type=mysql 时使用
	TargetType string      `yaml:"target_type"`          // "mysql" (默认) | "doris"
	Doris      DorisConfig `yaml:"doris"`                // target_type=doris 时使用
	BatchSize  int         `yaml:"batch_size"`           // 每批 SELECT 行数
	InsertBatch int        `yaml:"insert_batch"`         // MySQL 目标：每条 INSERT 行数
	StreamBatch int        `yaml:"stream_batch"`         // Doris 目标：每批 Stream Load 行数
	DryRun     bool        `yaml:"dry_run"`
	Tables     []TableCfg  `yaml:"tables"`
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
	Source       string      `yaml:"source"`
	Target       string      `yaml:"target"`
	UniqueKey    string      `yaml:"unique_key"`
	InsertBatch  int         `yaml:"insert_batch,omitempty"`  // MySQL 表级覆盖
	StreamBatch  int         `yaml:"stream_batch,omitempty"`  // Doris 表级覆盖
	Description  string      `yaml:"description"`
	Columns      []ColumnCfg `yaml:"columns"`
}

type ColumnCfg struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
	Type   string `yaml:"type"` // "string" | "int"
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
//  数据库连接（仅 MySQL 源库）
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
//  配置加载
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

	// 默认值
	if cfg.TargetType == "" {
		cfg.TargetType = "mysql"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5000
	}
	if cfg.InsertBatch <= 0 {
		cfg.InsertBatch = 500
	}
	if cfg.StreamBatch <= 0 {
		cfg.StreamBatch = 200000
	}
	if cfg.Doris.HttpPort <= 0 {
		cfg.Doris.HttpPort = 8030
	}
	if cfg.Doris.LabelPrefix == "" {
		cfg.Doris.LabelPrefix = "migrate"
	}
	if cfg.Doris.MaxFilterRatio <= 0 {
		cfg.Doris.MaxFilterRatio = 0.0
	}
	if cfg.Doris.Timeout <= 0 {
		cfg.Doris.Timeout = 600
	}
	if cfg.Doris.StrictMode == nil {
		f := false
		cfg.Doris.StrictMode = &f
	}
	if cfg.Doris.StreamBatch <= 0 {
		cfg.Doris.StreamBatch = cfg.StreamBatch
	}

	return &cfg, nil
}

// ============================================================
//  迁移单张表
// ============================================================

func migrateTable(srcDB *sql.DB, writer targetWriter, tc TableCfg, batchSize, flushSize int, dryRun bool) (success, skipped, failed int, err error) {
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
	selectSQL := fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(srcCols, ", "), tc.Source)

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
		for i, c := range tc.Columns {
			if c.Source == tc.UniqueKey {
				keyIdx = i
				break
			}
		}
	}

	if dryRun {
		log.Printf("[%s] DRY-RUN — 共 %d 行, 预估跳过 %d, 预估写入 %d", desc, total, nullKeyCount, total-nullKeyCount)
		return 0, 0, 0, nil
	}

	// ===== 游标分批迁移 =====
	var lastKey string
	cursorUsed := tc.UniqueKey != "" && keyIdx >= 0
	batchTotal := 0

	reportInterval := 50000
	if pctInterval := total / 20; pctInterval > reportInterval {
		reportInterval = pctInterval
	}

	for {
		// 分页查询
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
		rowBatch := make([][]interface{}, 0, flushSize) // 本地攒批

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
				lastKey = ns.String
			}

			// 类型转换
			values := make([]interface{}, len(tc.Columns))
			for i, c := range tc.Columns {
				switch c.Type {
				case "int":
					values[i] = toInt(scanDests[i])
				default:
					values[i] = toString(scanDests[i])
				}
			}

			rowBatch = append(rowBatch, values)
			rowCount++

			// 攒够一批就 flush
			if len(rowBatch) >= flushSize {
				stored, err := writer.flushBatch(context.Background(), tc.Target, tgtCols, rowBatch)
				if err != nil {
					log.Printf("[%s] 写入失败: %v", desc, err)
					failed += len(rowBatch)
				} else {
					success += int(stored)
					failed += len(rowBatch) - int(stored)
				}
				rowBatch = rowBatch[:0]
			}
		}
		rows.Close()

		// flush 剩余
		if len(rowBatch) > 0 {
			stored, err := writer.flushBatch(context.Background(), tc.Target, tgtCols, rowBatch)
			if err != nil {
				log.Printf("[%s] 写入失败: %v", desc, err)
				failed += len(rowBatch)
			} else {
				success += int(stored)
				failed += len(rowBatch) - int(stored)
			}
		}

		batchTotal += rowCount
		processed := success + skipped + failed

		// 进度日志
		pct := processed * 100 / max(total, 1)
		if processed%reportInterval < batchSize || rowCount < batchSize || pct%5 == 0 {
			log.Printf("[%s] %d/%d (%d%%)  %s/s",
				desc, processed, total, pct,
				formatRate(success, startTime))
		}

		if rowCount < batchSize {
			break
		}
	}

	return success, skipped, failed, nil
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
	tableFilter := flag.String("table", "", "只迁移指定表: 索引(0,1..) 或 目标表名 或 描述")
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

	tables := filterTables(cfg.Tables, *tableFilter)
	if len(tables) == 0 {
		log.Fatalf("未匹配到表: %s", *tableFilter)
	}
	if *tableFilter != "" {
		log.Printf("仅迁移 %d 张表", len(tables))
	}

	// ===== 连接源库（始终是 MySQL）=====
	log.Printf("连接源库 %s:%s", cfg.SourceDB.Host, cfg.SourceDB.Database)
	srcDB, err := connect(cfg.SourceDB)
	if err != nil {
		log.Fatalf("连接源库失败: %v", err)
	}
	defer srcDB.Close()

	// ===== 根据 target_type 创建写入器 =====
	var writer targetWriter

	switch strings.ToLower(cfg.TargetType) {
	case "doris":
		log.Printf("目标类型: Apache Doris (Stream Load)")
		writer = NewDorisWriter(cfg.Doris)

	default:
		log.Printf("目标类型: MySQL (Batch INSERT)")
		log.Printf("连接目标库 %s:%s", cfg.TargetDB.Host, cfg.TargetDB.Database)
		dstDB, err := connect(cfg.TargetDB)
		if err != nil {
			log.Fatalf("连接目标库失败: %v", err)
		}
		defer dstDB.Close()

		writer = NewMySQLWriter(dstDB, cfg.InsertBatch)
	}

	// 准备
	if err := writer.prepare(context.Background()); err != nil {
		log.Fatalf("写入器准备失败: %v", err)
	}
	defer writer.close()

	if isDryRun {
		log.Println("========== DRY-RUN 模式 (不写入) ==========")
	}

	totalSuccess, totalSkipped, totalFailed := 0, 0, 0

	for _, tc := range tables {
		desc := tc.Description
		if desc == "" {
			desc = tc.Target
		}

		// 确定每批 flush 大小
		flushSize := cfg.InsertBatch
		if cfg.TargetType == "doris" {
			flushSize = cfg.Doris.StreamBatch
		}
		// 表级覆盖
		if tc.StreamBatch > 0 && cfg.TargetType == "doris" {
			flushSize = tc.StreamBatch
		}
		if tc.InsertBatch > 0 && cfg.TargetType != "doris" {
			flushSize = tc.InsertBatch
		}

		targetLabel := cfg.TargetType
		log.Printf("开始迁移: %s (SELECT %d行/批, flush %d行/批, 目标=%s)", desc, batchSize, flushSize, targetLabel)

		s, sk, f, err := migrateTable(srcDB, writer, tc, batchSize, flushSize, isDryRun)
		if err != nil {
			log.Printf("[%s] 迁移出错: %v", desc, err)
			continue
		}
		totalSuccess += s
		totalSkipped += sk
		totalFailed += f
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
