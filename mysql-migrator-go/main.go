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
	SourceDB  DBConfig  `yaml:"source_db"`
	TargetDB  DBConfig  `yaml:"target_db"`
	BatchSize int       `yaml:"batch_size"`
	DryRun    bool      `yaml:"dry_run"`
	Tables    []TableCfg `yaml:"tables"`
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
	UniqueKey   string      `yaml:"unique_key"`
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

// toInt 将 sql.NullString 转为 int（空值返回 0）
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

// toString 将 sql.NullString 转为 string（NULL 返回 ""）
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
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
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
		cfg.BatchSize = 1000
	}
	return &cfg, nil
}

// ============================================================
//  迁移单张表
// ============================================================

func migrateTable(srcDB, dstDB *sql.DB, tc TableCfg, batchSize int, dryRun bool) (success, skipped, failed int, err error) {
	desc := tc.Description
	if desc == "" {
		desc = fmt.Sprintf("%s → %s", tc.Source, tc.Target)
	}

	// ----- 构建 SELECT / INSERT SQL -----
	var srcCols, tgtCols []string
	for _, c := range tc.Columns {
		srcCols = append(srcCols, fmt.Sprintf("`%s`", c.Source))
		tgtCols = append(tgtCols, fmt.Sprintf("`%s`", c.Target))
	}

	selectSQL := fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(srcCols, ", "), tc.Source)

	placeholders := strings.Repeat("?,", len(tgtCols))
	placeholders = placeholders[:len(placeholders)-1] // 去掉末尾逗号
	insertSQL := fmt.Sprintf(
		"INSERT IGNORE INTO `%s` (%s) VALUES (%s)",
		tc.Target, strings.Join(tgtCols, ", "), placeholders,
	)

	// ----- 获取总数 -----
	var total int
	if err := srcDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tc.Source)).Scan(&total); err != nil {
		return 0, 0, 0, fmt.Errorf("查询总数失败: %w", err)
	}

	// ----- 预检 unique_key 空值 -----
	nullKeyCount := 0
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
	}

	if dryRun {
		log.Printf("[%s] DRY-RUN — 共 %d 行, 预估跳过 %d, 预估写入 %d", desc, total, nullKeyCount, total-nullKeyCount)
		log.Printf("[%s] INSERT SQL: %s", desc, insertSQL)
		return 0, 0, 0, nil
	}

	// ===== 分批迁移 =====
	// 找到 unique_key 在 columns 中的位置
	keyIdx := -1
	if tc.UniqueKey != "" {
		for i, c := range tc.Columns {
			if c.Source == tc.UniqueKey {
				keyIdx = i
				break
			}
		}
	}

	// 准备 INSERT 语句（复用）
	stmt, err := dstDB.Prepare(insertSQL)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("预编译 INSERT 失败: %w", err)
	}
	defer stmt.Close()

	for offset := 0; offset < total; offset += batchSize {
		query := fmt.Sprintf("%s LIMIT %d OFFSET %d", selectSQL, batchSize, offset)

		// 用 sql.NullString 扫描（源库全是 TEXT，可能为 NULL）
		rows, err := srcDB.Query(query)
		if err != nil {
			return success, skipped, failed, fmt.Errorf("查询失败 (offset=%d): %w", offset, err)
		}

		for rows.Next() {
			// 动态构建扫描目标
			scanDests := make([]sql.NullString, len(tc.Columns))
			scanPtrs := make([]interface{}, len(tc.Columns))
			for i := range scanDests {
				scanPtrs[i] = &scanDests[i]
			}

			if err := rows.Scan(scanPtrs...); err != nil {
				log.Printf("[%s] 扫描行失败: %v", desc, err)
				failed++
				continue
			}

			// 检查 unique_key 是否为空
			if keyIdx >= 0 {
				ns := scanDests[keyIdx]
				if !ns.Valid || strings.TrimSpace(ns.String) == "" {
					skipped++
					continue
				}
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

			if _, err := stmt.Exec(values...); err != nil {
				log.Printf("[%s] 写入失败: %v", desc, err)
				failed++
			} else {
				success++
			}
		}
		rows.Close()

		done := offset + batchSize
		if done > total {
			done = total
		}
		pct := done * 100 / max(total, 1)
		log.Printf("[%s] %d/%d (%d%%)  成功 %d  跳过 %d  失败 %d",
			desc, done, total, pct, success, skipped, failed)
	}

	return success, skipped, failed, nil
}

// ============================================================
//  主流程
// ============================================================

func main() {
	configPath := flag.String("c", "config.yaml", "配置文件路径")
	configPathLong := flag.String("config", "config.yaml", "配置文件路径")
	dryRun := flag.Bool("dry-run", false, "仅验证不写入")
	tableFilter := flag.String("table", "", "只迁移指定表: 索引(0,1..) 或 目标表名 或 描述关键词")
	flag.Parse()

	// 合并长短参数
	cfgFile := *configPath
	if cfgFile == "config.yaml" && *configPathLong != "config.yaml" {
		cfgFile = *configPathLong
	}

	log.SetFlags(log.Ltime)
	log.SetPrefix("")

	// 加载配置
	cfg, err := loadConfig(cfgFile)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	if len(cfg.Tables) == 0 {
		log.Fatal("config.yaml 中 tables 为空, 请先配置表映射")
	}

	isDryRun := *dryRun || cfg.DryRun
	batchSize := cfg.BatchSize

	// 过滤表
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

	// 连接数据库
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

	// 关闭外键检查
	if !isDryRun {
		if _, err := dstDB.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
			log.Fatalf("关闭外键检查失败: %v", err)
		}
	}

	start := time.Now()
	totalSuccess, totalSkipped, totalFailed := 0, 0, 0

	for _, tc := range tables {
		desc := tc.Description
		if desc == "" {
			desc = tc.Target
		}
		log.Printf("开始迁移: %s", desc)

		s, sk, f, err := migrateTable(srcDB, dstDB, tc, batchSize, isDryRun)
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

	elapsed := time.Since(start).Seconds()
	prefix := ""
	if isDryRun {
		prefix = "DRY-RUN "
	}
	log.Printf("===== %s完成 (%.1fs) =====", prefix, elapsed)
	if !isDryRun {
		log.Printf("成功: %d  跳过: %d  失败: %d", totalSuccess, totalSkipped, totalFailed)
	}
}

// ============================================================
//  辅助函数
// ============================================================

// filterTables 按索引/目标表名/描述关键词过滤
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
