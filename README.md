# MySQL 跨实例数据迁移工具

将 MySQL A 库（全 TEXT 字段）的数据按字段映射迁移到 B 库（VARCHAR / INT 等类型），支持多表、跨实例、类型转换、分批处理。

## 目录结构

```
├── mysql-migrator/          # Python 版
│   ├── config.yaml
│   ├── migrate.py
│   └── requirements.txt
├── mysql-migrator-go/       # Go 版
│   ├── config.yaml
│   ├── main.go
│   ├── go.mod
│   └── go.sum
└── README.md
```

两个版本共用同一套 `config.yaml`，功能完全一致，按语言偏好选择。

---

## 快速开始

### Python 版

```bash
cd mysql-migrator
pip install -r requirements.txt

# 先验证（不写入）
python migrate.py --dry-run

# 正式迁移
python migrate.py
```

### Go 版

```bash
cd mysql-migrator-go
go mod tidy

# 先验证
go run . --dry-run

# 正式迁移（或编译后运行）
go build -o mysql-migrator .
./mysql-migrator
```

---

## config.yaml 配置说明

```yaml
# 源库（A库）
source_db:
  host: "10.0.0.1"
  port: 3306
  user: "root"
  password: "your_password"
  database: "source_db"
  charset: "utf8mb4"

# 目标库（B库）
target_db:
  host: "10.0.0.2"
  port: 3306
  user: "root"
  password: "your_password"
  database: "target_db"
  charset: "utf8mb4"

# 全局设置
batch_size: 1000       # 每批读取行数
dry_run: false         # true = 只打印不写入

# 表映射列表
tables:
  - source: "A表名"
    target: "B表名"
    unique_key: "B库主键字段"       # 为空则跳过该行
    description: "说明文字"          # 可选
    columns:
      - source: "A表字段1"
        target: "B表字段1"
        type: "string"              # string 或 int
      - source: "A表字段2"
        target: "B表字段2"
        type: "int"
```

### 字段 type 说明

| type | 含义 | 空值处理 |
|------|------|---------|
| `string` | TEXT → VARCHAR，原样传递 | NULL → `""` |
| `int` | TEXT → INT，自动转换 | NULL / 非数字 → `0` |

### 注意事项

- A 表多余的字段不写进 `columns` 就不会导出
- B 表多出但 A 表没有的字段需要提前设好默认值
- `unique_key` 为空或重复的行会被跳过（`INSERT IGNORE`）
- 迁移前自动关闭外键检查，完成后恢复

---

## CLI 参数

| 参数 | 说明 |
|------|------|
| `-c, --config` | 配置文件路径（默认 `config.yaml`） |
| `--dry-run` | 仅验证，不写入数据 |
| `--table` | 只迁移指定表：索引 `0` / 目标表名 / 描述关键词 |

```bash
# 只迁移第 0 张表
python migrate.py --table 0

# 按目标表名过滤
go run . --table dwd_boc_fxpf1

# 先用另一个配置文件
./mysql-migrator -c /path/to/other.yaml --dry-run
```

---

## 使用场景

- **数仓 ETL**：ODS 层全 TEXT 表 → DWD 层类型化表
- **系统升级**：旧系统宽松 schema 迁移到新系统严格 schema
- **跨环境同步**：开发库 → 测试库 / 生产库，结构不同

## License

MIT
