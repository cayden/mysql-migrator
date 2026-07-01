#!/usr/bin/env python3
"""
MySQL 跨实例数据迁移工具

读取 config.yaml，将源库多张表的数据按字段映射关系迁移到目标库。
支持 TEXT → INT / TEXT → VARCHAR 类型转换，跳过主键为空或重复的行。

用法:  python migrate.py [--dry-run] [--table <索引或描述>]
       python migrate.py --dry-run           # 仅验证，不写入
       python migrate.py --table 0            # 只迁移第 0 张表
       python migrate.py --table dwd_boc_fxpf1  # 按目标表名过滤
"""

import pymysql
import yaml
import argparse
import logging
import sys
from datetime import datetime


# ============================================================
#  类型转换器
# ============================================================

def to_string(v):
    if v is None:
        return ''
    return str(v)


def to_int(v):
    if v is None:
        return 0
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return 0


CONVERTERS = {
    'string': to_string,
    'int': to_int,
}


# ============================================================
#  核心迁移逻辑
# ============================================================

def load_config(path='config.yaml'):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def db_connect(cfg):
    return pymysql.connect(
        host=cfg['host'],
        port=cfg.get('port', 3306),
        user=cfg['user'],
        password=cfg['password'],
        database=cfg['database'],
        charset=cfg.get('charset', 'utf8mb4'),
        cursorclass=pymysql.cursors.Cursor,
    )


def migrate_table(cur_a, cur_b, table_cfg, batch_size, dry_run=False):
    """迁移单张表，返回 (成功, 跳过, 失败)"""
    src = table_cfg['source']
    dst = table_cfg['target']
    columns = table_cfg['columns']
    unique_key = table_cfg.get('unique_key')
    desc = table_cfg.get('description', f'{src}→{dst}')

    # ----- 构建 SQL -----
    src_cols = [c['source'] for c in columns]
    tgt_cols = [c['target'] for c in columns]

    select_sql = f"SELECT {', '.join(f'`{c}`' for c in src_cols)} FROM `{src}`"
    placeholders = ', '.join(['%s'] * len(tgt_cols))
    insert_sql = (
        f"INSERT IGNORE INTO `{dst}` "
        f"({', '.join(f'`{c}`' for c in tgt_cols)}) "
        f"VALUES ({placeholders})"
    )

    # ----- 获取总数 -----
    cur_a.execute(f"SELECT COUNT(*) FROM `{src}`")
    total = cur_a.fetchone()[0]

    # ----- 预检 unique_key -----
    null_key_count = 0
    if unique_key and unique_key in src_cols:
        cur_a.execute(
            f"SELECT COUNT(*) FROM `{src}` "
            f"WHERE `{unique_key}` IS NULL OR `{unique_key}` = ''"
        )
        null_key_count = cur_a.fetchone()[0]
        if null_key_count > 0:
            logging.warning(f"[{desc}] {null_key_count} 行 {unique_key} 为空, 将被跳过")

    if dry_run:
        logging.info(f"[{desc}] DRY-RUN — 共 {total} 行, "
                     f"预估跳过 {null_key_count}, 预估写入 {total - null_key_count}")
        logging.info(f"[{desc}] INSERT SQL: {insert_sql}")
        return 0, 0, 0

    # ----- 分批迁移 -----
    success = skipped = failed = 0
    offset = 0

    while offset < total:
        cur_a.execute(f"{select_sql} LIMIT {batch_size} OFFSET {offset}")
        rows = cur_a.fetchall()
        if not rows:
            break

        for row in rows:
            # 检查 unique_key 是否为空
            if unique_key:
                key_idx = src_cols.index(unique_key)
                key_val = row[key_idx]
                if not key_val or str(key_val).strip() == '':
                    skipped += 1
                    continue

            # 逐列类型转换
            converted = []
            for i, col_cfg in enumerate(columns):
                col_type = col_cfg.get('type', 'string')
                fn = CONVERTERS.get(col_type, to_string)
                converted.append(fn(row[i]))

            try:
                cur_b.execute(insert_sql, converted)
                success += 1
            except Exception as e:
                logging.error(f"[{desc}] 写入失败: {e}")
                failed += 1

        cur_b.commit()
        offset += batch_size
        pct = min(100, offset * 100 // max(total, 1))
        logging.info(
            f"[{desc}] {min(offset, total)}/{total} ({pct}%)  "
            f"成功 {success}  跳过 {skipped}  失败 {failed}"
        )

    return success, skipped, failed


# ============================================================
#  主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='MySQL 跨实例数据迁移')
    parser.add_argument('-c', '--config', default='config.yaml', help='配置文件路径')
    parser.add_argument('--dry-run', action='store_true', help='仅验证不写入')
    parser.add_argument('--table', type=str, default=None,
                        help='只迁移指定表: 索引(0,1..) 或 目标表名 或 描述关键词')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )

    # 加载配置
    cfg = load_config(args.config)

    # dry_run 以命令行参数优先, 其次 config 文件
    dry_run = args.dry_run or cfg.get('dry_run', False)
    batch_size = cfg.get('batch_size', 1000)
    all_tables = cfg.get('tables', [])

    if not all_tables:
        logging.error("config.yaml 中 tables 为空, 请先配置表映射")
        sys.exit(1)

    # 过滤表
    if args.table is not None:
        filtered = []
        for i, t in enumerate(all_tables):
            match = (
                args.table == str(i) or
                args.table.lower() == t['target'].lower() or
                args.table in t.get('description', '')
            )
            if match:
                filtered.append(t)
        if not filtered:
            logging.error(f"未匹配到表: {args.table}")
            sys.exit(1)
        tables = filtered
        logging.info(f"仅迁移 {len(tables)} 张表")
    else:
        tables = all_tables

    if dry_run:
        logging.info("========== DRY-RUN 模式 (不写入) ==========")

    # 连接数据库
    logging.info(f"连接源库 {cfg['source_db']['host']}:{cfg['source_db']['database']}")
    conn_a = db_connect(cfg['source_db'])
    logging.info(f"连接目标库 {cfg['target_db']['host']}:{cfg['target_db']['database']}")
    conn_b = db_connect(cfg['target_db'])

    try:
        cur_a = conn_a.cursor()
        cur_b = conn_b.cursor()

        if not dry_run:
            cur_b.execute("SET FOREIGN_KEY_CHECKS=0")

        total_success = total_skipped = total_failed = 0
        start = datetime.now()

        for table_cfg in tables:
            s, sk, f = migrate_table(cur_a, cur_b, table_cfg, batch_size, dry_run)
            total_success += s
            total_skipped += sk
            total_failed += f

        elapsed = (datetime.now() - start).total_seconds()

        if not dry_run:
            cur_b.execute("SET FOREIGN_KEY_CHECKS=1")

        logging.info(f"===== {'DRY-RUN ' if dry_run else ''}完成 ({elapsed:.1f}s) =====")
        if not dry_run:
            logging.info(f"成功: {total_success}  跳过: {total_skipped}  失败: {total_failed}")

    finally:
        conn_a.close()
        conn_b.close()


if __name__ == '__main__':
    main()
