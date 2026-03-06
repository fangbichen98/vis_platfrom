#!/usr/bin/env python3
"""
Extract GBA (Greater Bay Area) first week flow data
从2021和2024全量数据中提取第一周的GBA格网流动数据
"""

import pandas as pd
from pathlib import Path

# 配置
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
GRID_METADATA = DATA_DIR / "grid_metadata" / "PRD_grid_metadata.csv"

# 输入文件
INPUT_2021 = DATA_DIR / "2021.csv"
INPUT_2024 = DATA_DIR / "2024.csv"

# 输出文件
OUTPUT_2021 = DATA_DIR / "2021_gba_week.csv"
OUTPUT_2024 = DATA_DIR / "2024_gba_week.csv"

# 第一周的日期范围
FIRST_WEEK_2021 = [20210315, 20210316, 20210317, 20210318, 20210319, 20210320, 20210321]
FIRST_WEEK_2024 = [20240318, 20240319, 20240320, 20240321, 20240322, 20240323, 20240324]

print("=" * 80)
print("GBA第一周流量数据提取")
print("=" * 80)

# 1. 加载GBA格网列表
print(f"\n[*] 加载GBA格网元数据: {GRID_METADATA}")
grid_meta = pd.read_csv(GRID_METADATA)
gba_grids = set(grid_meta['grid_id'].values)
print(f"    ✓ GBA格网数量: {len(gba_grids):,}")

print(f"\n    包含城市: {', '.join(grid_meta['city_name'].unique())}")

# 2. 提取2021年第一周数据
print(f"\n[*] 提取2021年第一周数据 (3月15-21日)")
print(f"    输入: {INPUT_2021}")

# 使用chunker读取大文件
chunks = []
for chunk in pd.read_csv(INPUT_2021, chunksize=1000000):
    # 筛选第一周的记录
    week_chunk = chunk[chunk['date_dt'].isin(FIRST_WEEK_2021)]

    # 筛选GBA格网之间的流动（o_grid和d_grid都在GBA中）
    gba_chunk = week_chunk[
        week_chunk['o_grid_500'].isin(gba_grids) &
        week_chunk['d_grid_500'].isin(gba_grids)
    ]

    if len(gba_chunk) > 0:
        chunks.append(gba_chunk)

    print(f"    已处理: {len(chunk):,} 行, 筛选后: {len(gba_chunk):,} 行", end='\r')

if chunks:
    df_2021_week = pd.concat(chunks, ignore_index=True)
    df_2021_week.to_csv(OUTPUT_2021, index=False)
    print(f"\n    ✓ 保存到: {OUTPUT_2021}")
    print(f"    ✓ 2021年GBA第一周记录数: {len(df_2021_week):,}")
else:
    print("\n    ⚠ 未找到符合条件的数据")

# 3. 提取2024年第一周数据
print(f"\n[*] 提取2024年第一周数据 (3月18-24日)")
print(f"    输入: {INPUT_2024}")

chunks = []
for chunk in pd.read_csv(INPUT_2024, chunksize=1000000):
    # 筛选第一周的记录
    week_chunk = chunk[chunk['date_dt'].isin(FIRST_WEEK_2024)]

    # 筛选GBA格网之间的流动
    gba_chunk = week_chunk[
        week_chunk['o_grid_500'].isin(gba_grids) &
        week_chunk['d_grid_500'].isin(gba_grids)
    ]

    if len(gba_chunk) > 0:
        chunks.append(gba_chunk)

    print(f"    已处理: {len(chunk):,} 行, 筛选后: {len(gba_chunk):,} 行", end='\r')

if chunks:
    df_2024_week = pd.concat(chunks, ignore_index=True)
    df_2024_week.to_csv(OUTPUT_2024, index=False)
    print(f"\n    ✓ 保存到: {OUTPUT_2024}")
    print(f"    ✓ 2024年GBA第一周记录数: {len(df_2024_week):,}")
else:
    print("\n    ⚠ 未找到符合条件的数据")

# 4. 验证数据
print("\n[*] 验证提取的数据")

if Path(OUTPUT_2021).exists():
    df_2021_verify = pd.read_csv(OUTPUT_2021)
    print(f"\n2021_gba_week.csv:")
    print(f"  - 总记录数: {len(df_2021_verify):,}")
    print(f"  - 日期范围: {df_2021_verify['date_dt'].min()} ~ {df_2021_verify['date_dt'].max()}")
    print(f"  - 起始格网数: {df_2021_verify['o_grid_500'].nunique():,}")
    print(f"  - 目的格网数: {df_2021_verify['d_grid_500'].nunique():,}")
    print(f"  - 总流量: {df_2021_verify['num_total'].sum():,}")

if Path(OUTPUT_2024).exists():
    df_2024_verify = pd.read_csv(OUTPUT_2024)
    print(f"\n2024_gba_week.csv:")
    print(f"  - 总记录数: {len(df_2024_verify):,}")
    print(f"  - 日期范围: {df_2024_verify['date_dt'].min()} ~ {df_2024_verify['date_dt'].max()}")
    print(f"  - 起始格网数: {df_2024_verify['o_grid_500'].nunique():,}")
    print(f"  - 目的格网数: {df_2024_verify['d_grid_500'].nunique():,}")
    print(f"  - 总流量: {df_2024_verify['num_total'].sum():,}")

print("\n" + "=" * 80)
print("提取完成！")
print("=" * 80)
