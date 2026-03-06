#!/usr/bin/env python3
"""
提取深圳-东莞-惠州（深莞惠）第一周的流量数据

工作流程：
1. 加载PRD格网元数据，提取深莞惠格网ID列表
2. 从2021.csv和2024.csv中筛选深莞惠内部流动数据
3. 提取第一周的数据
4. 保存为 2021_sgh_week.csv 和 2024_sgh_week.csv
"""

import pandas as pd
from pathlib import Path

# 配置
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
GRID_METADATA = DATA_DIR / "grid_metadata" / "PRD_grid_metadata.csv"

# 输入文件
DATA_2021 = DATA_DIR / "2021.csv"
DATA_2024 = DATA_DIR / "2024.csv"

# 输出文件
OUTPUT_2021 = DATA_DIR / "2021_sgh_week.csv"
OUTPUT_2024 = DATA_DIR / "2024_sgh_week.csv"

def extract_sgh_week_data():
    """提取深莞惠第一周数据"""

    print("=" * 80)
    print("提取深圳-东莞-惠州第一周流量数据")
    print("=" * 80)

    # 1. 加载格网元数据
    print(f"\n[*] 加载格网元数据: {GRID_METADATA}")
    metadata_df = pd.read_csv(GRID_METADATA)

    # 提取深莞惠格网
    sgh_grids_df = metadata_df[metadata_df['city_name'].isin(['深圳市', '东莞市', '惠州市'])]
    sgh_grid_ids = set(sgh_grids_df['grid_id'].values)

    print(f"    ✓ 深莞惠格网总数: {len(sgh_grid_ids):,}")
    print(f"      - 深圳市: {len(sgh_grids_df[sgh_grids_df['city_name'] == '深圳市']):,}")
    print(f"      - 东莞市: {len(sgh_grids_df[sgh_grids_df['city_name'] == '东莞市']):,}")
    print(f"      - 惠州市: {len(sgh_grids_df[sgh_grids_df['city_name'] == '惠州市']):,}")

    # 2. 处理2021年数据
    print(f"\n[*] 处理2021年数据...")
    process_year(DATA_2021, OUTPUT_2021, sgh_grid_ids, 2021)

    # 3. 处理2024年数据
    print(f"\n[*] 处理2024年数据...")
    process_year(DATA_2024, OUTPUT_2024, sgh_grid_ids, 2024)

    print("\n" + "=" * 80)
    print("✓ 数据提取完成！")
    print("=" * 80)


def process_year(input_file: Path, output_file: Path, sgh_grid_ids: set, year: int):
    """
    处理单年数据

    Args:
        input_file: 输入CSV文件路径
        output_file: 输出CSV文件路径
        sgh_grid_ids: 深莞惠格网ID集合
        year: 年份
    """
    print(f"    - 读取数据: {input_file}")

    # 分块读取数据
    chunks = []
    total_rows = 0

    for chunk in pd.read_csv(input_file, chunksize=10000000):
        # 筛选深莞惠内部流动
        # 条件：起点和终点都在深莞惠格网集合中
        filtered = chunk[
            (chunk['o_grid_500'].isin(sgh_grid_ids)) &
            (chunk['d_grid_500'].isin(sgh_grid_ids))
        ]
        chunks.append(filtered)
        total_rows += len(filtered)
        print(f"      已处理: {total_rows:,} 行", end='\r')

    if not chunks:
        print(f"    ✗ 没有找到符合条件的数据")
        return

    df = pd.concat(chunks, ignore_index=True)
    print(f"\n    ✓ 筛选后数据量: {len(df):,} 行")

    # 获取第一周的数据
    print(f"    - 提取第一周数据...")
    # 获取所有唯一日期
    unique_dates = sorted(df['date_dt'].unique())
    print(f"      数据包含日期数: {len(unique_dates)}")

    # 取前7天作为第一周
    first_week_dates = unique_dates[:7]
    print(f"      第一周日期范围: {first_week_dates[0]} - {first_week_dates[-1]}")

    # 筛选第一周数据
    df_week = df[df['date_dt'].isin(first_week_dates)]
    print(f"      ✓ 第一周数据量: {len(df_week):,} 行")

    # 保存数据
    print(f"    - 保存数据到: {output_file}")
    df_week.to_csv(output_file, index=False)
    print(f"    ✓ 数据已保存 ({output_file.stat().st_size / (1024**3):.2f} GB)")


if __name__ == "__main__":
    extract_sgh_week_data()
