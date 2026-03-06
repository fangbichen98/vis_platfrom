#!/usr/bin/env python3
"""
粤港澳大湾区(GBA)内部流动自动格网标注脚本（基于Shannon Entropy版本 - 优化版）

优化版本：使用向量化操作和数据预处理加速
"""

import pandas as pd
import numpy as np
import math
import pickle
from pathlib import Path
from typing import Dict, Set, Tuple
from tqdm import tqdm

# 配置
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
LABELS_DIR = SCRIPT_DIR / "labels"
CACHE_DIR = SCRIPT_DIR / "cache"
GBA_DATA_2021 = DATA_DIR / "2021_gba_week.csv"
GBA_DATA_2024 = DATA_DIR / "2024_gba_week.csv"
GBA_GRID_METADATA = DATA_DIR / "grid_metadata" / "PRD_grid_metadata.csv"
OUTPUT_LABELS = LABELS_DIR / "labels_gba_entropy.csv"
OUTPUT_EDGE_CASES = LABELS_DIR / "edge_cases_gba_entropy.csv"

# 确保缓存目录存在
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# 熵变阈值（用于判断聚集/扩散）
ENTROPY_CHANGE_THRESHOLD = 0.05  # 5%变化视为显著

# 确保目录存在
LABELS_DIR.mkdir(parents=True, exist_ok=True)

# 标签映射
LABEL_MAP = {
    1: "稳定静态型", 2: "稳定聚集型", 3: "稳定扩散型",
    4: "增长静态型", 5: "增长聚集型", 6: "增长扩散型",
    7: "衰减静态型", 8: "衰减聚集型", 9: "衰减扩散型"
}


class GBAAutoLabelerEntropyOptimized:
    """基于Shannon Entropy的GBA自动标注器（优化版）"""

    def __init__(self):
        self.gba_grids: Set[int] = set()
        self.grid_metadata: pd.DataFrame = None

    def load_gba_grids(self):
        """加载GBA格网列表"""
        print(f"[*] 加载GBA格网列表: {GBA_GRID_METADATA}")
        self.grid_metadata = pd.read_csv(GBA_GRID_METADATA)
        self.gba_grids = set(self.grid_metadata['grid_id'].values)
        print(f"    ✓ GBA格网数量: {len(self.gba_grids):,}")
        print(f"    ✓ 包含城市: {', '.join(self.grid_metadata['city_name'].unique())}")
        return self.gba_grids

    def show_cache_info(self):
        """显示缓存信息"""
        print("[*] 缓存信息:")
        for year in [2021, 2024]:
            cache_file = CACHE_DIR / f"processed_{year}.pkl"
            if cache_file.exists():
                size_mb = cache_file.stat().st_size / (1024 * 1024)
                print(f"  {year}年: ✓ 存在 ({size_mb:.1f} MB)")
                # 尝试读取格网数量
                try:
                    with open(cache_file, 'rb') as f:
                        data = pickle.load(f)
                    print(f"      - 缓存格网数: {len(data['daily_totals']):,}")
                except:
                    print(f"      - ⚠ 无法读取详细信息")
            else:
                print(f"  {year}年: ✗ 不存在")

    def preprocess_data(self, filepath: Path, year: int, use_cache: bool = True) -> Tuple[pd.DataFrame, Dict[int, float], Dict[int, Dict[int, float]], Dict[int, np.ndarray]]:
        """
        预处理数据，计算每个格网的日均流量、OD分布和hourly数据
        支持缓存机制避免重复计算

        Args:
            filepath: 原始数据文件路径
            year: 年份
            use_cache: 是否使用缓存（默认True）

        Returns:
            (hourly_df, daily_totals, od_distributions, hourly_arrays)
            - hourly_df: 按(grid_id, date_dt, time)聚合的流量DataFrame
            - daily_totals: {grid_id: daily_average}
            - od_distributions: {grid_id: {dest_grid: total_flow}}
            - hourly_arrays: {grid_id: (7, 24) array}
        """
        # 生成缓存文件路径
        cache_file = CACHE_DIR / f"processed_{year}.pkl"

        # 尝试加载缓存
        if use_cache and cache_file.exists():
            print(f"[*] 从缓存加载 {year} 年数据: {cache_file}")
            try:
                with open(cache_file, 'rb') as f:
                    cached_data = pickle.load(f)
                print(f"    ✓ 成功加载缓存（{len(cached_data['daily_totals']):,} 个格网）")
                return cached_data['hourly'], cached_data['daily_totals'], cached_data['od_distributions'], cached_data['hourly_arrays']
            except Exception as e:
                print(f"    ⚠ 缓存加载失败: {e}，将重新处理数据")

        # 缓存不存在或加载失败，进行预处理
        print(f"[*] 预处理 {year} 年数据: {filepath}")

        # 分块读取数据
        print(f"    - 读取原始数据...")
        chunks = []
        for chunk in pd.read_csv(filepath, chunksize=10000000):
            chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True)
        print(f"    ✓ 读取了 {len(df):,} 行数据")

        # 1. 计算hourly数据（流入+流出）
        print(f"    - 计算hourly数据...")
        inflow = df.groupby(['d_grid_500', 'date_dt', 'time'])['num_total'].sum().reset_index()
        inflow = inflow.rename(columns={'d_grid_500': 'grid_id', 'num_total': 'inflow'})

        outflow = df.groupby(['o_grid_500', 'date_dt', 'time'])['num_total'].sum().reset_index()
        outflow = outflow.rename(columns={'o_grid_500': 'grid_id', 'num_total': 'outflow'})

        # 合并流入和流出
        hourly = pd.merge(inflow, outflow, on=['grid_id', 'date_dt', 'time'], how='outer').fillna(0)
        hourly['total'] = hourly['inflow'] + hourly['outflow']

        # 2. 计算日均流量
        print(f"    - 计算日均流量...")
        daily_sums = hourly.groupby(['grid_id', 'date_dt'])['total'].sum().reset_index()
        daily_totals = daily_sums.groupby('grid_id')['total'].mean().to_dict()

        # 3. 计算OD分布（只计算流出）
        print(f"    - 计算OD分布...")
        od_agg = df.groupby(['o_grid_500', 'd_grid_500'])['num_total'].sum().reset_index()
        od_distributions = {}
        for grid_id in tqdm(self.gba_grids, desc=f"   {year}年OD", leave=False):
            grid_od = od_agg[od_agg['o_grid_500'] == grid_id]
            if len(grid_od) > 0:
                od_distributions[grid_id] = dict(zip(grid_od['d_grid_500'], grid_od['num_total']))
            else:
                od_distributions[grid_id] = {}

        # 4. 计算hourly数组 (7x24)
        print(f"    - 计算hourly数组...")
        hourly_arrays = {}
        dates = sorted(hourly['date_dt'].unique())

        for grid_id in tqdm(self.gba_grids, desc=f"   {year}年hourly", leave=False):
            grid_hourly = hourly[hourly['grid_id'] == grid_id]

            # 初始化7×24数组
            arr = np.zeros((7, 24))

            for day_idx, date in enumerate(dates[:7]):
                day_data = grid_hourly[grid_hourly['date_dt'] == date]
                for _, row in day_data.iterrows():
                    hour = int(row['time'])
                    if 0 <= hour < 24:
                        arr[day_idx, hour] = row['total']

            hourly_arrays[grid_id] = arr

        print(f"    ✓ 完成！")

        # 保存缓存
        print(f"    - 保存缓存到: {cache_file}")
        cached_data = {
            'hourly': hourly,
            'daily_totals': daily_totals,
            'od_distributions': od_distributions,
            'hourly_arrays': hourly_arrays
        }
        with open(cache_file, 'wb') as f:
            pickle.dump(cached_data, f)
        print(f"    ✓ 缓存已保存")

        return hourly, daily_totals, od_distributions, hourly_arrays

    def calculate_shannon_entropy(self, od_distribution: Dict[int, float]) -> float:
        """计算Shannon Entropy"""
        if not od_distribution:
            return 0.0

        total_flow = sum(od_distribution.values())
        if total_flow == 0:
            return 0.0

        probabilities = [flow / total_flow for flow in od_distribution.values()]
        entropy = 0.0
        for p in probabilities:
            if p > 0:
                entropy -= p * math.log(p)

        return entropy

    def normalize_entropy(self, entropy: float, num_destinations: int) -> float:
        """归一化熵值到[0, 1]区间"""
        if num_destinations <= 1:
            return 0.0

        max_entropy = math.log(num_destinations)
        if max_entropy == 0:
            return 0.0

        return entropy / max_entropy

    def analyze_trend(self, total_2021: float, total_2024: float) -> str:
        """分析流量趋势"""
        if total_2021 == 0:
            return "growth" if total_2024 > 0 else "stable"

        change_ratio = (total_2024 - total_2021) / total_2021

        if change_ratio > 0.15:
            return "growth"
        elif change_ratio < -0.15:
            return "decay"
        else:
            return "stable"

    def analyze_spatial_pattern(self, od_2021: Dict[int, float], od_2024: Dict[int, float]) -> str:
        """分析空间模式（基于Shannon Entropy）"""
        entropy_2021 = self.calculate_shannon_entropy(od_2021)
        entropy_2024 = self.calculate_shannon_entropy(od_2024)

        num_dest_2021 = len(od_2021)
        num_dest_2024 = len(od_2024)

        norm_entropy_2021 = self.normalize_entropy(entropy_2021, num_dest_2021)
        norm_entropy_2024 = self.normalize_entropy(entropy_2024, num_dest_2024)

        if num_dest_2021 == 0 and num_dest_2024 == 0:
            return "static"
        if num_dest_2021 == 0:
            return "diffusion" if num_dest_2024 > 0 else "static"
        if num_dest_2024 == 0:
            return "aggregation"

        if norm_entropy_2021 == 0:
            return "diffusion" if norm_entropy_2024 > 0 else "static"

        change_ratio = (norm_entropy_2024 - norm_entropy_2021) / norm_entropy_2021

        if change_ratio > ENTROPY_CHANGE_THRESHOLD:
            return "diffusion"
        elif change_ratio < -ENTROPY_CHANGE_THRESHOLD:
            return "aggregation"
        else:
            return "static"

    def clear_cache(self):
        """清除缓存文件"""
        print("[*] 清除缓存文件...")
        for year in [2021, 2024]:
            cache_file = CACHE_DIR / f"processed_{year}.pkl"
            if cache_file.exists():
                cache_file.unlink()
                print(f"    ✓ 已删除: {cache_file}")
            else:
                print(f"    - 不存在: {cache_file}")

    def run_batch(self, num_labels: int = None, use_cache: bool = True):
        """
        批量标注GBA格网

        Args:
            num_labels: 要生成的标签数量（None表示全部）
            use_cache: 是否使用缓存（默认True）
        """
        print("=" * 80)
        print("粤港澳大湾区(GBA)内部流动自动标注（基于Shannon Entropy - 优化版）")
        print("=" * 80)

        # 加载数据
        self.load_gba_grids()

        # 预处理数据
        _, daily_totals_2021, od_dist_2021, _ = self.preprocess_data(GBA_DATA_2021, 2021, use_cache)
        _, daily_totals_2024, od_dist_2024, _ = self.preprocess_data(GBA_DATA_2024, 2024, use_cache)

        # 确定要处理的格网列表
        grids_to_process = list(self.gba_grids)
        if num_labels is not None and num_labels < len(grids_to_process):
            print(f"\n[*] 限制标签数量为: {num_labels:,}")
            grids_to_process = sorted(grids_to_process)[:num_labels]

        # 统计信息
        stats = {
            "total": len(grids_to_process),
            "success": 0,
            "edge_cases": [],
            "errors": [],
            "label_distribution": {i: 0 for i in range(1, 10)},
            "entropy_stats": {
                "entropy_2021": [],
                "entropy_2024": [],
                "entropy_changes": []
            }
        }

        # 准备输出
        labels_data = []
        edge_cases_data = []

        # 主处理循环
        print(f"\n[*] 开始批量标注...")
        for grid_id in tqdm(grids_to_process, desc="标注进度"):
            try:
                total_2021 = daily_totals_2021.get(grid_id, 0)
                total_2024 = daily_totals_2024.get(grid_id, 0)
                od_2021 = od_dist_2021.get(grid_id, {})
                od_2024 = od_dist_2024.get(grid_id, {})

                # 分析趋势和空间模式
                trend = self.analyze_trend(total_2021, total_2024)
                spatial = self.analyze_spatial_pattern(od_2021, od_2024)

                # 映射到标签
                trend_map = {"stable": 0, "growth": 3, "decay": 6}
                spatial_map = {"static": 1, "aggregation": 2, "diffusion": 3}
                label = trend_map[trend] + spatial_map[spatial]

                labels_data.append({
                    'grid_id': grid_id,
                    'label': label
                })

                stats["success"] += 1
                stats["label_distribution"][label] += 1

                # 计算熵统计
                entropy_2021 = self.calculate_shannon_entropy(od_2021)
                entropy_2024 = self.calculate_shannon_entropy(od_2024)

                stats["entropy_stats"]["entropy_2021"].append(entropy_2021)
                stats["entropy_stats"]["entropy_2024"].append(entropy_2024)

                num_dest_2021 = len(od_2021)
                num_dest_2024 = len(od_2024)
                norm_entropy_2021 = self.normalize_entropy(entropy_2021, num_dest_2021)
                norm_entropy_2024 = self.normalize_entropy(entropy_2024, num_dest_2024)

                if norm_entropy_2021 > 0:
                    entropy_change = (norm_entropy_2024 - norm_entropy_2021) / norm_entropy_2021
                    stats["entropy_stats"]["entropy_changes"].append(entropy_change)

                # 检查边缘案例
                is_edge_trend = False
                if total_2021 > 0:
                    flow_change_ratio = (total_2024 - total_2021) / total_2021
                    is_edge_trend = 0.12 < abs(flow_change_ratio) < 0.18

                is_edge_spatial = False
                if norm_entropy_2021 > 0 and num_dest_2021 > 0:
                    entropy_change_ratio = (norm_entropy_2024 - norm_entropy_2021) / norm_entropy_2021
                    is_edge_spatial = 0.12 < abs(entropy_change_ratio) < 0.18

                is_edge = is_edge_trend or is_edge_spatial

                if is_edge:
                    edge_cases_data.append({
                        'grid_id': grid_id,
                        'label': label,
                        'label_name': LABEL_MAP[label],
                        'flow_2021': f"{total_2021:.1f}",
                        'flow_2024': f"{total_2024:.1f}",
                        'entropy_2021': f"{entropy_2021:.3f}",
                        'entropy_2024': f"{entropy_2024:.3f}",
                        'num_dest_2021': num_dest_2021,
                        'num_dest_2024': num_dest_2024,
                    })

            except Exception as e:
                stats["errors"].append({"grid_id": grid_id, "error": str(e)})

        # 保存标签文件
        print(f"\n[*] 保存标签到: {OUTPUT_LABELS}")
        labels_df = pd.DataFrame(labels_data)
        labels_df.to_csv(OUTPUT_LABELS, index=False)
        print(f"    ✓ 保存了 {len(labels_df):,} 个标签")

        # 保存边缘案例
        print(f"\n[*] 保存边缘案例到: {OUTPUT_EDGE_CASES}")
        edge_cases_df = pd.DataFrame(edge_cases_data)
        edge_cases_df.to_csv(OUTPUT_EDGE_CASES, index=False)
        print(f"    ✓ 保存了 {len(edge_cases_df):,} 个边缘案例")

        # 输出统计信息
        print("\n" + "=" * 80)
        print(f"批次完成！")
        print(f"成功标注: {stats['success']:,}/{stats['total']:,}")
        print(f"边缘案例: {len(edge_cases_data):,} 个")
        print(f"错误: {len(stats['errors']):,} 个")

        print(f"\n标签分布:")
        for label_id in range(1, 10):
            count = stats['label_distribution'][label_id]
            if count > 0:
                print(f"  标签 {label_id} ({LABEL_MAP[label_id]}): {count:,} 个格网")

        # 熵统计
        entropy_2021_list = stats["entropy_stats"]["entropy_2021"]
        entropy_2024_list = stats["entropy_stats"]["entropy_2024"]
        entropy_changes = stats["entropy_stats"]["entropy_changes"]

        if entropy_2021_list and entropy_2024_list:
            print(f"\nShannon Entropy统计:")
            print(f"  2021年:")
            print(f"    - 平均熵: {np.mean(entropy_2021_list):.3f}")
            print(f"    - 熵范围: {np.min(entropy_2021_list):.3f} ~ {np.max(entropy_2021_list):.3f}")
            print(f"  2024年:")
            print(f"    - 平均熵: {np.mean(entropy_2024_list):.3f}")
            print(f"    - 熵范围: {np.min(entropy_2024_list):.3f} ~ {np.max(entropy_2024_list):.3f}")

            if entropy_changes:
                print(f"  熵变化:")
                print(f"    - 平均变化: {np.mean(entropy_changes)*100:.1f}%")
                print(f"    - 扩散型格网（熵增加）: {sum(1 for x in entropy_changes if x > 0.15):,} 个")
                print(f"    - 聚集型格网（熵减少）: {sum(1 for x in entropy_changes if x < -0.15):,} 个")
                print(f"    - 静态型格网（熵稳定）: {sum(1 for x in entropy_changes if abs(x) <= 0.15):,} 个")

        print("=" * 80)

        return stats


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='粤港澳大湾区(GBA)内部流动自动标注脚本（基于Shannon Entropy - 优化版）'
    )
    parser.add_argument(
        '--num-labels', '-n',
        type=int,
        default=None,
        help='要生成的标签数量（默认：全部格网）'
    )
    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='不使用缓存，强制重新处理数据'
    )
    parser.add_argument(
        '--clear-cache',
        action='store_true',
        help='清除缓存文件后退出'
    )
    parser.add_argument(
        '--show-cache',
        action='store_true',
        help='显示缓存信息后退出'
    )

    args = parser.parse_args()

    labeler = GBAAutoLabelerEntropyOptimized()

    # 处理显示缓存信息
    if args.show_cache:
        labeler.show_cache_info()
        return

    # 处理清除缓存
    if args.clear_cache:
        labeler.clear_cache()
        return

    # 运行标注
    use_cache = not args.no_cache
    labeler.run_batch(num_labels=args.num_labels, use_cache=use_cache)


if __name__ == "__main__":
    main()
