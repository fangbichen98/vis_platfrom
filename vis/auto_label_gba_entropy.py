#!/usr/bin/env python3
"""
粤港澳大湾区(GBA)内部流动自动格网标注脚本（基于Shannon Entropy版本）

工作流程：
1. 加载GBA格网列表
2. 从GBA CSV数据中提取格网的24小时流量和OD分布
3. 分析流量趋势（稳定/增长/衰减）- 基于流量总量
4. 分析空间模式（静态/聚集/扩散）- 基于Shannon Entropy变化
5. 判断类型（1-9）
6. 保存标签到CSV文件

Shannon Entropy方法：
- 熵 = 流量分布的均匀程度
- 高熵：流量分散到多个目的地 → 扩散
- 低熵：流量集中在少数目的地 → 聚集
- 熵增加：从2021到2024变得更扩散
- 熵减少：从2021到2024变得更聚集
"""

import pandas as pd
import numpy as np
import math
from pathlib import Path
from typing import Dict, Set, Tuple, Optional
from tqdm import tqdm

# 配置
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
LABELS_DIR = SCRIPT_DIR / "labels"
GBA_DATA_2021 = DATA_DIR / "2021_gba_week.csv"
GBA_DATA_2024 = DATA_DIR / "2024_gba_week.csv"
GBA_GRID_METADATA = DATA_DIR / "grid_metadata" / "PRD_grid_metadata.csv"
OUTPUT_LABELS = LABELS_DIR / "labels_gba_entropy.csv"
OUTPUT_EDGE_CASES = LABELS_DIR / "edge_cases_gba_entropy.csv"

# 熵变阈值（用于判断聚集/扩散）
ENTROPY_CHANGE_THRESHOLD = 0.15  # 15%变化视为显著

# 确保目录存在
LABELS_DIR.mkdir(parents=True, exist_ok=True)

# 标签映射
LABEL_MAP = {
    1: "稳定静态型", 2: "稳定聚集型", 3: "稳定扩散型",
    4: "增长静态型", 5: "增长聚集型", 6: "增长扩散型",
    7: "衰减静态型", 8: "衰减聚集型", 9: "衰减扩散型"
}


class GBAAutoLabelerEntropy:
    """基于Shannon Entropy的GBA自动标注器"""

    def __init__(self):
        self.gba_grids: Set[int] = set()
        self.hourly_data_2021: Dict[int, np.ndarray] = {}
        self.hourly_data_2024: Dict[int, np.ndarray] = {}
        # 新增：存储OD分布数据用于计算熵
        self.od_distribution_2021: Dict[int, Dict[int, float]] = {}  # grid_id -> {dest_grid: total_flow}
        self.od_distribution_2024: Dict[int, Dict[int, float]] = {}

    def load_gba_grids(self):
        """加载GBA格网列表"""
        print(f"[*] 加载GBA格网列表: {GBA_GRID_METADATA}")
        df = pd.read_csv(GBA_GRID_METADATA)
        self.gba_grids = set(df['grid_id'].values)
        print(f"    ✓ GBA格网数量: {len(self.gba_grids):,}")
        print(f"    ✓ 包含城市: {', '.join(df['city_name'].unique())}")
        return self.gba_grids

    def compute_hourly_and_od_for_grid(self, grid_id: int, df: pd.DataFrame) -> Tuple[np.ndarray, Dict[int, float]]:
        """
        计算单个格网的24小时流量数据和OD分布

        Args:
            grid_id: 格网ID
            df: 流量数据DataFrame

        Returns:
            (hourly_array, od_distribution)
            - hourly_array: shape (7, 24), 表示7周×24小时的流量
            - od_distribution: {destination_grid: total_flow}
        """
        # 获取该格网的所有流出记录
        outflow = df[df['o_grid_500'] == grid_id].copy()

        # 计算OD分布（用于熵计算）
        od_distribution = {}
        if len(outflow) > 0:
            od_agg = outflow.groupby('d_grid_500')['num_total'].sum()
            od_distribution = od_agg.to_dict()

        # 获取该格网的所有流入和流出记录（用于hourly数据）
        inflow = df[df['d_grid_500'] == grid_id].copy()

        # 按(date_dt, time)聚合流量
        inflow_agg = inflow.groupby(['date_dt', 'time'])['num_total'].sum().reset_index()
        outflow_agg = outflow.groupby(['date_dt', 'time'])['num_total'].sum().reset_index()

        # 合并流入和流出
        merged = pd.concat([
            inflow_agg.rename(columns={'num_total': 'inflow'}),
            outflow_agg.rename(columns={'num_total': 'outflow'})
        ])

        # 重新聚合
        total_agg = merged.groupby(['date_dt', 'time'])[['inflow', 'outflow']].sum().reset_index()
        total_agg['total'] = total_agg['inflow'] + total_agg['outflow']

        # 获取唯一的日期列表（最多7天）
        dates = sorted(total_agg['date_dt'].unique())

        # 初始化7×24的数组
        hourly_array = np.zeros((7, 24))

        for day_idx, date in enumerate(dates[:7]):  # 最多取7天
            day_data = total_agg[total_agg['date_dt'] == date]
            for _, row in day_data.iterrows():
                hour = int(row['time'])
                if 0 <= hour < 24:
                    hourly_array[day_idx, hour] = row['total']

        return hourly_array, od_distribution

    def load_gba_flow_data(self, year: int) -> pd.DataFrame:
        """加载GBA流动数据（CSV已经是GBA内部流动数据）"""
        if year == 2021:
            filepath = GBA_DATA_2021
        else:
            filepath = GBA_DATA_2024

        print(f"[*] 加载 {year} 年GBA数据: {filepath}")

        # 直接读取CSV，无需过滤（数据已经是GBA内部流动）
        df = pd.read_csv(filepath)
        print(f"    ✓ {year}年GBA内部流动记录数: {len(df):,}")
        return df

    def compute_all_hourly_data(self):
        """计算所有GBA格网的24小时流量数据和OD分布"""
        print("\n[*] 计算格网24小时流量数据和OD分布...")

        # 加载两年数据
        df_2021 = self.load_gba_flow_data(2021)
        df_2024 = self.load_gba_flow_data(2024)

        # 计算每个格网的数据
        print(f"[*] 处理2021年数据...")
        for grid_id in tqdm(self.gba_grids, desc="2021年"):
            hourly_array, od_dist = self.compute_hourly_and_od_for_grid(grid_id, df_2021)
            self.hourly_data_2021[grid_id] = hourly_array
            self.od_distribution_2021[grid_id] = od_dist

        print(f"[*] 处理2024年数据...")
        for grid_id in tqdm(self.gba_grids, desc="2024年"):
            hourly_array, od_dist = self.compute_hourly_and_od_for_grid(grid_id, df_2024)
            self.hourly_data_2024[grid_id] = hourly_array
            self.od_distribution_2024[grid_id] = od_dist

        print(f"    ✓ 完成！共处理 {len(self.hourly_data_2021)} 个格网")

    def get_daily_total(self, hourly_array: np.ndarray) -> float:
        """
        计算日均流量

        Args:
            hourly_array: shape (7, 24) 的数组

        Returns:
            日均流量
        """
        # 计算7天每天的总流量
        daily_totals = hourly_array.sum(axis=1)  # shape (7,)
        # 计算日均
        daily_average = daily_totals.sum() / 7.0
        return daily_average

    def calculate_shannon_entropy(self, od_distribution: Dict[int, float]) -> float:
        """
        计算流出流量的Shannon Entropy

        Args:
            od_distribution: {destination_grid: total_flow}

        Returns:
            Shannon Entropy值

        公式：H = -Σ(p_i * log(p_i))
        其中 p_i 是流向第i个目的地的比例
        """
        if not od_distribution:
            return 0.0

        # 计算总流量
        total_flow = sum(od_distribution.values())

        if total_flow == 0:
            return 0.0

        # 计算每个目的地的概率分布
        probabilities = [flow / total_flow for flow in od_distribution.values()]

        # 计算Shannon Entropy
        entropy = 0.0
        for p in probabilities:
            if p > 0:  # 避免log(0)
                entropy -= p * math.log(p)

        return entropy

    def normalize_entropy(self, entropy: float, num_destinations: int) -> float:
        """
        归一化熵值到[0, 1]区间

        Args:
            entropy: 原始熵值
            num_destinations: 目的地数量

        Returns:
            归一化后的熵值

        最大熵 = log(num_destinations)
        归一化熵 = entropy / log(num_destinations)
        """
        if num_destinations <= 1:
            return 0.0

        max_entropy = math.log(num_destinations)
        if max_entropy == 0:
            return 0.0

        return entropy / max_entropy

    def analyze_trend(self, grid_id: int) -> str:
        """
        分析流量趋势：稳定/增长/衰减

        判断逻辑：
        - 计算2021和2024的日均流量总量
        - 变化率 > +15% → 增长型
        - 变化率 < -15% → 衰减型
        - 其他 → 稳定型

        Returns: "stable", "growth", "decay"
        """
        if grid_id not in self.hourly_data_2021 or grid_id not in self.hourly_data_2024:
            return "stable"

        total_2021 = self.get_daily_total(self.hourly_data_2021[grid_id])
        total_2024 = self.get_daily_total(self.hourly_data_2024[grid_id])

        if total_2021 == 0:
            return "growth" if total_2024 > 0 else "stable"

        # 计算变化率
        change_ratio = (total_2024 - total_2021) / total_2021

        # 按照README的定义：15%阈值
        if change_ratio > 0.15:
            return "growth"
        elif change_ratio < -0.15:
            return "decay"
        else:
            return "stable"

    def analyze_spatial_pattern(self, grid_id: int) -> str:
        """
        分析空间模式：静态/聚集/扩散（基于Shannon Entropy）

        判断逻辑：
        1. 计算2021和2024的流出流量熵
        2. 归一化熵值（考虑目的地数量）
        3. 计算熵变化率

        - 熵显著增加（> 15%）→ 扩散（流量分布更均匀）
        - 熵显著减少（< -15%）→ 聚集（流量更集中）
        - 熵变化在 ±15% 以内 → 静态

        Returns: "static", "aggregation", "diffusion"
        """
        # 获取OD分布
        od_2021 = self.od_distribution_2021.get(grid_id, {})
        od_2024 = self.od_distribution_2024.get(grid_id, {})

        # 计算原始熵
        entropy_2021 = self.calculate_shannon_entropy(od_2021)
        entropy_2024 = self.calculate_shannon_entropy(od_2024)

        # 归一化熵值
        num_dest_2021 = len(od_2021)
        num_dest_2024 = len(od_2024)

        norm_entropy_2021 = self.normalize_entropy(entropy_2021, num_dest_2021)
        norm_entropy_2024 = self.normalize_entropy(entropy_2024, num_dest_2024)

        # 如果两年都没有数据，返回静态
        if num_dest_2021 == 0 and num_dest_2024 == 0:
            return "static"

        # 如果只有一年有数据
        if num_dest_2021 == 0:
            # 2021无流出，2024有 → 扩散
            return "diffusion" if num_dest_2024 > 0 else "static"

        if num_dest_2024 == 0:
            # 2024无流出，2021有 → 聚集
            return "aggregation"

        # 计算熵变化率
        if norm_entropy_2021 == 0:
            # 2021熵为0（完全集中），2024熵>0 → 扩散
            return "diffusion" if norm_entropy_2024 > 0 else "static"

        change_ratio = (norm_entropy_2024 - norm_entropy_2021) / norm_entropy_2021

        # 基于熵变化判断空间模式
        if change_ratio > ENTROPY_CHANGE_THRESHOLD:  # 熵增加 → 扩散
            return "diffusion"
        elif change_ratio < -ENTROPY_CHANGE_THRESHOLD:  # 熵减少 → 聚集
            return "aggregation"
        else:  # 熵变化不大 → 静态
            return "static"

    def predict_label(self, grid_id: int) -> Tuple[int, str, dict]:
        """
        预测格网类型

        Returns: (label_number, label_name, metadata)
        """
        try:
            # 分析趋势
            trend = self.analyze_trend(grid_id)  # stable/growth/decay

            # 分析空间模式（基于Shannon Entropy）
            spatial = self.analyze_spatial_pattern(grid_id)  # static/aggregation/diffusion

            # 映射到标签
            trend_map = {"stable": 0, "growth": 3, "decay": 6}
            spatial_map = {"static": 1, "aggregation": 2, "diffusion": 3}

            label = trend_map[trend] + spatial_map[spatial]

            # 判断是否是边缘案例
            edge_info = self._check_edge_case(grid_id, trend, spatial)

            # 添加熵信息到metadata
            edge_info['entropy'] = {
                'entropy_2021': self.calculate_shannon_entropy(self.od_distribution_2021.get(grid_id, {})),
                'entropy_2024': self.calculate_shannon_entropy(self.od_distribution_2024.get(grid_id, {})),
                'num_dest_2021': len(self.od_distribution_2021.get(grid_id, {})),
                'num_dest_2024': len(self.od_distribution_2024.get(grid_id, {}))
            }

            return label, LABEL_MAP.get(label, f"未知类型{label}"), edge_info

        except Exception as e:
            print(f"  [分析错误] {e}")
            return 0, "其他", {"is_edge_case": False, "error": str(e)}

    def _check_edge_case(self, grid_id: int, trend: str, spatial: str) -> dict:
        """检查是否是边缘案例（基于Shannon Entropy）"""

        # 获取流量数据
        total_2021 = self.get_daily_total(self.hourly_data_2021.get(grid_id, np.zeros((7, 24))))
        total_2024 = self.get_daily_total(self.hourly_data_2024.get(grid_id, np.zeros((7, 24))))

        # 获取熵数据
        entropy_2021 = self.calculate_shannon_entropy(self.od_distribution_2021.get(grid_id, {}))
        entropy_2024 = self.calculate_shannon_entropy(self.od_distribution_2024.get(grid_id, {}))

        num_dest_2021 = len(self.od_distribution_2021.get(grid_id, {}))
        num_dest_2024 = len(self.od_distribution_2024.get(grid_id, {}))

        # 归一化熵
        norm_entropy_2021 = self.normalize_entropy(entropy_2021, num_dest_2021)
        norm_entropy_2024 = self.normalize_entropy(entropy_2024, num_dest_2024)

        # 流量趋势边缘检测（15%阈值 ± 3%）
        is_edge_trend = False
        if total_2021 > 0:
            flow_change_ratio = (total_2024 - total_2021) / total_2021
            is_edge_trend = 0.12 < abs(flow_change_ratio) < 0.18

        # 空间模式边缘检测（熵变阈值 15% ± 3%）
        is_edge_spatial = False
        if norm_entropy_2021 > 0 and num_dest_2021 > 0:
            entropy_change_ratio = (norm_entropy_2024 - norm_entropy_2021) / norm_entropy_2021
            is_edge_spatial = 0.12 < abs(entropy_change_ratio) < 0.18

        is_edge = is_edge_trend or is_edge_spatial

        return {
            "is_edge_case": is_edge,
            "edge_reason": {
                "trend_near_threshold": is_edge_trend,
                "spatial_near_threshold": is_edge_spatial,
                "flow_change_ratio": ((total_2024 - total_2021) / total_2021 * 100) if total_2021 > 0 else None,
                "flow_threshold": 15.0,
                "entropy_change_ratio": ((norm_entropy_2024 - norm_entropy_2021) / norm_entropy_2021 * 100) if norm_entropy_2021 > 0 else None,
                "entropy_threshold": ENTROPY_CHANGE_THRESHOLD * 100
            },
            "confidence": {
                "trend": trend,
                "spatial": spatial
            }
        }

    def run_batch(self, max_count: int = None):
        """批量标注所有GBA格网"""
        print("=" * 80)
        print("粤港澳大湾区(GBA)内部流动自动标注（基于Shannon Entropy）")
        print("=" * 80)

        # 加载数据
        self.load_gba_grids()
        self.compute_all_hourly_data()

        # 准备输出：不排序，直接处理格网
        print(f"\n[*] 准备处理格网（不排序）...")
        grids_to_process = list(self.gba_grids)

        if max_count:
            grids_to_process = grids_to_process[:max_count]
            print(f"\n[*] 选择前 {max_count} 个格网")
        else:
            print(f"\n[*] 处理所有 {len(grids_to_process):,} 个格网")

        # 统计信息
        stats = {
            "total": 0,
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

        # 边缘案例CSV文件
        import csv
        edge_csv_file = open(OUTPUT_EDGE_CASES, 'w', newline='', encoding='utf-8')
        edge_csv_writer = csv.writer(edge_csv_file)
        edge_csv_writer.writerow([
            'grid_id', 'label', 'label_name',
            'flow_2021', 'flow_2024', 'flow_change_ratio', 'flow_threshold',
            'entropy_2021', 'entropy_2024', 'entropy_change_ratio', 'entropy_threshold',
            'num_dest_2021', 'num_dest_2024',
            'edge_trend', 'edge_spatial', 'reason'
        ])

        # 主处理循环
        print("\n[*] 开始批量标注...\n")

        for grid_id in tqdm(grids_to_process, desc="标注进度"):
            stats["total"] += 1

            try:
                # 预测标签
                label, label_name, edge_info = self.predict_label(grid_id)

                if label == 0:
                    continue

                stats["success"] += 1
                stats["label_distribution"][label] += 1

                # 记录熵统计
                entropy_info = edge_info.get('entropy', {})
                stats["entropy_stats"]["entropy_2021"].append(entropy_info.get('entropy_2021', 0))
                stats["entropy_stats"]["entropy_2024"].append(entropy_info.get('entropy_2024', 0))
                if entropy_info.get('entropy_2021', 0) > 0:
                    entropy_change = (entropy_info.get('entropy_2024', 0) - entropy_info.get('entropy_2021', 0)) / entropy_info.get('entropy_2021', 1)
                    stats["entropy_stats"]["entropy_changes"].append(entropy_change)

                # 检查边缘案例
                if edge_info.get("is_edge_case", False):
                    stats["edge_cases"].append(grid_id)
                    reason = edge_info.get("edge_reason", {})

                    total_2021 = self.get_daily_total(self.hourly_data_2021[grid_id])
                    total_2024 = self.get_daily_total(self.hourly_data_2024[grid_id])

                    entropy_2021 = entropy_info.get('entropy_2021', 0)
                    entropy_2024 = entropy_info.get('entropy_2024', 0)

                    flow_ratio = reason.get('flow_change_ratio') or 0
                    entropy_ratio = reason.get('entropy_change_ratio') or 0

                    edge_csv_writer.writerow([
                        grid_id,
                        label,
                        label_name,
                        f"{total_2021:.1f}",
                        f"{total_2024:.1f}",
                        f"{flow_ratio:.1f}%",
                        f"{reason.get('flow_threshold', 0):.1f}%",
                        f"{entropy_2021:.3f}",
                        f"{entropy_2024:.3f}",
                        f"{entropy_ratio:.1f}%",
                        f"{reason.get('entropy_threshold', 0):.1f}%",
                        entropy_info.get('num_dest_2021', 0),
                        entropy_info.get('num_dest_2024', 0),
                        reason.get('trend_near_threshold', False),
                        reason.get('spatial_near_threshold', False),
                        f"Trend:{'Y' if reason.get('trend_near_threshold') else 'N'} Entropy:{'Y' if reason.get('spatial_near_threshold') else 'N'}"
                    ])

            except Exception as e:
                stats["errors"].append({"grid_id": grid_id, "error": str(e)})

        edge_csv_file.close()

        # 保存标签文件
        print(f"\n[*] 保存标签到: {OUTPUT_LABELS}")
        labels_data = []
        for grid_id in grids_to_process:
            label, label_name, _ = self.predict_label(grid_id)
            if label != 0:
                labels_data.append({
                    'grid_id': grid_id,
                    'label': label
                })

        labels_df = pd.DataFrame(labels_data)
        labels_df.to_csv(OUTPUT_LABELS, index=False)
        print(f"    ✓ 保存了 {len(labels_df):,} 个标签")

        # 输出统计信息
        print("\n" + "=" * 80)
        print(f"批次完成！")
        print(f"成功标注: {stats['success']:,}/{stats['total']:,}")
        print(f"边缘案例: {len(stats['edge_cases']):,} 个 ({len(stats['edge_cases'])/stats['success']*100:.1f}%)")
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

        if stats['edge_cases']:
            print(f"\n⚠ 边缘案例已保存到: {OUTPUT_EDGE_CASES}")
            print(f"  请人工检查这 {len(stats['edge_cases']):,} 个格网的标签")

        print("=" * 80)

        return stats


def main():
    import sys

    count = None  # 默认处理所有格网
    if len(sys.argv) > 1:
        count = int(sys.argv[1])

    labeler = GBAAutoLabelerEntropy()
    labeler.run_batch(max_count=count)


if __name__ == "__main__":
    main()
