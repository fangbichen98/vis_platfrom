#!/usr/bin/env python3
"""
深圳市内部流动自动格网标注脚本

工作流程：
1. 加载深圳市格网列表
2. 从深圳CSV数据中提取格网的24小时流量（只统计o_grid和d_grid都在深圳的记录）
3. 分析流量趋势（稳定/增长/衰减）
4. 分析空间模式（静态/聚集/扩散）
5. 判断类型（1-9）
6. 保存标签到CSV文件
"""

import pandas as pd
import numpy as np
import math
import json
from pathlib import Path
from typing import Dict, Set, Tuple, Optional
from tqdm import tqdm

# 配置
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
LABELS_DIR = SCRIPT_DIR / "labels"
SHENZHEN_DATA_2021 = DATA_DIR / "2021_shenzhen_week.csv"
SHENZHEN_DATA_2024 = DATA_DIR / "2024_shenzhen_week.csv"
METADATA_PATH = SCRIPT_DIR.parent / "analysis/PRD-Mobility-Change-Pattern/data/grid_metadata/PRD_grid_metadata.csv"
ELLIPSES_PATH = SCRIPT_DIR / "appdata" / "ellipses.json"
OUTPUT_LABELS = LABELS_DIR / "labels_shenzhen_internal_all.csv"
OUTPUT_EDGE_CASES = LABELS_DIR / "edge_cases_shenzhen.csv"

# 确保目录存在
LABELS_DIR.mkdir(parents=True, exist_ok=True)

# 标签映射
LABEL_MAP = {
    1: "稳定静态型", 2: "稳定聚集型", 3: "稳定扩散型",
    4: "增长静态型", 5: "增长聚集型", 6: "增长扩散型",
    7: "衰减静态型", 8: "衰减聚集型", 9: "衰减扩散型"
}


class ShenzhenAutoLabeler:
    def __init__(self):
        self.shenzhen_grids: Set[int] = set()
        self.ellipses_data: Optional[dict] = None
        self.hourly_data_2021: Dict[int, np.ndarray] = {}
        self.hourly_data_2024: Dict[int, np.ndarray] = {}

    def load_shenzhen_grids(self):
        """加载深圳市格网列表"""
        print(f"[*] 加载深圳格网列表: {METADATA_PATH}")
        df = pd.read_csv(METADATA_PATH)
        shenzhen_df = df[df['city_name'] == '深圳市']
        self.shenzhen_grids = set(shenzhen_df['grid_id'].values)
        print(f"    ✓ 深圳格网数量: {len(self.shenzhen_grids)}")
        return self.shenzhen_grids

    def load_ellipses(self):
        """加载椭圆数据"""
        print(f"[*] 加载椭圆数据: {ELLIPSES_PATH}")
        if not ELLIPSES_PATH.exists():
            print(f"    ✗ 警告: 椭圆数据文件不存在，空间模式判断将全部返回'static'")
            return

        try:
            with open(ELLIPSES_PATH, 'r') as f:
                self.ellipses_data = json.load(f)
            print(f"    ✓ 椭圆数据加载成功")
        except Exception as e:
            print(f"    ✗ 警告: 加载椭圆数据失败: {e}")
            self.ellipses_data = None

    def load_shenzhen_flow_data(self, year: int) -> pd.DataFrame:
        """加载深圳流动数据并过滤内部流动"""
        if year == 2021:
            filepath = SHENZHEN_DATA_2021
        else:
            filepath = SHENZHEN_DATA_2024

        print(f"[*] 加载 {year} 年深圳数据: {filepath}")

        # 分块读取数据
        chunks = []
        for chunk in pd.read_csv(filepath, chunksize=1000000):
            # 只保留深圳内部流动（o_grid和d_grid都在深圳）
            chunk_filtered = chunk[
                chunk['o_grid_500'].isin(self.shenzhen_grids) &
                chunk['d_grid_500'].isin(self.shenzhen_grids)
            ]
            chunks.append(chunk_filtered)

        df = pd.concat(chunks, ignore_index=True)
        print(f"    ✓ {year}年深圳内部流动记录数: {len(df):,}")
        return df

    def compute_hourly_for_grid(self, grid_id: int, df: pd.DataFrame) -> np.ndarray:
        """
        计算单个格网的24小时流量数据

        Args:
            grid_id: 格网ID
            df: 流量数据DataFrame

        Returns:
            numpy array shape (7, 24), 表示7周×24小时的流量
        """
        # 获取该格网的所有流入和流出记录
        inflow = df[df['d_grid_500'] == grid_id].copy()
        outflow = df[df['o_grid_500'] == grid_id].copy()

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

        return hourly_array

    def compute_all_hourly_data(self):
        """计算所有深圳格网的24小时流量数据"""
        print("\n[*] 计算格网24小时流量数据...")

        # 加载两年数据
        df_2021 = self.load_shenzhen_flow_data(2021)
        df_2024 = self.load_shenzhen_flow_data(2024)

        # 计算每个格网的hourly数据
        print(f"[*] 处理2021年数据...")
        for grid_id in tqdm(self.shenzhen_grids, desc="2021年"):
            self.hourly_data_2021[grid_id] = self.compute_hourly_for_grid(grid_id, df_2021)

        print(f"[*] 处理2024年数据...")
        for grid_id in tqdm(self.shenzhen_grids, desc="2024年"):
            self.hourly_data_2024[grid_id] = self.compute_hourly_for_grid(grid_id, df_2024)

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

    def analyze_trend(self, grid_id: int) -> str:
        """
        分析流量趋势：稳定/增长/衰减

        判断逻辑（按照AUTO_LABEL_README.md的定义）：
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

    def get_ellipse_area(self, grid_id: int, year: int) -> Optional[float]:
        """获取格网某年的椭圆面积"""
        if not self.ellipses_data:
            return None

        try:
            years_data = self.ellipses_data.get("years", {})
            year_data = years_data.get(str(year), [])

            for item in year_data:
                if item.get("grid_id") == grid_id:
                    axes = item.get("axes", {})
                    a = axes.get("a", 0)
                    b = axes.get("b", 0)
                    # 椭圆面积 = π * a * b
                    return math.pi * a * b
        except Exception as e:
            pass

        return None

    def analyze_spatial_pattern(self, grid_id: int) -> str:
        """
        分析空间模式：静态/聚集/扩散

        判断逻辑（基于2021→2024椭圆面积变化，按照AUTO_LABEL_README.md定义）：
        - 静态：椭圆面积变化在 ±20% 以内（基本不变）
        - 扩散：椭圆面积增长超过 20%（向外扩展）
        - 聚集：椭圆面积减少超过 20%（向内收缩）

        Returns: "static", "aggregation", "diffusion"
        """
        # 尝试使用椭圆数据判断
        area_2021 = self.get_ellipse_area(grid_id, 2021)
        area_2024 = self.get_ellipse_area(grid_id, 2024)

        if area_2021 and area_2024:
            if area_2021 == 0:
                return "diffusion" if area_2024 > 0 else "static"

            change_ratio = (area_2024 - area_2021) / area_2021

            # 按照README的定义：20%阈值
            if change_ratio > 0.20:  # 面积增长超过20% -> 扩散
                return "diffusion"
            elif change_ratio < -0.20:  # 面积减少超过20% -> 聚集
                return "aggregation"
            else:
                return "static"

        # 如果没有椭圆数据，返回默认值
        return "static"

    def predict_label(self, grid_id: int) -> Tuple[int, str, dict]:
        """
        预测格网类型

        Returns: (label_number, label_name, metadata)
        """
        try:
            # 分析趋势
            trend = self.analyze_trend(grid_id)  # stable/growth/decay

            # 分析空间模式（基于椭圆数据）
            spatial = self.analyze_spatial_pattern(grid_id)  # static/aggregation/diffusion

            # 映射到标签
            trend_map = {"stable": 0, "growth": 3, "decay": 6}
            spatial_map = {"static": 1, "aggregation": 2, "diffusion": 3}

            label = trend_map[trend] + spatial_map[label]

            # 判断是否是边缘案例
            edge_info = self._check_edge_case(grid_id, trend, spatial)

            return label, LABEL_MAP.get(label, f"未知类型{label}"), edge_info

        except Exception as e:
            print(f"  [分析错误] {e}")
            return 0, "其他", {"is_edge_case": False, "error": str(e)}

    def _check_edge_case(self, grid_id: int, trend: str, spatial: str) -> dict:
        """检查是否是边缘案例（按照README定义的阈值）"""

        # 获取流量和椭圆数据
        total_2021 = self.get_daily_total(self.hourly_data_2021.get(grid_id, np.zeros((7, 24))))
        total_2024 = self.get_daily_total(self.hourly_data_2024.get(grid_id, np.zeros((7, 24))))

        area_2021 = self.get_ellipse_area(grid_id, 2021)
        area_2024 = self.get_ellipse_area(grid_id, 2024)

        # 流量趋势边缘检测（15%阈值 ± 3%）
        is_edge_trend = False
        if total_2021 > 0:
            flow_change_ratio = (total_2024 - total_2021) / total_2021
            # 如果在12%-18%之间（阈值15%±3%），认为是边缘
            is_edge_trend = 0.12 < abs(flow_change_ratio) < 0.18

        # 空间模式边缘检测（20%阈值 ± 3%）
        is_edge_spatial = False
        if area_2021 and area_2021 > 0:
            area_change_ratio = (area_2024 - area_2021) / area_2021
            # 如果在17%-23%之间（阈值20%±3%），认为是边缘
            is_edge_spatial = 0.17 < abs(area_change_ratio) < 0.23

        is_edge = is_edge_trend or is_edge_spatial

        return {
            "is_edge_case": is_edge,
            "edge_reason": {
                "trend_near_threshold": is_edge_trend,
                "spatial_near_threshold": is_edge_spatial,
                "flow_change_ratio": ((total_2024 - total_2021) / total_2021 * 100) if total_2021 > 0 else None,
                "flow_threshold": 15.0,
                "area_change_ratio": ((area_2024 - area_2021) / area_2021 * 100) if area_2021 and area_2021 > 0 else None,
                "area_threshold": 20.0
            },
            "confidence": {
                "trend": trend,
                "spatial": spatial
            }
        }

    def run_batch(self, max_count: int = None):
        """批量标注所有深圳格网"""
        print("=" * 80)
        print("深圳市内部流动自动标注")
        print("=" * 80)

        # 加载数据
        self.load_shenzhen_grids()
        self.load_ellipses()
        self.compute_all_hourly_data()

        # 准备输出：不排序，直接处理格网
        print(f"\n[*] 准备处理格网（不排序）...")
        grids_to_process = list(self.shenzhen_grids)

        if max_count:
            grids_to_process = grids_to_process[:max_count]
            print(f"\n[*] 选择前 {max_count} 个格网")
        else:
            print(f"\n[*] 处理所有 {len(grids_to_process)} 个格网")

        # 统计信息
        stats = {
            "total": 0,
            "success": 0,
            "edge_cases": [],
            "errors": [],
            "label_distribution": {i: 0 for i in range(1, 10)}
        }

        # 边缘案例CSV文件
        import csv
        edge_csv_file = open(OUTPUT_EDGE_CASES, 'w', newline='', encoding='utf-8')
        edge_csv_writer = csv.writer(edge_csv_file)
        edge_csv_writer.writerow([
            'grid_id', 'label', 'label_name',
            'flow_2021', 'flow_2024', 'flow_change_ratio', 'flow_threshold',
            'area_change_ratio', 'area_threshold',
            'edge_trend', 'edge_spatial', 'reason'
        ])

        # 主处理循环
        print("\n[*] 开始批量标注...\n")

        for i, grid_id in enumerate(tqdm(grids_to_process, desc="标注进度")):
            stats["total"] += 1

            try:
                # 映射到标签
                trend = self.analyze_trend(grid_id)
                spatial = self.analyze_spatial_pattern(grid_id)

                trend_map = {"stable": 0, "growth": 3, "decay": 6}
                spatial_map = {"static": 1, "aggregation": 2, "diffusion": 3}

                label = trend_map[trend] + spatial_map[spatial]
                label_name = LABEL_MAP.get(label, f"未知类型{label}")

                if label == 0:
                    continue

                stats["success"] += 1
                stats["label_distribution"][label] += 1

                # 检查边缘案例
                edge_info = self._check_edge_case(grid_id, trend, spatial)

                if edge_info.get("is_edge_case", False):
                    stats["edge_cases"].append(grid_id)
                    reason = edge_info.get("edge_reason", {})

                    total_2021 = self.get_daily_total(self.hourly_data_2021[grid_id])
                    total_2024 = self.get_daily_total(self.hourly_data_2024[grid_id])

                    flow_ratio = reason.get('flow_change_ratio') or 0
                    area_ratio = reason.get('area_change_ratio') or 0
                    edge_csv_writer.writerow([
                        grid_id,
                        label,
                        label_name,
                        f"{total_2021:.1f}",
                        f"{total_2024:.1f}",
                        f"{flow_ratio:.1f}%",
                        f"{reason.get('flow_threshold', 0):.1f}%",
                        f"{area_ratio:.1f}%",
                        f"{reason.get('area_threshold', 0):.1f}%",
                        reason.get('trend_near_threshold', False),
                        reason.get('spatial_near_threshold', False),
                        f"Trend:{'Y' if reason.get('trend_near_threshold') else 'N'} Spatial:{'Y' if reason.get('spatial_near_threshold') else 'N'}"
                    ])

            except Exception as e:
                stats["errors"].append({"grid_id": grid_id, "error": str(e)})

        edge_csv_file.close()

        # 保存标签文件
        print(f"\n[*] 保存标签到: {OUTPUT_LABELS}")
        labels_data = []
        for grid_id in grids_to_process:
            trend = self.analyze_trend(grid_id)
            spatial = self.analyze_spatial_pattern(grid_id)
            trend_map = {"stable": 0, "growth": 3, "decay": 6}
            spatial_map = {"static": 1, "aggregation": 2, "diffusion": 3}
            label = trend_map[trend] + spatial_map[spatial]

            if label != 0:
                labels_data.append({
                    'grid_id': grid_id,
                    'label': label
                })

        labels_df = pd.DataFrame(labels_data)
        labels_df.to_csv(OUTPUT_LABELS, index=False)
        print(f"    ✓ 保存了 {len(labels_df)} 个标签")

        # 输出统计信息
        print("\n" + "=" * 80)
        print(f"批次完成！")
        print(f"成功标注: {stats['success']}/{stats['total']}")
        print(f"边缘案例: {len(stats['edge_cases'])} 个 ({len(stats['edge_cases'])/stats['success']*100:.1f}%)")
        print(f"错误: {len(stats['errors'])} 个")

        print(f"\n标签分布:")
        for label_id in range(1, 10):
            count = stats['label_distribution'][label_id]
            if count > 0:
                print(f"  标签 {label_id} ({LABEL_MAP[label_id]}): {count} 个格网")

        if stats['edge_cases']:
            print(f"\n⚠ 边缘案例已保存到: {OUTPUT_EDGE_CASES}")
            print(f"  请人工检查这 {len(stats['edge_cases'])} 个格网的标签")

        print("=" * 80)

        return stats


def main():
    import sys

    count = None  # 默认处理所有格网
    if len(sys.argv) > 1:
        count = int(sys.argv[1])

    labeler = ShenzhenAutoLabeler()
    labeler.run_batch(max_count=count)


if __name__ == "__main__":
    main()
