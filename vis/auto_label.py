#!/usr/bin/env python3
"""
自动格网标注脚本（带截图保存）

工作流程：
1. 从队列获取当前格网ID
2. 获取格网的24小时数据
3. 分析流量趋势（稳定/增长/衰减）
4. 分析空间模式（静态/聚集/扩散）
5. 判断类型（1-9）
6. 通过API提交标签（会自动触发前端截图上传到服务器）
7. 从服务器复制截图并重命名（序号_格网ID_类型.jpg）
8. 自动跳到下一个格网
9. 循环直到完成
"""

import requests
import time
import math
import shutil
from pathlib import Path
from typing import Optional, Tuple

# 配置
BASE_URL = "http://127.0.0.1:8000"
API_BASE = f"{BASE_URL}/api"

# 目录配置
SCRIPT_DIR = Path(__file__).parent
LABELS_DIR = SCRIPT_DIR / "labels"
SHOTS_DIR = LABELS_DIR / "shots"  # 服务器保存截图的目录
SCREENSHOTS_DIR = LABELS_DIR / "screenshots"  # 我们按序号保存的截图目录

# 确保目录存在
SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

# 标签映射
LABEL_MAP = {
    1: "稳定静态型", 2: "稳定聚集型", 3: "稳定扩散型",
    4: "增长静态型", 5: "增长聚集型", 6: "增长扩散型",
    7: "衰减静态型", 8: "衰减聚集型", 9: "衰减扩散型"
}


class AutoLabeler:
    def __init__(self, base_url: str = BASE_URL, ellipses_path: Optional[str] = None):
        self.base_url = base_url
        self.api_base = f"{base_url}/api"
        self.session = requests.Session()

        # 加载椭圆数据（用于空间模式判断）
        self.ellipses_data = None
        if ellipses_path is None:
            # 默认路径
            script_dir = Path(__file__).parent
            ellipses_path = script_dir / "appdata" / "ellipses.json"

        if ellipses_path and Path(ellipses_path).exists():
            try:
                import json
                with open(ellipses_path, 'r') as f:
                    self.ellipses_data = json.load(f)
                print(f"[已加载椭圆数据: {ellipses_path}]")
            except Exception as e:
                print(f"[警告] 加载椭圆数据失败: {e}")

    def fetch_json(self, endpoint: str, method: str = "GET", data: Optional[dict] = None) -> dict:
        """通用API调用"""
        url = f"{self.api_base}/{endpoint}"
        if method == "GET":
            resp = self.session.get(url, timeout=10)
        elif method == "POST":
            headers = {"Content-Type": "application/json"}
            resp = self.session.post(url, json=data, headers=headers, timeout=10)
        else:
            raise ValueError(f"Unsupported method: {method}")

        resp.raise_for_status()
        return resp.json()

    def get_queue(self) -> dict:
        """获取当前标注队列"""
        return self.fetch_json("label_queue")

    def get_grid_hourly(self, grid_id: int) -> dict:
        """获取格网24小时数据"""
        return self.fetch_json(f"hourly?grid_id={grid_id}")

    def get_grid_flows(self, grid_id: int, year: str = "all") -> dict:
        """获取格网流量数据"""
        return self.fetch_json(f"flows?grid_id={grid_id}&year={year}&direction=both&topk=100")

    def submit_label(self, grid_id: int, label: int) -> dict:
        """提交标签"""
        return self.fetch_json("label", method="POST", data={
            "grid_id": grid_id,
            "label": label
        })

    def advance_queue(self) -> dict:
        """前进到下一个格网"""
        return self.fetch_json("label_queue/advance", method="POST")

    def copy_and_rename_screenshot(self, index: int, grid_id: int, label: int, label_name: str):
        """从服务器复制截图并重命名"""
        # 服务器上的截图文件名格式: {grid_id}-{label}.jpg
        source_filename = f"{grid_id}-{label}.jpg"
        source_path = SHOTS_DIR / source_filename

        # 新文件名格式: {序号:03d}_{grid_id}_{label}{label_name}.jpg
        target_filename = f"{index:03d}_{grid_id}_{label}{label_name}.jpg"
        target_path = SCREENSHOTS_DIR / target_filename

        if source_path.exists():
            try:
                shutil.copy2(source_path, target_path)
                print(f"  ✓ 截图已保存: {target_filename}")
            except Exception as e:
                print(f"  ⚠ 截图复制失败: {e}")
        else:
            print(f"  ⚠ 服务器截图不存在: {source_filename}")

    def analyze_trend(self, hourly_data: dict) -> str:
        """
        分析流量趋势：稳定/增长/衰减

        判断逻辑（基于视觉感受，主要看绝对变化量）：
        - 视觉感受主要基于y轴的绝对值变化
        - 对于不同流量级别，设定不同的绝对变化阈值
        - 百分比变化仅在极端情况下作为辅助判断（>100%且绝对值也够大）
        - 特殊情况：如果绝对变化量不明显且曲线形状相似，判为稳定

        Returns: "stable", "growth", "decay"
        """
        if not hourly_data or "2021" not in hourly_data or "2024" not in hourly_data:
            return "stable"  # 默认

        # 计算日均总量
        def get_daily_total(year_data):
            if not year_data or "total" not in year_data:
                return 0
            weeks = year_data["total"][:1]  # 取前1周
            if not weeks:
                return 0
            # 计算24小时的平均值
            hourly_avg = []
            for h in range(24):
                total = sum((week[h] if h < len(week) else 0) for week in weeks)
                hourly_avg.append(total / 1)
            return sum(hourly_avg), hourly_avg

        total_2021, hourly_2021 = get_daily_total(hourly_data["2021"])
        total_2024, hourly_2024 = get_daily_total(hourly_data["2024"])

        if total_2021 == 0:
            return "growth" if total_2024 > 200 else "stable"

        abs_change = total_2024 - total_2021
        ratio_change = abs_change / total_2021

        # 基于视觉感受的判断逻辑
        # 核心思想：在图表上，y轴的刻度是固定的绝对值
        # 所以视觉感受主要取决于绝对变化量，而非百分比

        # 根据流量基数设置不同的阈值
        if total_2021 > 2000:
            # 超大流量格网：绝对变化需要超过1000才算明显
            threshold = 1000
        elif total_2021 > 1000:
            # 大流量格网：绝对变化需要超过900才算明显
            threshold = 900
        elif total_2021 > 500:
            # 中大流量格网：绝对变化需要超过700
            threshold = 700
        elif total_2021 > 200:
            # 中等流量格网：绝对变化需要超过400
            threshold = 400
        elif total_2021 > 100:
            # 中小流量格网：绝对变化需要超过250
            threshold = 250
        else:
            # 小流量格网：绝对变化需要超过200
            threshold = 200

        # 主要基于绝对变化判断
        if abs_change > threshold:
            return "growth"
        elif abs_change < -threshold:
            return "decay"
        else:
            # 绝对变化不明显时，检查曲线形状相似性
            # 如果形状相似，视觉上看起来更稳定
            if len(hourly_2021) == 24 and len(hourly_2024) == 24 and total_2021 > 0 and total_2024 > 0:
                import statistics
                try:
                    # 归一化到0-1范围
                    min_2021, max_2021 = min(hourly_2021), max(hourly_2021)
                    min_2024, max_2024 = min(hourly_2024), max(hourly_2024)

                    if max_2021 > min_2021 and max_2024 > min_2024:
                        norm_2021 = [(h - min_2021) / (max_2021 - min_2021) for h in hourly_2021]
                        norm_2024 = [(h - min_2024) / (max_2024 - min_2024) for h in hourly_2024]

                        # 计算相关系数
                        mean_2021 = statistics.mean(norm_2021)
                        mean_2024 = statistics.mean(norm_2024)

                        if mean_2021 > 0 and mean_2024 > 0:
                            # 简化的皮尔逊相关系数计算
                            numerator = sum((n1 - mean_2021) * (n2 - mean_2024) for n1, n2 in zip(norm_2021, norm_2024))
                            var_2021 = sum((n1 - mean_2021) ** 2 for n1 in norm_2021)
                            var_2024 = sum((n2 - mean_2024) ** 2 for n2 in norm_2024)

                            if var_2021 > 0 and var_2024 > 0:
                                correlation = numerator / (var_2021 ** 0.5 * var_2024 ** 0.5)

                                # 如果曲线形状高度相似（相关系数>0.85），视觉上可能看起来稳定
                                if correlation > 0.85:
                                    return "stable"
                except:
                    pass  # 如果相关系数计算失败，继续返回stable

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

        判断逻辑（基于2021→2024椭圆面积变化）：
        - 静态：椭圆面积变化在 ±12% 以内（基本不变）
        - 扩散：椭圆面积增长超过 12%（向外扩展）
        - 聚集：椭圆面积减少超过 12%（向内收缩）

        Returns: "static", "aggregation", "diffusion"
        """
        # 尝试使用椭圆数据判断
        area_2021 = self.get_ellipse_area(grid_id, 2021)
        area_2024 = self.get_ellipse_area(grid_id, 2024)

        if area_2021 and area_2024:
            if area_2021 == 0:
                return "diffusion" if area_2024 > 0 else "static"

            change_ratio = (area_2024 - area_2021) / area_2021

            # 调整后的阈值（12%变化认为显著）
            if change_ratio > 0.12:  # 面积增长超过12% -> 扩散
                return "diffusion"
            elif change_ratio < -0.12:  # 面积减少超过12% -> 聚集
                return "aggregation"
            else:
                return "static"

        # 如果没有椭圆数据，返回默认值
        # TODO: 可以基于flows_data的流线分布范围进行辅助判断
        return "static"

    def predict_label(self, grid_id: int) -> Tuple[int, str, dict]:
        """
        预测格网类型

        Returns: (label_number, label_name, metadata)
        metadata包含：
        - trend: 趋势判断
        - spatial: 空间模式判断
        - is_edge_case: 是否是边缘案例
        - confidence: 置信度信息
        """
        try:
            # 获取数据
            hourly_data = self.get_grid_hourly(grid_id)

            # 分析趋势
            trend = self.analyze_trend(hourly_data)  # stable/growth/decay

            # 分析空间模式（基于椭圆数据）
            spatial = self.analyze_spatial_pattern(grid_id)  # static/aggregation/diffusion

            # 映射到标签
            trend_map = {"stable": 0, "growth": 3, "decay": 6}
            spatial_map = {"static": 1, "aggregation": 2, "diffusion": 3}

            label = trend_map[trend] + spatial_map[spatial]

            # 判断是否是边缘案例
            edge_info = self._check_edge_case(grid_id, hourly_data, trend, spatial)

            return label, LABEL_MAP.get(label, f"未知类型{label}"), edge_info

        except Exception as e:
            print(f"  [分析错误] {e}")
            return 0, "其他", {"is_edge_case": False, "error": str(e)}

    def _check_edge_case(self, grid_id: int, hourly_data: dict, trend: str, spatial: str) -> dict:
        """检查是否是边缘案例"""
        import math

        # 获取流量和椭圆数据
        def get_daily_total(year_data):
            if not year_data or "total" not in year_data:
                return 0
            weeks = year_data["total"][:1]
            if not weeks:
                return 0
            hourly_avg = []
            for h in range(24):
                total = sum((week[h] if h < len(week) else 0) for week in weeks)
                hourly_avg.append(total / 1)
            return sum(hourly_avg)

        total_2021 = get_daily_total(hourly_data.get("2021", {}))
        total_2024 = get_daily_total(hourly_data.get("2024", {}))
        abs_change = total_2024 - total_2021

        area_2021 = self.get_ellipse_area(grid_id, 2021)
        area_2024 = self.get_ellipse_area(grid_id, 2024)

        # 确定阈值
        if total_2021 > 2000:
            threshold = 1000
        elif total_2021 > 1000:
            threshold = 900
        elif total_2021 > 500:
            threshold = 700
        elif total_2021 > 200:
            threshold = 400
        elif total_2021 > 100:
            threshold = 250
        else:
            threshold = 200

        # 判断是否接近阈值（±20%以内）
        is_edge_trend = abs(abs(abs_change) - threshold) / threshold < 0.2

        # 判断椭圆变化是否接近阈值（±3%以内）
        is_edge_spatial = False
        if area_2021 and area_2021 > 0:
            area_change_ratio = (area_2024 - area_2021) / area_2021
            # 如果在9%-15%之间（阈值12%±3%），认为是边缘
            is_edge_spatial = 0.09 < abs(area_change_ratio) < 0.15

        is_edge = is_edge_trend or is_edge_spatial

        return {
            "is_edge_case": is_edge,
            "edge_reason": {
                "trend_near_threshold": is_edge_trend,
                "spatial_near_threshold": is_edge_spatial,
                "flow_change": abs_change,
                "flow_threshold": threshold,
                "area_change_ratio": ((area_2024 - area_2021) / area_2021 * 100) if area_2021 and area_2021 > 0 else None,
                "area_threshold": 12.0
            },
            "confidence": {
                "trend": trend,
                "spatial": spatial
            }
        }

    def run_batch(self, max_count: int = 100):
        """批量标注"""
        print("=== 自动标注开始 ===")
        print(f"提示：请确保浏览器中只勾选了2021和2024年（取消勾选2018年）\n")

        # 获取队列
        queue_info = self.get_queue()
        queue = queue_info.get("queue", [])
        index = queue_info.get("index", 0)

        if not queue:
            print("队列为空，请先在网页上点击'开始打标签'")
            return

        remaining = queue[index:]
        count = min(len(remaining), max_count)

        print(f"队列中有 {len(remaining)} 个格网待标注")
        print(f"准备标注前 {count} 个\n")

        # 统计信息
        stats = {
            "total": 0,
            "success": 0,
            "edge_cases": [],
            "errors": []
        }

        # 边缘案例CSV文件
        edge_csv_path = LABELS_DIR / "edge_cases.csv"
        import csv
        edge_csv_file = open(edge_csv_path, 'w', newline='', encoding='utf-8')
        edge_csv_writer = csv.writer(edge_csv_file)
        edge_csv_writer.writerow([
            'grid_id', 'label', 'label_name',
            'flow_2021', 'flow_2024', 'flow_change', 'flow_threshold',
            'area_change_ratio', 'area_threshold',
            'edge_trend', 'edge_spatial', 'reason'
        ])

        for i in range(count):
            grid_id = remaining[i]
            stats["total"] += 1
            print(f"[{i+1}/{count}] 格网 ID: {grid_id}", end=" ")

            try:
                # 预测标签（带边缘案例检测）
                label, label_name, edge_info = self.predict_label(grid_id)

                if label == 0:
                    print(f"✗ 无法判断，跳过")
                    self.advance_queue()
                    continue

                # 提交标签
                self.submit_label(grid_id, label)
                stats["success"] += 1

                # 如果是边缘案例，记录下来
                if edge_info.get("is_edge_case", False):
                    stats["edge_cases"].append(grid_id)
                    reason = edge_info.get("edge_reason", {})

                    # 获取流量数据
                    hourly_data = self.get_grid_hourly(grid_id)
                    def get_daily_total(year_data):
                        if not year_data or "total" not in year_data:
                            return 0
                        weeks = year_data["total"][:1]
                        if not weeks:
                            return 0
                        hourly_avg = []
                        for h in range(24):
                            total = sum((week[h] if h < len(week) else 0) for week in weeks)
                            hourly_avg.append(total / 1)
                        return sum(hourly_avg)

                    flow_2021 = get_daily_total(hourly_data.get("2021", {}))
                    flow_2024 = get_daily_total(hourly_data.get("2024", {}))

                    # 写入边缘案例CSV
                    area_ratio = reason.get('area_change_ratio') or 0
                    edge_csv_writer.writerow([
                        grid_id,
                        label,
                        label_name,
                        f"{flow_2021:.1f}",
                        f"{flow_2024:.1f}",
                        f"{reason.get('flow_change', 0):.1f}",
                        f"{reason.get('flow_threshold', 0):.0f}",
                        f"{area_ratio:.1f}%",
                        f"{reason.get('area_threshold', 0):.1f}%",
                        reason.get('trend_near_threshold', False),
                        reason.get('spatial_near_threshold', False),
                        f"Trend:{'Y' if reason.get('trend_near_threshold') else 'N'} Spatial:{'Y' if reason.get('spatial_near_threshold') else 'N'}"
                    ])

                    print(f"✓ {label} [边缘案例]")
                else:
                    print(f"✓ {label}")

                # 前进到下一个
                self.advance_queue()

            except Exception as e:
                print(f"✗ 失败: {e}")
                stats["errors"].append({"grid_id": grid_id, "error": str(e)})
                break

        edge_csv_file.close()

        print("\n" + "=" * 60)
        print(f"批次完成！")
        print(f"成功标注: {stats['success']}/{stats['total']}")
        print(f"边缘案例: {len(stats['edge_cases'])} 个 ({len(stats['edge_cases'])/stats['success']*100:.1f}%)")
        print(f"错误: {len(stats['errors'])} 个")

        if stats['edge_cases']:
            print(f"\n⚠ 边缘案例已保存到: {edge_csv_path}")
            print(f"  请人工检查这 {len(stats['edge_cases'])} 个格网的标签")

        queue_info = self.get_queue()
        print(f"\n当前进度: {queue_info.get('index', 0)}/{len(queue_info.get('queue', []))}")

        return stats


def main():
    import sys

    count = 100
    if len(sys.argv) > 1:
        count = int(sys.argv[1])

    labeler = AutoLabeler()
    labeler.run_batch(max_count=count)


if __name__ == "__main__":
    main()
