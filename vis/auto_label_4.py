#!/usr/bin/env python3
"""
自动格网标注脚本（4种分类版本）

分类类型：
1. 增长聚集型 - 流量增加，空间范围收缩
2. 增长扩散型 - 流量增加，空间范围扩展
3. 衰减聚集型 - 流量减少，空间范围收缩
4. 衰减扩散型 - 流量减少，空间范围扩展

判断依据：
- 流量强度对比（不同年份之间）→ 判断增长/衰减
- 空间方向的聚集程度对比（椭圆面积变化）→ 判断聚集/扩散
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
SCREENSHOTS_DIR = LABELS_DIR / "screenshots"  # 按序号保存截图的目录

# 确保目录存在
SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

# 标签映射（4种分类）
LABEL_MAP = {
    1: "增长聚集型",  # 流量增长 + 空间收缩
    2: "增长扩散型",  # 流量增长 + 空间扩展
    3: "衰减聚集型",  # 流量衰减 + 空间收缩
    4: "衰减扩散型",  # 流量衰减 + 空间扩展
}


class AutoLabeler4:
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
            raise ValueError(f"不支持的HTTP方法: {method}")

        if resp.status_code == 200:
            return resp.json()
        else:
            raise Exception(f"API请求失败: {resp.status_code} - {resp.text}")

    def get_current_grid(self) -> Optional[int]:
        """获取当前格网ID"""
        queue_info = self.fetch_json("label_queue")
        queue = queue_info.get("queue", [])
        index = queue_info.get("index", 0)

        if index < len(queue):
            return queue[index]
        return None

    def get_queue(self) -> dict:
        """获取队列信息"""
        return self.fetch_json("label_queue")

    def get_grid_hourly(self, grid_id: int) -> dict:
        """获取格网的24小时数据"""
        return self.fetch_json(f"grid_hourly/{grid_id}")

    def get_grid_flows(self, grid_id: int) -> dict:
        """获取格网的流线数据"""
        return self.fetch_json(f"grid_flows/{grid_id}")

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
        分析流量趋势：增长/衰减（强制二分类）

        判断逻辑：
        - 计算两年间的日均流量差值（绝对值）
        - 根据流量基数设置不同的阈值
        - 超过阈值且为正 → 增长
        - 超过阈值且为负 → 衰减
        - 未超过阈值时，基于变化方向进行判断（总是倾向于做出判断）

        Returns: "growth" 或 "decay"
        """
        if not hourly_data or "2021" not in hourly_data or "2024" not in hourly_data:
            return "growth"  # 默认返回增长

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
            return sum(hourly_avg)

        total_2021 = get_daily_total(hourly_data["2021"])
        total_2024 = get_daily_total(hourly_data["2024"])

        if total_2021 == 0:
            return "growth"  # 从无到有，判为增长

        abs_change = total_2024 - total_2021

        # 根据流量基数设置阈值
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

        # 强制二分类：基于变化方向
        if abs_change >= 0:
            return "growth"
        else:
            return "decay"

    def get_ellipse_area(self, grid_id: int, year: int) -> Optional[float]:
        """获取格网某年的椭圆面积"""
        if not self.ellipses_data:
            return None

        try:
            years_data = self.ellipses_data.get("years", {})
            year_data = years_data.get(str(year), [])

            for item in year_data:
                if item.get("grid_id") == grid_id:
                    ellipse = item.get("ellipse", {})
                    axes = ellipse.get("axes", {})
                    a = axes.get("a", 0)  # 长半轴
                    b = axes.get("b", 0)  # 短半轴
                    # 椭圆面积 = π * a * b
                    return math.pi * a * b
        except Exception as e:
            pass

        return None

    def analyze_spatial_pattern(self, grid_id: int) -> str:
        """
        分析空间模式：聚集/扩散（强制二分类）

        判断逻辑（基于2021→2024椭圆面积变化）：
        - 面积增长 → 扩散
        - 面积减少 → 聚集
        - 面积基本不变时，基于微小变化方向判断

        Returns: "aggregation" 或 "diffusion"
        """
        # 使用椭圆数据判断
        area_2021 = self.get_ellipse_area(grid_id, 2021)
        area_2024 = self.get_ellipse_area(grid_id, 2024)

        if area_2021 and area_2024:
            if area_2021 == 0:
                return "diffusion" if area_2024 > 0 else "aggregation"

            change_ratio = (area_2024 - area_2021) / area_2021

            # 阈值：12%的变化认为显著
            if change_ratio >= 0:
                return "diffusion"  # 面积增加或基本不变 → 扩散
            else:
                return "aggregation"  # 面积减少 → 聚集

        # 如果没有椭圆数据，基于流量变化辅助推断
        # 流量增长倾向于扩散，流量衰减倾向于聚集（这是一种简化假设）
        return "diffusion"  # 默认返回扩散

    def predict_label(self, grid_id: int) -> Tuple[int, str, dict]:
        """
        预测格网类型（4分类）

        Returns: (label_number, label_name, metadata)
        metadata包含：
        - trend: 趋势判断 (growth/decay)
        - spatial: 空间模式判断 (aggregation/diffusion)
        - flow_change: 流量变化值
        - area_change_ratio: 椭圆面积变化比例
        """
        try:
            # 获取数据
            hourly_data = self.get_grid_hourly(grid_id)

            # 分析趋势
            trend = self.analyze_trend(hourly_data)  # growth/decay

            # 分析空间模式
            spatial = self.analyze_spatial_pattern(grid_id)  # aggregation/diffusion

            # 映射到标签（4分类）
            # 1: 增长聚集, 2: 增长扩散, 3: 衰减聚集, 4: 衰减扩散
            if trend == "growth":
                if spatial == "aggregation":
                    label = 1  # 增长聚集型
                else:
                    label = 2  # 增长扩散型
            else:  # decay
                if spatial == "aggregation":
                    label = 3  # 衰减聚集型
                else:
                    label = 4  # 衰减扩散型

            # 获取元数据
            metadata = self._get_metadata(grid_id, hourly_data, trend, spatial)

            return label, LABEL_MAP.get(label, f"未知类型{label}"), metadata

        except Exception as e:
            print(f"  [分析错误] {e}")
            return 0, "其他", {"error": str(e)}

    def _get_metadata(self, grid_id: int, hourly_data: dict, trend: str, spatial: str) -> dict:
        """获取元数据信息"""
        # 获取流量数据
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
        flow_change = total_2024 - total_2021
        flow_change_ratio = (flow_change / total_2021 * 100) if total_2021 > 0 else 0

        # 获取椭圆数据
        area_2021 = self.get_ellipse_area(grid_id, 2021)
        area_2024 = self.get_ellipse_area(grid_id, 2024)
        area_change_ratio = None
        if area_2021 and area_2021 > 0:
            area_change_ratio = (area_2024 - area_2021) / area_2021 * 100

        return {
            "trend": trend,
            "spatial": spatial,
            "flow_2021": total_2021,
            "flow_2024": total_2024,
            "flow_change": flow_change,
            "flow_change_ratio": flow_change_ratio,
            "area_2021": area_2021,
            "area_2024": area_2024,
            "area_change_ratio": area_change_ratio
        }

    def run_batch(self, max_count: int = 100):
        """批量标注"""
        print("=== 自动标注开始（4分类版本）===")
        print(f"分类类型：")
        print(f"  1 - 增长聚集型（流量增加 + 空间收缩）")
        print(f"  2 - 增长扩散型（流量增加 + 空间扩展）")
        print(f"  3 - 衰减聚集型（流量减少 + 空间收缩）")
        print(f"  4 - 衰减扩散型（流量减少 + 空间扩展）")
        print(f"\n提示：请确保浏览器中只勾选了2021和2024年（取消勾选2018年）\n")

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
            "label_counts": {1: 0, 2: 0, 3: 0, 4: 0},
            "errors": []
        }

        for i in range(count):
            grid_id = remaining[i]
            stats["total"] += 1
            print(f"[{i+1}/{count}] 格网 ID: {grid_id}", end=" ")

            try:
                # 预测标签
                label, label_name, metadata = self.predict_label(grid_id)

                if label == 0:
                    print(f"✗ 无法判断，跳过")
                    self.advance_queue()
                    continue

                # 提交标签
                self.submit_label(grid_id, label)
                stats["success"] += 1
                stats["label_counts"][label] += 1

                # 显示详细信息
                flow_change = metadata.get("flow_change", 0)
                flow_ratio = metadata.get("flow_change_ratio", 0)
                area_ratio = metadata.get("area_change_ratio", 0)

                print(f"✓ {label} ({label_name})")
                print(f"    流量: {metadata.get('flow_2021', 0):.0f} → {metadata.get('flow_2024', 0):.0f} "
                      f"({flow_change:+.0f}, {flow_ratio:+.1f}%)")

                if area_ratio is not None:
                    print(f"    椭圆面积变化: {area_ratio:+.1f}%")

                # 前进到下一个
                self.advance_queue()

            except Exception as e:
                print(f"✗ 失败: {e}")
                stats["errors"].append({"grid_id": grid_id, "error": str(e)})
                break

        print("\n" + "=" * 60)
        print(f"批次完成！")
        print(f"成功标注: {stats['success']}/{stats['total']}")
        print(f"\n标签分布：")
        for label_id, count_val in stats["label_counts"].items():
            label_name = LABEL_MAP.get(label_id, f"类型{label_id}")
            percentage = (count_val / stats['success'] * 100) if stats['success'] > 0 else 0
            print(f"  {label_id} - {label_name}: {count_val} 个 ({percentage:.1f}%)")

        if stats["errors"]:
            print(f"\n错误: {len(stats['errors'])} 个")
            for err in stats["errors"][:5]:  # 只显示前5个错误
                print(f"  - 格网 {err['grid_id']}: {err['error']}")

        queue_info = self.get_queue()
        print(f"\n当前进度: {queue_info.get('index', 0)}/{len(queue_info.get('queue', []))}")

        return stats


def main():
    import sys

    count = 100
    if len(sys.argv) > 1:
        count = int(sys.argv[1])

    labeler = AutoLabeler4()
    labeler.run_batch(max_count=count)


if __name__ == "__main__":
    main()
