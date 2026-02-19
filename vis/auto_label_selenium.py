#!/usr/bin/env python3
"""
基于Selenium的自动格网标注脚本（带截图保存）

功能：
1. 自动控制浏览器进行标注
2. 自动截图保存（序号+格网ID+类型）
3. 只显示2021和2024年的数据
"""

import time
import json
import math
from pathlib import Path
from typing import Optional, Tuple
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# 配置
BASE_URL = "http://127.0.0.1:8055"
SCREENSHOTS_DIR = Path(__file__).parent / "labels" / "screenshots"

# 标签映射
LABEL_MAP = {
    1: "稳定静态型", 2: "稳定聚集型", 3: "稳定扩散型",
    4: "增长静态型", 5: "增长聚集型", 6: "增长扩散型",
    7: "衰减静态型", 8: "衰减聚集型", 9: "衰减扩散型"
}


class SeleniumAutoLabeler:
    def __init__(self, headless: bool = False):
        # 设置Chrome选项
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')

        # 初始化浏览器
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)

        # 加载椭圆数据
        self.ellipses_data = self.load_ellipses()

        # 确保截图目录存在
        SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    def load_ellipses(self):
        """加载椭圆数据"""
        ellipses_path = Path(__file__).parent / "appdata" / "ellipses.json"
        if ellipses_path.exists():
            try:
                with open(ellipses_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"[警告] 加载椭圆数据失败: {e}")
        return None

    def open_page(self):
        """打开标注页面"""
        print(f"正在打开页面: {BASE_URL}")
        self.driver.get(BASE_URL)

        # 等待页面加载
        time.sleep(2)

        # 取消勾选2018年，只显示2021和2024
        print("设置年份：只显示2021和2024年")
        try:
            chk_2018 = self.driver.find_element(By.ID, "yearChk2018")
            if chk_2018.is_selected():
                chk_2018.click()
                time.sleep(0.5)

            # 确保2021和2024已勾选
            chk_2021 = self.driver.find_element(By.ID, "yearChk2021")
            chk_2024 = self.driver.find_element(By.ID, "yearChk2024")
            if not chk_2021.is_selected():
                chk_2021.click()
            if not chk_2024.is_selected():
                chk_2024.click()
            time.sleep(1)
            print("✓ 年份设置完成")
        except Exception as e:
            print(f"✗ 设置年份失败: {e}")

    def get_current_grid_id(self) -> Optional[int]:
        """获取当前格网ID"""
        try:
            info = self.driver.find_element(By.ID, "currentInfo").text
            # 格式: "ID 270910 (113.2891,22.1697) 珠海市 斗门区"
            if "ID" in info:
                parts = info.split()
                return int(parts[1])
        except Exception as e:
            print(f"  获取格网ID失败: {e}")
        return None

    def fetch_hourly_data(self, grid_id: int) -> dict:
        """通过JavaScript获取24小时数据"""
        script = f"""
        return fetch('/api/hourly?grid_id={grid_id}')
            .then(r => r.json())
            .catch(e => null);
        """
        result = self.driver.execute_script(script)
        return result or {}

    def analyze_trend(self, hourly_data: dict) -> str:
        """分析流量趋势"""
        if not hourly_data or "2021" not in hourly_data or "2024" not in hourly_data:
            return "stable"

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

        total_2021 = get_daily_total(hourly_data["2021"])
        total_2024 = get_daily_total(hourly_data["2024"])

        if total_2021 == 0:
            return "growth"

        change_ratio = (total_2024 - total_2021) / total_2021

        if change_ratio > 0.15:
            return "growth"
        elif change_ratio < -0.15:
            return "decay"
        else:
            return "stable"

    def get_ellipse_area(self, grid_id: int, year: int) -> Optional[float]:
        """获取椭圆面积"""
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
                    return math.pi * a * b
        except Exception:
            pass
        return None

    def analyze_spatial_pattern(self, grid_id: int) -> str:
        """分析空间模式"""
        area_2021 = self.get_ellipse_area(grid_id, 2021)
        area_2024 = self.get_ellipse_area(grid_id, 2024)

        if area_2021 and area_2024:
            if area_2021 == 0:
                return "diffusion" if area_2024 > 0 else "static"

            change_ratio = (area_2024 - area_2021) / area_2021

            if change_ratio > 0.2:
                return "diffusion"
            elif change_ratio < -0.2:
                return "aggregation"
            else:
                return "static"

        return "static"

    def predict_label(self, grid_id: int) -> Tuple[int, str]:
        """预测标签"""
        try:
            hourly_data = self.fetch_hourly_data(grid_id)
            trend = self.analyze_trend(hourly_data)
            spatial = self.analyze_spatial_pattern(grid_id)

            trend_map = {"stable": 0, "growth": 3, "decay": 6}
            spatial_map = {"static": 1, "aggregation": 2, "diffusion": 3}

            label = trend_map[trend] + spatial_map[spatial]
            return label, LABEL_MAP.get(label, f"未知类型{label}")
        except Exception as e:
            print(f"  [分析错误] {e}")
            return 0, "其他"

    def submit_label(self, label: int):
        """通过按键提交标签"""
        # 按下数字键
        key = str(label)
        from selenium.webdriver.common.keys import Keys
        # 使用JavaScript发送按键事件
        script = f"""
        const event = new KeyboardEvent('keydown', {{
            key: '{key}',
            code: 'Digit{key}',
            keyCode: {key.charCodeAt(0)},
            which: {key.charCodeAt(0)},
            bubbles: true
        }});
        document.dispatchEvent(event);
        """
        self.driver.execute_script(script)
        time.sleep(0.3)  # 等待提交和截图

    def take_screenshot(self, index: int, grid_id: int, label: int, label_name: str):
        """保存截图"""
        filename = f"{index:03d}_{grid_id}_{label}{label_name}.jpg"
        filepath = SCREENSHOTS_DIR / filename

        # 使用html2canvas截取整个页面
        script = """
        return html2canvas(document.querySelector('.app'), {
            backgroundColor: '#ffffff',
            scale: 1
        }).then(canvas => {
            return canvas.toDataURL('image/jpeg', 0.92);
        });
        """
        try:
            data_url = self.driver.execute_script(script)
            if data_url:
                # 解析base64数据
                header, encoded = data_url.split(",", 1)
                import base64
                data = base64.b64decode(encoded)

                with open(filepath, 'wb') as f:
                    f.write(data)
                print(f"  ✓ 截图已保存: {filename}")
        except Exception as e:
            print(f"  ✗ 截图失败: {e}")

    def run_batch(self, count: int = 100):
        """批量标注"""
        print("=== 自动标注开始 ===\n")

        for i in range(count):
            grid_id = self.get_current_grid_id()

            if not grid_id:
                print(f"[{i+1}/{count}] 无法获取格网ID，可能已完成")
                break

            print(f"[{i+1}/{count}] 格网 ID: {grid_id}")

            try:
                # 预测标签
                label, label_name = self.predict_label(grid_id)

                if label == 0:
                    print(f"  → 无法判断，跳过")
                    self.press_key("Enter")  # 按Enter跳过
                    continue

                print(f"  → 判断: {label} ({label_name})")

                # 提交标签（会自动触发截图上传）
                self.submit_label(label)

                # 自己也保存一份本地截图（带序号）
                self.take_screenshot(i+1, grid_id, label, label_name)

                print(f"  ✓ 已提交")

                # 等待页面跳转
                time.sleep(0.5)

            except Exception as e:
                print(f"  ✗ 失败: {e}")
                break

        print("\n=== 批次完成 ===")

    def press_key(self, key: str):
        """模拟按键"""
        from selenium.webdriver.common.keys import Keys
        script = f"""
        const event = new KeyboardEvent('keydown', {{
            key: '{key}',
            code: 'Enter' if '{key}' === 'Enter' else 'Digit{key}',
            keyCode: {'13' if key == 'Enter' else key.charCodeAt(0)},
            which: {'13' if key == 'Enter' else key.charCodeAt(0)},
            bubbles: true
        }});
        document.dispatchEvent(event);
        """
        self.driver.execute_script(script)
        time.sleep(0.3)

    def close(self):
        """关闭浏览器"""
        self.driver.quit()


def main():
    import sys

    count = 100
    if len(sys.argv) > 1:
        count = int(sys.argv[1])

    labeler = SeleniumAutoLabeler(headless=False)  # 设置为True可以在后台运行

    try:
        labeler.open_page()
        print("\n浏览器已打开，准备开始标注...")
        print("提示：你可以在浏览器中看到实时进度\n")

        labeler.run_batch(count)

    finally:
        # 保持浏览器打开，方便查看
        input("\n按Enter键关闭浏览器...")
        labeler.close()


if __name__ == "__main__":
    main()
