#!/usr/bin/env python3
"""
实时监控自动标注进度
"""

import requests
import time

def monitor_progress(refresh_interval=5):
    """监控标注进度"""
    print("=== 实时监控标注进度 ===")
    print(f"刷新间隔: {refresh_interval}秒")
    print("按 Ctrl+C 停止\n")

    last_index = 0
    start_time = time.time()

    try:
        while True:
            resp = requests.get('http://127.0.0.1:8055/api/label_queue')
            data = resp.json()

            queue = data.get('queue', [])
            index = data.get('index', 0)
            total = len(queue)

            if total == 0:
                print("队列为空")
                break

            pct = (index / total * 100) if total > 0 else 0
            delta = index - last_index
            elapsed = time.time() - start_time

            # 计算速度
            speed = delta / refresh_interval if refresh_interval > 0 else 0

            # 计算剩余时间
            remaining = total - index
            eta_seconds = remaining / speed if speed > 0 else 0
            eta_minutes = eta_seconds / 60

            print(f"[{elapsed:5.0f}s] 进度: {index}/{total} ({pct:5.1f}%) | "
                  f"速度: {speed:4.1f}个/秒 | 剩余: {eta_minutes:5.1f}分钟")

            last_index = index
            time.sleep(refresh_interval)

    except KeyboardInterrupt:
        print("\n\n监控已停止")
    except Exception as e:
        print(f"\n错误: {e}")

if __name__ == "__main__":
    import sys
    interval = 5
    if len(sys.argv) > 1:
        interval = int(sys.argv[1])
    monitor_progress(interval)
