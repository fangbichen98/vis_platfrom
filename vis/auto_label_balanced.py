#!/usr/bin/env python3
"""
平衡标注脚本 - 确保每个类别至少有指定数量的样本

使用方法：
    python auto_label_balanced.py --min-per-class 800
"""

import sys
import csv
import time
import argparse
from pathlib import Path
from collections import Counter
from auto_label import AutoLabeler, LABEL_MAP

# 配置
SCRIPT_DIR = Path(__file__).parent
LABELS_DIR = SCRIPT_DIR / "labels"
LABELS_CSV = LABELS_DIR / "labels.csv"


def get_label_counts():
    """获取当前各类别的标注数量"""
    if not LABELS_CSV.exists():
        return Counter()

    counts = Counter()
    with LABELS_CSV.open('r', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                label = int(row['label'])
                if 1 <= label <= 9:
                    counts[label] += 1
            except (ValueError, KeyError):
                continue

    return counts


def print_progress(counts, min_count):
    """打印当前进度"""
    print("\n" + "=" * 70)
    print("当前标注进度：")
    print("-" * 70)

    total = sum(counts.values())
    completed = 0

    for label in range(1, 10):
        count = counts.get(label, 0)
        name = LABEL_MAP.get(label, f"类型{label}")
        progress = min(100, (count / min_count) * 100)
        status = "✓" if count >= min_count else " "

        bar_length = 30
        filled = int(bar_length * progress / 100)
        bar = "█" * filled + "░" * (bar_length - filled)

        print(f"[{status}] {label}. {name:12s} [{bar}] {count:4d}/{min_count} ({progress:5.1f}%)")

        if count >= min_count:
            completed += 1

    print("-" * 70)
    print(f"总计: {total} 个样本")
    print(f"完成类别: {completed}/9")
    print("=" * 70 + "\n")

    return completed == 9


def main():
    parser = argparse.ArgumentParser(description='平衡标注脚本')
    parser.add_argument('--min-per-class', type=int, default=800,
                        help='每个类别最少样本数 (默认: 800)')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='每批次标注数量 (默认: 1000)')
    parser.add_argument('--max-iterations', type=int, default=50,
                        help='最大迭代次数 (默认: 50)')

    args = parser.parse_args()

    min_count = args.min_per_class
    batch_size = args.batch_size
    max_iterations = args.max_iterations

    print(f"\n{'='*70}")
    print(f"平衡标注任务启动")
    print(f"{'='*70}")
    print(f"目标: 每个类别至少 {min_count} 个样本")
    print(f"批次大小: {batch_size}")
    print(f"最大迭代次数: {max_iterations}")
    print(f"{'='*70}\n")

    # 初始化标注器
    labeler = AutoLabeler()

    iteration = 0
    while iteration < max_iterations:
        iteration += 1

        # 检查当前进度
        counts = get_label_counts()
        print(f"\n第 {iteration} 轮检查:")

        if print_progress(counts, min_count):
            print("\n🎉 所有类别都已达到目标数量！")
            print(f"总计标注: {sum(counts.values())} 个样本")
            break

        # 找出未达标的类别
        incomplete = [label for label in range(1, 10) if counts.get(label, 0) < min_count]
        print(f"\n未达标类别: {incomplete}")
        print(f"继续标注 {batch_size} 个样本...\n")

        # 执行一批标注
        try:
            stats = labeler.run_batch(max_count=batch_size)

            if stats['success'] == 0:
                print("\n⚠️  没有新的样本被标注，可能队列已空")
                print("提示：请在网页上点击'开始打标签'重新生成队列")
                break

            print(f"\n本批次完成: 成功 {stats['success']}/{stats['total']}")

        except KeyboardInterrupt:
            print("\n\n用户中断，正在保存进度...")
            break
        except Exception as e:
            print(f"\n❌ 错误: {e}")
            print("等待5秒后继续...")
            time.sleep(5)

    # 最终统计
    print("\n" + "="*70)
    print("最终统计")
    print("="*70)

    final_counts = get_label_counts()
    print_progress(final_counts, min_count)

    # 检查是否完成
    incomplete = [label for label in range(1, 10) if final_counts.get(label, 0) < min_count]

    if not incomplete:
        print("✅ 任务完成！所有类别都已达到目标数量。")
    else:
        print(f"⚠️  以下类别仍未达标: {incomplete}")
        for label in incomplete:
            count = final_counts.get(label, 0)
            needed = min_count - count
            name = LABEL_MAP.get(label, f"类型{label}")
            print(f"   - {label}. {name}: 还需 {needed} 个样本")

    print("\n标注数据已保存到: labels/labels.csv")
    print("边缘案例已保存到: labels/edge_cases.csv")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()
