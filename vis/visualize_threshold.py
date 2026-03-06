#!/usr/bin/env python3
"""
可视化阈值分析结果
"""
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import numpy as np
import pandas as pd

# 添加中文字体
font_path = '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc'
font_prop = fm.FontProperties(fname=font_path)
plt.rcParams['font.family'] = font_prop.get_name()
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 数据
thresholds = [10, 20, 30, 40, 50, 75, 100]
isolated_2021 = [52.4, 57.1, 60.1, 62.4, 64.2, 68.2, 71.2]
isolated_2024 = [51.9, 60.4, 66.4, 71.1, 74.6, 81.8, 87.1]
edges_2021 = [39530, 15028, 8137, 5194, 3692, 2224, 1624]
edges_2024 = [64120, 30594, 18299, 12086, 8684, 4402, 2508]
avg_deg_2021 = [18.08, 6.87, 3.72, 2.38, 1.69, 1.02, 0.74]
avg_deg_2024 = [29.33, 14.00, 8.37, 5.53, 3.97, 2.01, 1.15]

# 创建图表
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('流量阈值对图结构的影响分析（采样数据：4372个格网）',
             fontsize=18, fontweight='bold', y=0.995)

# 1. 孤立节点比例（最重要）
ax1 = axes[0, 0]
ax1.plot(thresholds, isolated_2021, 'o-', label='2021年', linewidth=2.5, markersize=8, color='#1f77b4')
ax1.plot(thresholds, isolated_2024, 's-', label='2024年', linewidth=2.5, markersize=8, color='#ff7f0e')
ax1.axvline(x=50, color='red', linestyle='--', linewidth=2, alpha=0.7, label='当前阈值=50')
ax1.axhline(y=50, color='gray', linestyle=':', linewidth=1.5, alpha=0.5, label='50%警戒线')
ax1.fill_between(thresholds, isolated_2021, isolated_2024, alpha=0.1, color='red')
ax1.set_xlabel('流量阈值（人次）', fontsize=13, fontweight='bold')
ax1.set_ylabel('孤立节点比例 (%)', fontsize=13, fontweight='bold')
ax1.set_title('⚠️ 孤立节点比例 vs 阈值\n（越高越差，>50%严重）', fontsize=14, fontweight='bold')
ax1.legend(fontsize=11, loc='upper left')
ax1.grid(True, alpha=0.3, linestyle='--')
ax1.set_xlim(5, 105)
ax1.set_ylim(45, 90)

# 标注关键点
ax1.annotate(f'阈值50:\n2021: 64.2%孤立\n2024: 74.6%孤立',
             xy=(50, isolated_2024[4]), xytext=(65, 85),
             fontsize=10, fontweight='bold',
             bbox=dict(boxstyle='round,pad=0.5', facecolor='red', alpha=0.3),
             arrowprops=dict(arrowstyle='->', color='red', lw=2))

# 2. 边数
ax2 = axes[0, 1]
ax2.plot(thresholds, edges_2021, 'o-', label='2021年', linewidth=2.5, markersize=8, color='#1f77b4')
ax2.plot(thresholds, edges_2024, 's-', label='2024年', linewidth=2.5, markersize=8, color='#ff7f0e')
ax2.axvline(x=50, color='red', linestyle='--', linewidth=2, alpha=0.7, label='当前阈值=50')
ax2.set_xlabel('流量阈值（人次）', fontsize=13, fontweight='bold')
ax2.set_ylabel('边数', fontsize=13, fontweight='bold')
ax2.set_title('📊 边数 vs 阈值\n（越高越密集）', fontsize=14, fontweight='bold')
ax2.legend(fontsize=11)
ax2.grid(True, alpha=0.3, linestyle='--')
ax2.set_xlim(5, 105)
ax2.set_yscale('log')

# 标注边数增长
ax2.annotate(f'阈值10 → 50:\n2021: -90%\n2024: -86%',
             xy=(10, edges_2021[0]), xytext=(15, 10000),
             fontsize=9,
             bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.4),
             arrowprops=dict(arrowstyle='->', color='orange', lw=1.5))

# 3. 平均度数
ax3 = axes[1, 0]
ax3.plot(thresholds, avg_deg_2021, 'o-', label='2021年', linewidth=2.5, markersize=8, color='#1f77b4')
ax3.plot(thresholds, avg_deg_2024, 's-', label='2024年', linewidth=2.5, markersize=8, color='#ff7f0e')
ax3.axvline(x=50, color='red', linestyle='--', linewidth=2, alpha=0.7, label='当前阈值=50')
ax3.axhline(y=2, color='green', linestyle=':', linewidth=2, alpha=0.5, label='健康度数=2')
ax3.set_xlabel('流量阈值（人次）', fontsize=13, fontweight='bold')
ax3.set_ylabel('平均度数', fontsize=13, fontweight='bold')
ax3.set_title('🔗 平均度数 vs 阈值\n（越高连通性越好）', fontsize=14, fontweight='bold')
ax3.legend(fontsize=11, loc='upper right')
ax3.grid(True, alpha=0.3, linestyle='--')
ax3.set_xlim(5, 105)

# 标注
ax3.annotate(f'2024年\n阈值50: 仅3.97\n阈值10: 29.33',
             xy=(50, avg_deg_2024[4]), xytext=(70, 20),
             fontsize=9, fontweight='bold',
             bbox=dict(boxstyle='round,pad=0.5', facecolor='orange', alpha=0.3),
             arrowprops=dict(arrowstyle='->', color='orange', lw=1.5))

# 4. 阈值对比表（关键发现）
ax4 = axes[1, 1]
ax4.axis('off')

# 创建对比表格
comparison_data = [
    ['指标', '阈值10', '阈值50', '差异', '影响'],
    ['孤立节点(2024)', '51.9%', '74.6%', '+22.7%', '⚠️ 严重'],
    ['边数(2024)', '64,120', '8,684', '-86%', '⚠️ 信息丢失'],
    ['平均度数(2024)', '29.33', '3.97', '-86%', '⚠️ 连通性差'],
    ['', '', '', '', ''],
    ['推荐', '✓ 阈值10-20', '', '', ''],
    ['或', '✓ 混合图', '', '', ''],
    ['或', '✓ 过滤孤立节点', '', '', ''],
]

table = ax4.table(cellText=comparison_data, cellLoc='center', loc='center',
                  colWidths=[0.2, 0.15, 0.15, 0.2, 0.2])
table.auto_set_font_size(False)
table.set_fontsize(11)
table.scale(1, 2.5)

# 设置表头样式
for i in range(5):
    table[(0, i)].set_facecolor('#4CAF50')
    table[(0, i)].set_text_props(weight='bold', color='white')

# 设置关键行样式
for row in [1, 2, 3]:
    for col in range(5):
        table[(row, col)].set_facecolor('#ffebee')

# 设置推荐行样式（检查行是否存在）
for row in [6, 7, 8]:
    try:
        for col in range(5):
            table[(row, col)].set_facecolor('#e8f5e9')
            table[(row, col)].set_text_props(weight='bold')
    except KeyError:
        pass

ax4.set_title('💡 关键发现与建议', fontsize=14, fontweight='bold', pad=20)

plt.tight_layout()

# 保存
output_path = '/workspace/Graph_Deep_Learning/20251001-PRD_18-21-24-mobility_change_pattern/vis/analysis_results/threshold_analysis.png'
plt.savefig(output_path, dpi=300, bbox_inches='tight')
print(f"✓ 图表已保存: {output_path}")

# 也显示图表
plt.show()

print("\n" + "="*80)
print("【总结】")
print("="*80)
print("阈值50导致的问题：")
print("  1. 64-75%的节点孤立，无法利用GNN的消息传递机制")
print("  2. 平均度数仅1.69-3.97，图结构过于稀疏")
print("  3. 丢失了86%的边（相比阈值10），大量空间连接信息被过滤")
print("\n这直接导致你的模型精度<55%！")
print("\n建议操作：")
print("  立即修改 FLOW_THRESHOLD = 10.0 或 20.0，重新训练模型")
print("="*80)
