#!/usr/bin/env python3
"""
Visualize threshold analysis results (English version)
"""
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Data
thresholds = [10, 20, 30, 40, 50, 75, 100]
isolated_2021 = [52.4, 57.1, 60.1, 62.4, 64.2, 68.2, 71.2]
isolated_2024 = [51.9, 60.4, 66.4, 71.1, 74.6, 81.8, 87.1]
edges_2021 = [39530, 15028, 8137, 5194, 3692, 2224, 1624]
edges_2024 = [64120, 30594, 18299, 12086, 8684, 4402, 2508]
avg_deg_2021 = [18.08, 6.87, 3.72, 2.38, 1.69, 1.02, 0.74]
avg_deg_2024 = [29.33, 14.00, 8.37, 5.53, 3.97, 2.01, 1.15]

# Create figure
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('Impact of Flow Threshold on Graph Structure (Sample: 4372 grids)',
             fontsize=18, fontweight='bold', y=0.995)

# 1. Isolated nodes ratio (most important)
ax1 = axes[0, 0]
ax1.plot(thresholds, isolated_2021, 'o-', label='2021', linewidth=2.5, markersize=8, color='#1f77b4')
ax1.plot(thresholds, isolated_2024, 's-', label='2024', linewidth=2.5, markersize=8, color='#ff7f0e')
ax1.axvline(x=50, color='red', linestyle='--', linewidth=2, alpha=0.7, label='Current threshold=50')
ax1.axhline(y=50, color='gray', linestyle=':', linewidth=1.5, alpha=0.5, label='50% warning line')
ax1.fill_between(thresholds, isolated_2021, isolated_2024, alpha=0.1, color='red')
ax1.set_xlabel('Flow Threshold (persons)', fontsize=13, fontweight='bold')
ax1.set_ylabel('Isolated Nodes Ratio (%)', fontsize=13, fontweight='bold')
ax1.set_title('Isolated Nodes vs Threshold (Lower is better, >50% is critical)',
              fontsize=14, fontweight='bold')
ax1.legend(fontsize=11, loc='upper left')
ax1.grid(True, alpha=0.3, linestyle='--')
ax1.set_xlim(5, 105)
ax1.set_ylim(45, 90)

# Annotate key point
ax1.annotate(f'Threshold 50:\n2021: 64.2% isolated\n2024: 74.6% isolated',
             xy=(50, isolated_2024[4]), xytext=(65, 85),
             fontsize=10, fontweight='bold',
             bbox=dict(boxstyle='round,pad=0.5', facecolor='red', alpha=0.3),
             arrowprops=dict(arrowstyle='->', color='red', lw=2))

# 2. Number of edges
ax2 = axes[0, 1]
ax2.plot(thresholds, edges_2021, 'o-', label='2021', linewidth=2.5, markersize=8, color='#1f77b4')
ax2.plot(thresholds, edges_2024, 's-', label='2024', linewidth=2.5, markersize=8, color='#ff7f0e')
ax2.axvline(x=50, color='red', linestyle='--', linewidth=2, alpha=0.7, label='Current threshold=50')
ax2.set_xlabel('Flow Threshold (persons)', fontsize=13, fontweight='bold')
ax2.set_ylabel('Number of Edges', fontsize=13, fontweight='bold')
ax2.set_title('Edges vs Threshold (Higher is denser)', fontsize=14, fontweight='bold')
ax2.legend(fontsize=11)
ax2.grid(True, alpha=0.3, linestyle='--')
ax2.set_xlim(5, 105)
ax2.set_yscale('log')

# Annotate edge loss
ax2.annotate(f'Threshold 10 -> 50:\n2021: -90% edges\n2024: -86% edges',
             xy=(10, edges_2021[0]), xytext=(15, 10000),
             fontsize=9,
             bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.4),
             arrowprops=dict(arrowstyle='->', color='orange', lw=1.5))

# 3. Average degree
ax3 = axes[1, 0]
ax3.plot(thresholds, avg_deg_2021, 'o-', label='2021', linewidth=2.5, markersize=8, color='#1f77b4')
ax3.plot(thresholds, avg_deg_2024, 's-', label='2024', linewidth=2.5, markersize=8, color='#ff7f0e')
ax3.axvline(x=50, color='red', linestyle='--', linewidth=2, alpha=0.7, label='Current threshold=50')
ax3.axhline(y=2, color='green', linestyle=':', linewidth=2, alpha=0.5, label='Healthy degree=2')
ax3.set_xlabel('Flow Threshold (persons)', fontsize=13, fontweight='bold')
ax3.set_ylabel('Average Degree', fontsize=13, fontweight='bold')
ax3.set_title('Average Degree vs Threshold (Higher is better connectivity)',
              fontsize=14, fontweight='bold')
ax3.legend(fontsize=11, loc='upper right')
ax3.grid(True, alpha=0.3, linestyle='--')
ax3.set_xlim(5, 105)

# Annotate
ax3.annotate(f'2024 Data:\nThresh 50: Only 3.97\nThresh 10: 29.33',
             xy=(50, avg_deg_2024[4]), xytext=(70, 20),
             fontsize=9, fontweight='bold',
             bbox=dict(boxstyle='round,pad=0.5', facecolor='orange', alpha=0.3),
             arrowprops=dict(arrowstyle='->', color='orange', lw=1.5))

# 4. Comparison table
ax4 = axes[1, 1]
ax4.axis('off')

# Create comparison table
comparison_data = [
    ['Metric', 'Threshold 10', 'Threshold 50', 'Difference', 'Impact'],
    ['Isolated (2024)', '51.9%', '74.6%', '+22.7%', 'CRITICAL'],
    ['Edges (2024)', '64,120', '8,684', '-86%', 'INFO LOSS'],
    ['Avg Degree (2024)', '29.33', '3.97', '-86%', 'POOR CONN'],
    ['', '', '', '', ''],
    ['RECOMMENDED', 'Use threshold 10-20', '', '', ''],
    ['OR', 'Use hybrid graph', '', '', ''],
    ['OR', 'Filter isolated nodes', '', '', ''],
]

table = ax4.table(cellText=comparison_data, cellLoc='center', loc='center',
                  colWidths=[0.2, 0.15, 0.15, 0.2, 0.2])
table.auto_set_font_size(False)
table.set_fontsize(11)
table.scale(1, 2.5)

# Set header style
for i in range(5):
    table[(0, i)].set_facecolor('#4CAF50')
    table[(0, i)].set_text_props(weight='bold', color='white')

# Set critical rows style
for row in [1, 2, 3]:
    for col in range(5):
        table[(row, col)].set_facecolor('#ffebee')

# Set recommended rows style
for row in [6, 7, 8]:
    try:
        for col in range(5):
            table[(row, col)].set_facecolor('#e8f5e9')
            table[(row, col)].set_text_props(weight='bold')
    except KeyError:
        pass

ax4.set_title('KEY FINDINGS & RECOMMENDATIONS', fontsize=14, fontweight='bold', pad=20)

plt.tight_layout()

# Save
output_path = '/workspace/Graph_Deep_Learning/20251001-PRD_18-21-24-mobility_change_pattern/vis/analysis_results/threshold_analysis_en.png'
plt.savefig(output_path, dpi=300, bbox_inches='tight')
print(f"✓ Chart saved: {output_path}")

# Also show
plt.show()

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print("Problems with threshold 50:")
print("  1. 64-75% nodes are isolated, cannot use GNN message passing")
print("  2. Average degree only 1.69-3.97, graph is too sparse")
print("  3. Lost 86% of edges (compared to threshold 10)")
print("\nThis directly causes your model accuracy < 55%!")
print("\nRECOMMENDED ACTION:")
print("  Immediately change FLOW_THRESHOLD = 10.0 or 20.0 and retrain")
print("="*80)
