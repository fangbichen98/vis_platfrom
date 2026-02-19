# 截图功能恢复指南

## 当前状态

✅ **截图功能已临时禁用以提升标注速度**

## 如何恢复截图功能

编辑 `auto_label.py` 文件，找到第 **283-287行**，将以下3行的注释取消：

```python
# 从这里：
# time.sleep(0.5)  # 等待截图上传
# self.copy_and_rename_screenshot(index + i + 1, grid_id, label, label_name)  # 复制截图
# time.sleep(0.3)  # 短暂延迟

# 改为：
time.sleep(0.5)  # 等待截图上传
self.copy_and_rename_screenshot(index + i + 1, grid_id, label, label_name)  # 复制截图
time.sleep(0.3)  # 短暂延迟
```

## 性能对比

| 模式 | 速度 | 10000个用时 |
|------|------|-------------|
| **禁用截图**（当前） | 20-30个/分钟 | 5-8小时 |
| **启用截图** | 10个/分钟 | 16小时+ |

## 为什么禁用截图能提速？

1. **省略了截图等待时间**：0.5秒 × 10000 = 83分钟
2. **省略了文件复制操作**：磁盘I/O操作
3. **省略了延迟时间**：0.3秒 × 10000 = 50分钟

总计节省：~2小时+

## 截图去哪了？

虽然脚本不复制截图，但**前端浏览器仍然会上传截图到服务器**：
- 路径：`vis/labels/shots/`
- 文件名格式：`{格网ID}-{标签}.jpg`

如果需要查看截图，可以直接从这个目录获取。

## 批量重命名已有截图

如果需要将 `vis/labels/shots/` 中的截图按序号重命名：

```bash
cd vis/labels
mkdir -p renamed
i=1
for file in shots/*.jpg; do
  if [ -f "$file" ]; then
    filename=$(basename "$file")
    # 提取格网ID和标签
    grid_id=$(echo $filename | cut -d'-' -f1)
    label=$(echo $filename | cut -d'-' -f2 | cut -d'.' -f1)

    # 根据标签获取类型名称
    case $label in
      1) label_name="1稳定静态型" ;;
      2) label_name="2稳定聚集型" ;;
      3) label_name="3稳定扩散型" ;;
      4) label_name="4增长静态型" ;;
      5) label_name="5增长聚集型" ;;
      6) label_name="6增长扩散型" ;;
      7) label_name="7衰减静态型" ;;
      8) label_name="8衰减聚集型" ;;
      9) label_name="9衰减扩散型" ;;
    esac

    # 重命名并复制
    cp "$file" "renamed/${i}_${grid_id}_${label_name}.jpg"
    ((i++))
  fi
done
```
