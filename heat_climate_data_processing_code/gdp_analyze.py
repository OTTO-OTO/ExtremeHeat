import rasterio

# --- 1. 定义要侦察的文件路径 ---
file_path = 'rast_gdpTot_1990_2020_30arcsec.tif'

# --- 2. 使用 rasterio 打开并打印概览信息 ---
try:
    with rasterio.open(file_path) as src:
        print(f"开始分析文件: {file_path}\n")
        print("="*50)
        print("GeoTIFF 文件概览信息:")
        
        # 1. 数据波段数量 (Count) - 这是最关键的信息
        print(f"  - 波段数量 (Count): {src.count}")
        
        # 2. 图像尺寸 (像素)
        print(f"  - 宽度 (Width): {src.width} 像素")
        print(f"  - 高度 (Height): {src.height} 像素")
        
        # 3. 坐标参考系统 (CRS)
        print(f"  - 坐标参考系统 (CRS): {src.crs}")
        
        # 4. 地理边界 (Bounds)
        print(f"  - 地理边界 (Bounds): {src.bounds}")
        
        # 5. 数据类型 (Data Type)
        print(f"  - 数据类型 (Data Type): {src.dtypes}")

        # 6. NoData值
        print(f"  - NoData 值: {src.nodata}")
        
        # 7. 波段描述 (Band Descriptions) - 同样非常关键
        #    这会告诉我们每个波段代表什么年份
        print("\n  - 波段描述:")
        for i in range(1, src.count + 1):
            # 尝试获取每个波段的描述信息
            desc = src.descriptions[i-1]
            if desc:
                print(f"    - 波段 {i}: {desc}")
            else:
                print(f"    - 波段 {i}: (无描述信息)")

        print("="*50)

except FileNotFoundError:
    print(f"错误：找不到文件 '{file_path}'。请检查路径是否正确。")
except Exception as e:
    print(f"读取文件时发生错误: {e}")