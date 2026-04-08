import xarray as xr
import pandas as pd

# --- 1. 定义文件路径 ---
# 假设您的文件在 'heatdata' 文件夹下
file_path = 't2m_mean_yearly_2015_2024_025degree_global.nc'

# --- 2. 使用 xarray 打开文件 ---
# a. 使用 with 语句可以确保文件在操作后被安全关闭
# b. decode_times=True (默认) 会自动将文件中的时间单位转为易于理解的日期时间格式
try:
    with xr.open_dataset(file_path) as ds:
        # --- 3. 打印数据集的概览信息 ---
        print("数据集概览信息:")
        print(ds)

except FileNotFoundError:
    print(f"错误：找不到文件 '{file_path}'。请检查路径是否正确。")
except Exception as e:
    print(f"读取文件时发生错误: {e}")