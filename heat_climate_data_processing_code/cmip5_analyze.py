import xarray as xr

# --- 1. 定义要侦察的文件路径 ---
# 请确保这个路径在您的集群上是正确的
file_path = 'prAdjust_day_GFDL-ESM2G_SMHI-DBSrev930-GFD-1981-2010-postproc_rcp85_r1i1p1_20210101-20251231.nc'

# --- 2. 使用 xarray 打开并打印概览信息 ---
try:
    with xr.open_dataset(file_path) as ds:
        print(f"开始分析文件: {file_path}\n")
        print("="*50)
        print("数据集概览信息:")
        print(ds)
        print("="*50)

        # 额外检查一下数据变量的详细属性，特别是单位
        # 假设变量名是 'prAdjust'，如果不是，请根据下面的输出修改
        try:
            var_name = 'prAdjust' # 根据文件名猜测，可能需要修改
            print(f"\n变量 '{var_name}' 的详细信息:")
            print(ds[var_name])
        except KeyError:
            print(f"\n注意：变量名不是 '{var_name}'，请查看上面的 'Data variables' 部分来确定正确的变量名。")


except FileNotFoundError:
    print(f"错误：找不到文件 '{file_path}'。请检查路径是否正确。")
except Exception as e:
    print(f"读取文件时发生错误: {e}")