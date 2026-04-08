import pandas as pd

# --- 定义要侦察的文件路径 ---
# 请根据您的实际情况修改路径
geo_lookup_path = 'raw_data/geo/base_grid_prio.parquet'
main_data_path = 'index_data/index-full.parquet'

print("="*50)
print(f"开始分析文件: {geo_lookup_path}")
print("="*50)
try:
    geo_df = pd.read_parquet(geo_lookup_path)
    print("文件列名:", geo_df.columns.tolist())
    print("前5行数据预览:")
    print(geo_df.head())
except Exception as e:
    print(f"读取文件时发生错误: {e}")

print("\n" + "="*50)
print(f"开始分析文件: {main_data_path}")
print("="*50)
try:
    # 只加载我们关心的几列来查看，可以节省内存
    columns_to_read = ['CCVI', 'CLI_risk', 'CON_risk']
    # 我们还需要知道pgid的列名叫什么，先假设是'pgid'
    # 如果报错，说明pgid列名不对，需要找到正确的列名
    try:
        main_df = pd.read_parquet(main_data_path, columns=['pgid'] + columns_to_read)
    except Exception:
        # 如果'pgid'不对，尝试不带列名读取，看看到底有哪些列
        print("尝试读取'pgid'失败，将读取所有列名以供检查...")
        main_df_full_cols = pd.read_parquet(main_data_path)
        main_df = main_df_full_cols.head() # 只看前几行
        
    print("文件列名:", main_df.columns.tolist())
    print("前5行数据预览:")
    print(main_df.head())
except Exception as e:
    print(f"读取文件时发生错误: {e}")