import pandas as pd
from scipy.spatial import cKDTree
import json
import time
import numpy as np

def process_ccvi_data(coordinates_csv_path, geo_lookup_path, main_data_path, output_prefix):
    """
    处理CCVI数据，通过pgid进行关联，并计算年度平均风险值。
    """
    start_time = time.time()

    # --- 1. 数据加载 ---
    try:
        print("正在加载所需文件...")
        coords_df = pd.read_csv(coordinates_csv_path)
        coords_df.columns = coords_df.columns.str.strip()
        
        geo_lookup_df = pd.read_parquet(geo_lookup_path).reset_index()

        main_data_df = pd.read_parquet(main_data_path).reset_index()
        print("所有文件加载完毕。")
    except Exception as e:
        print(f"文件加载时发生错误: {e}")
        return None

    # --- 2. 预处理主数据：计算年度平均值 ---
    print("正在计算年度平均风险值...")
    vars_to_process = ['CCVI', 'CLI_risk', 'CON_risk']
    annual_risk_df = main_data_df.groupby(['pgid', 'year'])[vars_to_process].mean().reset_index()
    print("年度平均值计算完毕。")

    # --- 3. 空间匹配：从坐标找到最近的pgid ---
    print("正在为查询坐标匹配最近的pgid...")
    tree = cKDTree(geo_lookup_df[['lat', 'lon']].values)
    distances, indices = tree.query(coords_df[['latitude', 'longitude']].values, k=1)
    matched_pgids = geo_lookup_df.loc[indices, 'pgid'].values
    coords_df['pgid'] = matched_pgids
    print("pgid匹配完成。")

    # --- 4. 属性连接：用pgid合并数据 ---
    print("正在合并数据...")
    merged_df = pd.merge(coords_df, annual_risk_df, on='pgid', how='left')
    print("数据合并完成。")

    # --- 5. 重塑数据为宽格式并输出 ---
    print("正在重塑数据并生成输出文件...")
    
    final_df_wide = merged_df.pivot_table(
        index=['latitude', 'longitude'],
        columns='year',
        values=vars_to_process
    ).reset_index()

    new_columns = []
    for col in final_df_wide.columns:
        if isinstance(col, tuple):
            if col[0] in vars_to_process:
                new_columns.append(f"{col[0].lower()}_{int(col[1])}")
            else:
                 new_columns.append(col[0])
        else:
            new_columns.append(col)
    final_df_wide.columns = new_columns

    output_csv_path = f'{output_prefix}_ccvi_yearly_wide.csv'
    final_df_wide.to_csv(output_csv_path, index=False, float_format='%.4f')
    print(f"宽格式年度CSV文件已保存至: {output_csv_path}")

    # --- d) 【完整修正功能】生成并保存为JSON ---
    output_json_path = f'{output_prefix}_ccvi_yearly.json'
    json_output_data = []
    df_for_json = final_df_wide.replace({np.nan: None})
    
    for record in df_for_json.to_dict('records'):
        lat = record.pop('latitude')
        lon = record.pop('longitude')
        
        ccvi_ts = {}
        cli_risk_ts = {}
        con_risk_ts = {}
        
        for key, value in record.items():
            if value is None: continue
            
            # 【核心修正】使用更稳健的方式解析变量名和年份
            parts = key.split('_')
            year = parts[-1]                # 年份是最后一个部分
            var_name = '_'.join(parts[:-1]) # 变量名是除年份外的所有部分
            
            if var_name == 'ccvi':
                ccvi_ts[year] = float(value)
            elif var_name == 'cli_risk':
                cli_risk_ts[year] = float(value)
            elif var_name == 'con_risk':
                con_risk_ts[year] = float(value)

        json_output_data.append({
            'latitude': float(lat),
            'longitude': float(lon),
            'ccvi_yearly_avg': ccvi_ts,
            'cli_risk_yearly_avg': cli_risk_ts,
            'con_risk_yearly_avg': con_risk_ts
        })
        
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(json_output_data, f, ensure_ascii=False, indent=4)
    print(f"结构化年度JSON文件已保存至: {output_json_path}")
    
    print("-" * 40)
    end_time = time.time()
    print(f"所有任务处理完毕！总耗时: {end_time - start_time:.2f} 秒。")
    
    return final_df_wide

# --- 如何使用这个函数 ---
if __name__ == '__main__':
    COORDS_PATH = '/Users/zhengye/ye/2024Autumn/heatevent/heatdata/master_coordinates_1015.csv' 
    GEO_LOOKUP_PATH = 'raw_data/geo/base_grid_prio.parquet'
    MAIN_DATA_PATH = 'index_data/index-full.parquet'
    OUTPUT_FILE_PREFIX = 'results'

    ccvi_dataframe = process_ccvi_data(COORDS_PATH, GEO_LOOKUP_PATH, MAIN_DATA_PATH, OUTPUT_FILE_PREFIX)

    if ccvi_dataframe is not None:
        print("\n函数返回的CCVI DataFrame结果预览:")
        print(ccvi_dataframe.head())