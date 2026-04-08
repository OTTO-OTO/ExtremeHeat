import pandas as pd
import json
import glob
import os
import time
import numpy as np
from scipy.spatial import cKDTree

def combine_all_datasets_spatial_optimized(coords_with_attr_path, results_folder_path):
    """
    使用空间邻近匹配来合并所有数据集。
    最终稳健版：通过精确选择列来避免列名重复和UserWarning。
    """
    start_time = time.time()
    
    # --- 1. 加载基础DataFrame ---
    try:
        print(f"步骤 1: 正在加载基础坐标与属性文件: {coords_with_attr_path}")
        try:
            base_df = pd.read_csv(coords_with_attr_path, encoding='utf-8')
        except UnicodeDecodeError:
            print("  - UTF-8编码读取失败，正在尝试使用GBK编码...")
            base_df = pd.read_csv(coords_with_attr_path, encoding='gbk')
            
        if 'X' in base_df.columns and 'Y' in base_df.columns:
            base_df = base_df.rename(columns={'X': 'longitude', 'Y': 'latitude'})
        
        static_columns = base_df.columns.tolist()
        print("基础表加载成功。")
    except FileNotFoundError:
        print(f"错误：找不到基础坐标文件 '{coords_with_attr_path}'。请检查路径。")
        return None
        
    # --- 2. 逐一匹配数据并存入列表 ---
    print("\n步骤 2: 开始匹配所有数据集...")
    dataframes_to_join = []

    all_files_to_merge = [
        'results_era5_t2m_yearly_wide.csv', 'results_cmip5_prAdjust_yearly_wide.csv', 'results_ccvi_yearly_wide.csv'
    ]
    for ssp_num in range(1, 6):
        ssp_name = f'ssp{ssp_num}'
        all_files_to_merge.append(f'results_gdp_{ssp_name}_yearly_wide.csv')
        all_files_to_merge.append(f'results_pop_{ssp_name}_yearly_wide.csv')

    for filename in all_files_to_merge:
        try:
            file_path = os.path.join(results_folder_path, filename)
            print(f"  - 正在匹配: {filename}")
            data_df = pd.read_csv(file_path)

            # 【核心修正】精确识别并只挑选出时间序列数据列
            time_series_cols = [
                col for col in data_df.columns 
                if col.startswith(('era5_', 'cmip5_', 'gdp_', 'pop_', 'ccvi_', 'cli_', 'con_'))
            ]
            
            # 如果没有找到任何时间序列列，就跳过这个文件
            if not time_series_cols:
                print(f"  - 警告: 在文件 {filename} 中未找到有效的时间序列数据列，跳过。")
                continue

            # 用来做空间匹配的子表只包含经纬度和时间序列列
            df_to_join_from = data_df[['latitude', 'longitude'] + time_series_cols]

            tree = cKDTree(df_to_join_from[['latitude', 'longitude']].values)
            distances, indices = tree.query(base_df[['latitude', 'longitude']].values, k=1)
            
            matched_data = df_to_join_from.iloc[indices]
            
            # 只将纯时间序列数据部分添加到待合并列表中
            dataframes_to_join.append(matched_data[time_series_cols].reset_index(drop=True))

        except FileNotFoundError:
            print(f"  - 警告：未找到文件 {filename}，跳过此数据集。")
        except Exception as e:
            print(f"  - 处理文件 {filename} 时发生错误: {e}")
    
    # --- 3. 一次性合并所有数据 ---
    print("\n步骤 3: 正在一次性合并所有数据...")
    final_df = pd.concat([base_df] + dataframes_to_join, axis=1)
    print("所有数据集已全部合并！")
    
    # --- 4. 生成最终产出 ---
    print("\n步骤 4: 正在生成最终的输出文件...")
    final_csv_path = os.path.join(results_folder_path, 'FINAL_MASTER_TABLE.csv')
    final_df.to_csv(final_csv_path, index=False, float_format='%.4f')
    print(f"最终的宽格式CSV母表已保存至: {final_csv_path}")

    final_json_path = os.path.join(results_folder_path, 'FINAL_MASTER_TABLE.json')
    json_output_data = []
    df_for_json = final_df.replace({np.nan: None})

    for record in df_for_json.to_dict('records'):
        location_attributes = {key: record[key] for key in static_columns}
        timeseries_data = {
            "era5_temperature_celsius": {}, "cmip5_precipitation_mm_year": {},
            "ccvi_risk": {"ccvi_yearly_avg": {}, "cli_risk_yearly_avg": {}, "con_risk_yearly_avg": {}},
            "gdp": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}},
            "population": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}}
        }
        for col, value in record.items():
            if col in static_columns or value is None:
                continue
            parts = col.split('_')
            prefix = parts[0]
            if prefix == 'era5':
                timeseries_data["era5_temperature_celsius"][parts[2]] = value
            elif prefix == 'cmip5':
                timeseries_data["cmip5_precipitation_mm_year"][parts[2]] = value
            elif prefix in ['ccvi', 'cli', 'con']:
                year = parts[-1]; var_name = '_'.join(parts[:-1]); dict_key = f'{var_name}_yearly_avg'
                if dict_key in timeseries_data["ccvi_risk"]: timeseries_data["ccvi_risk"][dict_key][year] = value
            elif prefix == 'gdp':
                ssp = parts[1]; year = parts[2]; timeseries_data['gdp'][ssp][year] = value
            elif prefix == 'pop':
                ssp = parts[1]; year = parts[2]; timeseries_data['population'][ssp][year] = value
        json_output_data.append({'location_attributes': location_attributes, 'timeseries_data': timeseries_data})
        
    with open(final_json_path, 'w', encoding='utf-8') as f:
        json.dump(json_output_data, f, ensure_ascii=False, indent=4)
    print(f"结构化JSON母表已保存至: {final_json_path}")
    
    print("-" * 40)
    end_time = time.time()
    print(f"所有任务处理完毕！总耗时: {end_time - start_time:.2f} 秒。")

    return final_df

# --- 如何使用这个函数 ---
if __name__ == '__main__':
    COORDS_ATTR_PATH = './coordinates_combined_with_type.csv' 
    RESULTS_FOLDER = './results'
    if not os.path.exists(RESULTS_FOLDER):
        os.makedirs(RESULTS_FOLDER)
    master_dataframe = combine_all_datasets_spatial_optimized(COORDS_ATTR_PATH, RESULTS_FOLDER)
    if master_dataframe is not None:
        print("\n函数返回的最终DataFrame母表预览:")
        print(master_dataframe.head())