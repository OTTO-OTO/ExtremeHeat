import pandas as pd
import json
import glob
import os
import time
import numpy as np
from scipy.spatial import cKDTree

def create_final_master_table(base_coords_attr_path, results_folder_path, output_prefix):
    """
    读取新的基础坐标属性表，并使用空间邻近匹配，合并所有已处理的数据集结果。
    最终版：生成完整的CSV、JSON产出，并添加uid和重排序列。
    """
    start_time = time.time()
    
    # --- 1. 加载基础DataFrame ---
    try:
        print(f"步骤 1: 正在加载新的基础坐标与属性文件: {base_coords_attr_path}")
        if base_coords_attr_path.endswith('.xlsx'):
            base_df = pd.read_excel(base_coords_attr_path, engine='openpyxl')
        else:
            try:
                base_df = pd.read_csv(base_coords_attr_path, encoding='utf-8')
            except UnicodeDecodeError:
                print("  - UTF-8编码读取失败，正在尝试使用GBK编码...")
                base_df = pd.read_csv(base_coords_attr_path, encoding='gbk')

        if 'X' in base_df.columns and 'Y' in base_df.columns:
            base_df = base_df.rename(columns={'X': 'longitude', 'Y': 'latitude'})
        
        static_columns_base = base_df.columns.tolist()
        print(f"基础表加载成功，共 {len(base_df)} 条记录。")
    except FileNotFoundError:
        print(f"错误：找不到基础坐标文件 '{base_coords_attr_path}'。请检查路径。")
        return None
        
    final_df = base_df.copy()

    # --- 2. 逐一进行空间邻近匹配 ---
    print("\n步骤 2: 开始使用空间邻近匹配合并所有新生成的结果...")
    files_to_merge = sorted(glob.glob(os.path.join(results_folder_path, 'results_*.csv')))
    
    if not files_to_merge:
        print(f"警告：在文件夹 '{results_folder_path}' 中没有找到任何results_*.csv文件。")
    
    for file_path in files_to_merge:
        try:
            filename = os.path.basename(file_path)
            print(f"  - 正在匹配: {filename}")
            data_df = pd.read_csv(file_path)

            tree = cKDTree(data_df[['latitude', 'longitude']].values)
            distances, indices = tree.query(final_df[['latitude', 'longitude']].values, k=1)
            
            matched_data = data_df.iloc[indices]
            
            data_cols = [col for col in data_df.columns if col not in ['latitude', 'longitude']]
            final_df[data_cols] = matched_data[data_cols].values
        except Exception as e:
            print(f"  - 处理文件 {filename} 时发生错误: {e}")
    
    print("所有数据集已全部匹配！")
    
    # --- 3. 最终精修：添加UID和重排序列 ---
    print("\n步骤 3: 正在进行最终精修（添加uid，重排序列）...")
    final_df.insert(0, 'uid', range(1, len(final_df) + 1))
    print("  - 已添加从1开始的uid列。")
    
    desired_order_base = ['uid', 'longitude', 'latitude']
    if 'report_type' in final_df.columns:
        desired_order_base.append('report_type')
        
    other_columns = [col for col in final_df.columns if col not in desired_order_base]
    new_column_order = desired_order_base + other_columns
    final_df = final_df[new_column_order]
    print(f"  - 已按要求重排关键列。")

    # --- 4. 生成最终产出 ---
    print("\n步骤 4: 正在生成最终的输出文件...")
    output_csv_path = f'{output_prefix}_NEW.csv'
    output_json_path = f'{output_prefix}_NEW.json'

    final_df.to_csv(output_csv_path, index=False, float_format='%.4f')
    print(f"最终的宽格式CSV母表已保存至: {output_csv_path}")

    # --- 【完整功能】生成并保存结构化的JSON ---
    timeseries_prefixes = ('era5_', 'cmip5_', 'gdp_', 'pop_', 'ccvi_', 'cli_', 'con_', 'NDGAIN_', 'WGI_', 'ICT_')
    static_columns = [col for col in final_df.columns if not col.startswith(timeseries_prefixes)]
    df_for_json = final_df.replace({np.nan: None})
    json_output_data = []

    wgi_map = {
        'va': 'voice_and_accountability', 'pv': 'political_stability', 'ge': 'government_effectiveness',
        'rq': 'regulatory_quality', 'rl': 'rule_of_law', 'cc': 'control_of_corruption'
    }

    for record in df_for_json.to_dict('records'):
        location_attributes = {key: record[key] for key in static_columns}
        timeseries_data = {
            "era5_temperature_celsius": {}, "cmip5_precipitation_mm_year": {},
            "ccvi_risk": {"ccvi_yearly_avg": {}, "cli_risk_yearly_avg": {}, "con_risk_yearly_avg": {}},
            "gdp": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}},
            "population": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}},
            "ndgain": {"ndgain_score": {}, "vulnerability_components": {}},
            "wgi_governance": {
                "voice_and_accountability": {}, "political_stability": {}, "government_effectiveness": {},
                "regulatory_quality": {}, "rule_of_law": {}, "control_of_corruption": {}
            },
            "ict_internet_user_percentage": {}
        }

        for col, value in record.items():
            if col in static_columns or value is None:
                continue
            
            parts = col.split('_'); prefix = parts[0]

            if prefix == 'era5':
                timeseries_data["era5_temperature_celsius"][parts[2]] = float(value)
            elif prefix == 'cmip5':
                timeseries_data["cmip5_precipitation_mm_year"][parts[2]] = float(value)
            elif prefix in ['ccvi', 'cli', 'con']:
                year = parts[-1]; var_name = '_'.join(parts[:-1]); dict_key = f'{var_name}_yearly_avg'
                if dict_key in timeseries_data["ccvi_risk"]:
                    timeseries_data["ccvi_risk"][dict_key][year] = float(value)
            elif prefix == 'gdp':
                ssp = parts[1]; year = parts[2]; timeseries_data['gdp'][ssp][year] = float(value)
            elif prefix == 'pop':
                ssp = parts[1]; year = parts[2]; timeseries_data['population'][ssp][year] = float(value)
            elif prefix == 'WGI':
                indicator_abbr = parts[1]; year = parts[2]
                full_indicator_name = wgi_map.get(indicator_abbr)
                if full_indicator_name:
                    timeseries_data['wgi_governance'][full_indicator_name][year] = float(value)
            elif prefix == 'ICT':
                year = parts[2]
                timeseries_data['ict_internet_user_percentage'][year] = float(value)
            elif prefix == 'NDGAIN':
                year = parts[-1]; sub_variable = '_'.join(parts[1:-1]).lower()
                if sub_variable == 'gain':
                    timeseries_data['ndgain']['ndgain_score'][year] = float(value)
                else:
                    if sub_variable not in timeseries_data['ndgain']['vulnerability_components']:
                        timeseries_data['ndgain']['vulnerability_components'][sub_variable] = {}
                    timeseries_data['ndgain']['vulnerability_components'][sub_variable][year] = float(value)
        
        json_output_data.append({'location_attributes': location_attributes, 'timeseries_data': timeseries_data})
        
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(json_output_data, f, ensure_ascii=False, indent=4)
    print(f"最终的结构化JSON母表已保存至: {output_json_path}")
    
    print("-" * 40)
    end_time = time.time()
    print(f"所有任务处理完毕！总耗时: {end_time - start_time:.2f} 秒。")

    return final_df

# --- 如何使用这个函数 ---
if __name__ == '__main__':
    NEW_BASE_EXCEL_PATH = './Ratio_country_joined_result.xlsx'
    RESULTS_FOLDER = './results'
    FINAL_OUTPUT_PREFIX = './results/FINAL_MASTER_TABLE'
    
    if not os.path.exists(RESULTS_FOLDER):
        os.makedirs(RESULTS_FOLDER)
    
    master_dataframe = create_final_master_table(NEW_BASE_EXCEL_PATH, RESULTS_FOLDER, FINAL_OUTPUT_PREFIX)
    
    if master_dataframe is not None:
        print("\n函数返回的最终DataFrame母表预览 (基于全新坐标):")
        print(master_dataframe.head())