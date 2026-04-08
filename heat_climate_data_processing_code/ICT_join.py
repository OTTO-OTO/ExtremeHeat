import pandas as pd
import os
import time
from scipy.spatial import cKDTree
import json
import numpy as np

def enrich_master_table_with_ict(master_table_path, new_excel_path, output_prefix):
    """
    使用ICT数据源来丰富和更新已有的总表。
    采用简化的JSON结构。
    """
    start_time = time.time()

    # --- 1. 准备和清洗新数据 (ICT) ---
    try:
        print(f"步骤 1: 正在从Excel文件 '{new_excel_path}' 加载并清洗ICT数据...")
        
        df_over = pd.read_excel(new_excel_path, sheet_name='OverReport')
        df_under = pd.read_excel(new_excel_path, sheet_name='UnderReport')
        
        ict_df = pd.concat([df_over, df_under], ignore_index=True)
        print(f"  - 已合并两个Sheet，共 {len(ict_df)} 条记录。")

        ict_df = ict_df.rename(columns={'X': 'longitude', 'Y': 'latitude'})
        
        columns_to_keep = ['latitude', 'longitude'] + [col for col in ict_df.columns if col.startswith('ICT_NETUSER_')]
        ict_df_clean = ict_df[columns_to_keep].copy()
        print(f"  - 已提取 {len(columns_to_keep) - 2} 个ICT指标列。")
        
    except Exception as e:
        print(f"处理新数据Excel文件时发生错误: {e}")
        return None

    # --- 2. 加载总表 ---
    try:
        print(f"\n步骤 2: 正在加载当前的总表: {master_table_path}")
        master_df = pd.read_csv(master_table_path)
        print("  - 总表加载成功。")

    except FileNotFoundError:
        print(f"错误：找不到总表文件 '{master_table_path}'。")
        return None

    # --- 3. 执行空间邻近匹配 ---
    print("\n步骤 3: 正在执行空间邻近匹配...")
    
    tree = cKDTree(ict_df_clean[['latitude', 'longitude']].values)
    distances, indices = tree.query(master_df[['latitude', 'longitude']].values, k=1)
    
    matched_data = ict_df_clean.iloc[indices]
    
    new_data_cols = [col for col in matched_data.columns if col not in ['latitude', 'longitude']]
    master_df[new_data_cols] = matched_data[new_data_cols].values
    print("  - 匹配和增补完成！")

    # --- 4. 保存最终结果 ---
    print("\n步骤 4: 正在保存已增补的最终结果...")
    
    output_csv_path = f'{output_prefix}_V7.csv'
    output_json_path = f'{output_prefix}_V7.json'

    master_df.to_csv(output_csv_path, index=False, float_format='%.4f')
    print(f"  - 已增补的CSV总表已保存至: {output_csv_path}")

    # 【核心修改】更新JSON生成逻辑以简化ICT结构
    timeseries_prefixes = ('era5_', 'cmip5_', 'gdp_', 'pop_', 'ccvi_', 'cli_', 'con_', 'NDGAIN_', 'WGI_', 'ICT_')
    static_columns = [col for col in master_df.columns if not col.startswith(timeseries_prefixes)]
    df_for_json = master_df.replace({np.nan: None})
    json_output_data = []

    for record in df_for_json.to_dict('records'):
        location_attributes = {key: record[key] for key in static_columns}
        
        # 1. 修改初始化字典的结构
        timeseries_data = {
            "era5_temperature_celsius": {}, "cmip5_precipitation_mm_year": {},
            "ccvi_risk": {"ccvi_yearly_avg": {}, "cli_risk_yearly_avg": {}, "con_risk_yearly_avg": {}},
            "gdp": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}},
            "population": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}},
            "ndgain": {"ndgain_score": {}, "vulnerability_components": {}},
            "wgi_governance": {"voice_and_accountability": {}, "political_stability": {}, "government_effectiveness": {}, "regulatory_quality": {}, "rule_of_law": {}, "control_of_corruption": {}},
            "ict_internet_user_percentage": {} # <-- 简化后的新键名
        }
        
        for col, value in record.items():
            if col in static_columns or value is None: continue
            
            # 2. 修改填充数据的逻辑
            if col.startswith('ICT_NETUSER_'):
                parts = col.split('_')
                year = parts[2]
                # 直接填充到新的、扁平的键中
                timeseries_data['ict_internet_user_percentage'][year] = value
            # ... 其他数据集的逻辑保持不变 ...
            else:
                # ... (此处省略的代码块与上一版本完全一致) ...
                parts = col.split('_'); prefix = parts[0]
                if prefix == 'era5': timeseries_data["era5_temperature_celsius"][parts[2]] = value
                elif prefix == 'cmip5': timeseries_data["cmip5_precipitation_mm_year"][parts[2]] = value
                elif prefix in ['ccvi', 'cli', 'con']:
                    year = parts[-1]; var_name = '_'.join(parts[:-1]); dict_key = f'{var_name}_yearly_avg'
                    if dict_key in timeseries_data["ccvi_risk"]: timeseries_data["ccvi_risk"][dict_key][year] = value
                elif prefix == 'gdp': ssp = parts[1]; year = parts[2]; timeseries_data['gdp'][ssp][year] = value
                elif prefix == 'pop': ssp = parts[1]; year = parts[2]; timeseries_data['population'][ssp][year] = value
        
        json_output_data.append({'location_attributes': location_attributes, 'timeseries_data': timeseries_data})
        
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(json_output_data, f, ensure_ascii=False, indent=4)
    print(f"  - 已增补的JSON总表已保存至: {output_json_path}")

    print("-" * 40)
    end_time = time.time()
    print(f"数据增补流程完毕！总耗时: {end_time - start_time:.2f} 秒。")

    return master_df



# --- 如何使用这个函数 ---
if __name__ == '__main__':
    MASTER_TABLE_PATH = './results/FINAL_MASTER_TABLE_V6.csv'
    NEW_DATA_EXCEL_PATH = './ICT_joined.xlsx' # 请替换为实际文件名
    FINAL_OUTPUT_PREFIX = './results/FINAL_MASTER_TABLE'

    final_enriched_dataframe = enrich_master_table_with_ict(MASTER_TABLE_PATH, NEW_DATA_EXCEL_PATH, FINAL_OUTPUT_PREFIX)

    if final_enriched_dataframe is not None:
        print("\n函数返回的最终增补DataFrame预览:")
        # 挑选几个关键列预览
        preview_cols = ['latitude', 'longitude', 'Country_ENG'] + [col for col in final_enriched_dataframe.columns if 'ICT_NETUSER' in col]
        print(final_enriched_dataframe[preview_cols].head())