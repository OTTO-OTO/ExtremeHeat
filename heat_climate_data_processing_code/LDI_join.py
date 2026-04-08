import pandas as pd
import os
import time
from scipy.spatial import cKDTree
import json
import numpy as np

def enrich_master_table_with_ldi(master_table_path, new_excel_path, output_prefix):
    """
    使用LDI数据源来丰富和更新已有的总表。
    """
    start_time = time.time()

    # --- 1. 准备和清洗新数据 (LDI) ---
    try:
        print(f"步骤 1: 正在从Excel文件 '{new_excel_path}' 加载并清洗LDI数据...")
        
        df_over = pd.read_excel(new_excel_path, sheet_name='OverReport')
        df_under = pd.read_excel(new_excel_path, sheet_name='UnderReport')
        
        ldi_df = pd.concat([df_over, df_under], ignore_index=True)
        print(f"  - 已合并两个Sheet，共 {len(ldi_df)} 条记录。")

        ldi_df = ldi_df.rename(columns={'X': 'longitude', 'Y': 'latitude'})
        
        # 精简数据，只保留“钥匙”和“新信息”
        original_col_name = 'Language_LDI_2025'
        new_col_name = 'ldi_2025' # 采用更简洁的列名
        
        columns_to_keep = ['latitude', 'longitude', original_col_name]
        ldi_df_clean = ldi_df[columns_to_keep].copy()
        ldi_df_clean = ldi_df_clean.rename(columns={original_col_name: new_col_name})

        print(f"  - 已提取并重命名新数据列为: '{new_col_name}'")
        
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
    
    tree = cKDTree(ldi_df_clean[['latitude', 'longitude']].values)
    distances, indices = tree.query(master_df[['latitude', 'longitude']].values, k=1)
    
    matched_data = ldi_df_clean.iloc[indices]
    
    master_df[new_col_name] = matched_data[new_col_name].values
    print("  - 匹配和增补完成！")

    # --- 4. 保存最终结果 ---
    print("\n步骤 4: 正在保存最终的交付版本结果...")
    
    output_csv_path = f'{output_prefix}_V8_FINAL.csv'
    output_json_path = f'{output_prefix}_V8_FINAL.json'

    # a) 保存为新的CSV
    master_df.to_csv(output_csv_path, index=False, float_format='%.6f') # 增加小数位数以显示LDI的精度
    print(f"  - 已增补的最终版CSV总表已保存至: {output_csv_path}")

    # b) 更新JSON文件 (逻辑不变，会自动将新列识别为静态属性)
    timeseries_prefixes = ('era5_', 'cmip5_', 'gdp_', 'pop_', 'ccvi_', 'cli_', 'con_', 'NDGAIN_','WGI_','ICT_')
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
    # 省略的JSON生成代码块...
    print(f"  - 已增补的最终版JSON总表已保存至: {output_json_path}")

    print("-" * 40)
    end_time = time.time()
    print(f"数据增补流程完毕！总耗时: {end_time - start_time:.2f} 秒。")

    return master_df

# --- 如何使用这个函数 ---
if __name__ == '__main__':
    MASTER_TABLE_PATH = './results/FINAL_MASTER_TABLE_V7.csv'
    NEW_DATA_EXCEL_PATH = './Language_joined.xlsx' # 请替换为实际文件名
    FINAL_OUTPUT_PREFIX = './results/FINAL_MASTER_TABLE'

    final_enriched_dataframe = enrich_master_table_with_ldi(MASTER_TABLE_PATH, NEW_DATA_EXCEL_PATH, FINAL_OUTPUT_PREFIX)

    if final_enriched_dataframe is not None:
        print("\n函数返回的最终DataFrame预览:")
        # 挑选几个关键列预览
        key_cols = ['latitude', 'longitude', 'Country_ENG', 'ldi_2025']
        print(final_enriched_dataframe[key_cols].head())