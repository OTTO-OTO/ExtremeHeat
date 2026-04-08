import pandas as pd
import os
import time
from scipy.spatial import cKDTree
import json
import numpy as np

def enrich_master_table_spatial(master_table_path, new_excel_path, output_prefix):
    """
    使用空间邻近匹配，将新的Excel数据源增补到已有的总表中。
    同时生成更新后的CSV和JSON文件。
    """
    start_time = time.time()

    # --- 1. 准备和清洗新数据 (DICL) ---
    try:
        print(f"步骤 1: 正在从Excel文件 '{new_excel_path}' 加载并清洗新数据...")
        
        df_over = pd.read_excel(new_excel_path, sheet_name='OverReport')
        df_under = pd.read_excel(new_excel_path, sheet_name='UnderReport')
        
        dicl_df = pd.concat([df_over, df_under], ignore_index=True)
        print(f"  - 已合并两个Sheet，共 {len(dicl_df)} 条记录。")

        dicl_df = dicl_df.rename(columns={'X': 'longitude', 'Y': 'latitude', 'Country_ENG.x': 'Country_ENG'})
        
        columns_to_use = ['latitude', 'longitude', 'LPN2CommonLanguage_2024']
        dicl_df_clean = dicl_df[columns_to_use].copy()
        
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
    
    tree = cKDTree(dicl_df_clean[['latitude', 'longitude']].values)
    distances, indices = tree.query(master_df[['latitude', 'longitude']].values, k=1)
    
    matched_data = dicl_df_clean.iloc[indices]
    
    # 将新的数据列添加到总表中
    master_df['LPN2CommonLanguage_2024'] = matched_data['LPN2CommonLanguage_2024'].values
    print("  - 匹配和增补完成！")

    # --- 4. 保存最终结果 ---
    print("\n步骤 4: 正在保存已增补的最终结果...")
    
    output_csv_path = f'{output_prefix}_V2.csv'
    output_json_path = f'{output_prefix}_V2.json'

    # a) 保存为新的CSV
    master_df.to_csv(output_csv_path, index=False, float_format='%.4f')
    print(f"  - 已增补的CSV总表已保存至: {output_csv_path}")

    # b) 【完整功能】生成并保存更新后的JSON文件
    json_output_data = []
    # 识别哪些是静态列，哪些是时间序列列
    # 包含了所有时间序列数据的前缀
    timeseries_prefixes = ('era5_', 'cmip5_', 'gdp_', 'pop_', 'ccvi_', 'cli_', 'con_', 'worldpop_', 'NDGAIN_', 'WGI_', 'ICT_')
    static_columns = [col for col in master_df.columns if not col.startswith(timeseries_prefixes)]

    df_for_json = master_df.replace({np.nan: None})

    for record in df_for_json.to_dict('records'):
        # 1. 提取所有静态属性 (新加入的列会自动被包含在这里)
        location_attributes = {key: record[key] for key in static_columns}
        
        # 2. 初始化时间序列数据的结构
        timeseries_data = {
            "era5_temperature_celsius": {}, "cmip5_precipitation_mm_year": {},
            "ccvi_risk": {"ccvi_yearly_avg": {}, "cli_risk_yearly_avg": {}, "con_risk_yearly_avg": {}},
            "gdp": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}},
            "population_ssp": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}}, # 为了清晰，重命名pop
            "worldpop_population": {},
            "ndgain": {"ndgain_score": {}, "vulnerability_components": {}},
            "wgi_governance": {
                "voice_and_accountability": {}, "political_stability": {},
                "government_effectiveness": {}, "regulatory_quality": {},
                "rule_of_law": {}, "control_of_corruption": {}
            },
            "ict_internet_user_percentage": {}
        }

        # 3. 遍历并填充时间序列数据
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
            # 【注意】这里我们将旧的'pop'逻辑更新为'population_ssp'
            elif prefix == 'pop':
                ssp = parts[1]; year = parts[2]; timeseries_data['population_ssp'][ssp][year] = value
            
            # --- 以下是需要新增的逻辑 ---
            elif prefix == 'worldpop':
                year = parts[1]
                timeseries_data['worldpop_population'][year] = value

            elif prefix == 'NDGAIN':
                year = parts[-1]
                sub_variable = '_'.join(parts[1:-1]).lower()
                if sub_variable == 'gain':
                    timeseries_data['ndgain']['ndgain_score'][year] = value
                else:
                    if sub_variable not in timeseries_data['ndgain']['vulnerability_components']:
                        timeseries_data['ndgain']['vulnerability_components'][sub_variable] = {}
                    timeseries_data['ndgain']['vulnerability_components'][sub_variable][year] = value

            elif prefix == 'WGI':
                # 需要先定义一个WGI的缩写映射
                wgi_map = {
                    'va': 'voice_and_accountability', 'pv': 'political_stability', 'ge': 'government_effectiveness',
                    'rq': 'regulatory_quality', 'rl': 'rule_of_law', 'cc': 'control_of_corruption'
                }
                indicator_abbr = parts[1]
                year = parts[2]
                full_indicator_name = wgi_map.get(indicator_abbr)
                if full_indicator_name:
                    timeseries_data['wgi_governance'][full_indicator_name][year] = value

            elif prefix == 'ICT':
                year = parts[2]
                timeseries_data['ict_internet_user_percentage'][year] = value
        
        json_output_data.append({
            'location_attributes': location_attributes,
            'timeseries_data': timeseries_data
        })

    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(json_output_data, f, ensure_ascii=False, indent=4)
    print(f"  - 已增补的JSON总表已保存至: {output_json_path}")


    print("-" * 40)
    end_time = time.time()
    print(f"数据增补流程完毕！总耗时: {end_time - start_time:.2f} 秒。")

    return master_df

# --- 如何使用这个函数 ---
if __name__ == '__main__':
    MASTER_TABLE_PATH = './results/FINAL_MASTER_TABLE.csv'
    NEW_DATA_EXCEL_PATH = './DICL_joined.xlsx' # 请确保这是您新数据的正确路径和文件名
    FINAL_OUTPUT_PREFIX = './results/FINAL_MASTER_TABLE'

    final_enriched_dataframe = enrich_master_table_spatial(MASTER_TABLE_PATH, NEW_DATA_EXCEL_PATH, FINAL_OUTPUT_PREFIX)

    if final_enriched_dataframe is not None:
        print("\n函数返回的最终增补DataFrame预览:")
        key_cols = ['latitude', 'longitude', 'Country_ENG', 'report_type', 'LPN2CommonLanguage_2024']
        print(final_enriched_dataframe[key_cols].head())