import pandas as pd
import os
import time
from scipy.spatial import cKDTree
import json
import numpy as np

def enrich_master_table_with_ndgain_final(master_table_path, gain_excel_path, vulnerability_excel_path, output_prefix):
    """
    使用NDGAIN的两个Excel数据源来丰富和更新已有的总表。
    采用分步独立空间匹配的最终稳健策略。
    """
    start_time = time.time()
    PRECISION = 6

    # --- 1. 加载并清洗两个独立的NDGAIN数据源 ---
    try:
        print("步骤 1: 正在加载并清洗NDGAIN数据...")
        
        # a) 清洗文件一：GAIN得分
        gain_df_over = pd.read_excel(gain_excel_path, sheet_name='OverReport')
        gain_df_under = pd.read_excel(gain_excel_path, sheet_name='UnderReport')
        gain_df = pd.concat([gain_df_over, gain_df_under], ignore_index=True).rename(columns={'X': 'longitude', 'Y': 'latitude'})
        gain_cols = ['latitude', 'longitude'] + [col for col in gain_df.columns if col.startswith('NDGAIN_gain')]
        gain_df_clean = gain_df[gain_cols].copy()
        print(f"  - GAIN得分数据已加载。")

        # b) 清洗文件二：Vulnerability子项
        vul_df_over = pd.read_excel(vulnerability_excel_path, sheet_name='OverReport')
        vul_df_under = pd.read_excel(vulnerability_excel_path, sheet_name='UnderReport')
        vul_df = pd.concat([vul_df_over, vul_df_under], ignore_index=True).rename(columns={'X': 'longitude', 'Y': 'latitude'})
        vul_cols = ['latitude', 'longitude'] + [col for col in vul_df.columns if col.startswith('NDGAIN_') and 'NDGAIN_gain' not in col]
        vul_df_clean = vul_df[vul_cols].copy()
        print(f"  - Vulnerability子项数据已加载。")

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

    # --- 3. 【核心修改】分步执行空间邻近匹配 ---
    print("\n步骤 3: 正在分步执行空间邻近匹配...")
    
    # a) 匹配GAIN得分
    print("  - 正在匹配GAIN总分...")
    tree_gain = cKDTree(gain_df_clean[['latitude', 'longitude']].values)
    dist_gain, idx_gain = tree_gain.query(master_df[['latitude', 'longitude']].values, k=1)
    matched_gain_data = gain_df_clean.iloc[idx_gain]
    gain_data_cols = [col for col in matched_gain_data.columns if col not in ['latitude', 'longitude']]
    master_df[gain_data_cols] = matched_gain_data[gain_data_cols].values

    # b) 匹配Vulnerability子项
    print("  - 正在匹配Vulnerability子项...")
    tree_vul = cKDTree(vul_df_clean[['latitude', 'longitude']].values)
    dist_vul, idx_vul = tree_vul.query(master_df[['latitude', 'longitude']].values, k=1)
    matched_vul_data = vul_df_clean.iloc[idx_vul]
    vul_data_cols = [col for col in matched_vul_data.columns if col not in ['latitude', 'longitude']]
    master_df[vul_data_cols] = matched_vul_data[vul_data_cols].values
    
    print("  - 所有NDGAIN数据匹配和增补完成！")

    # --- 4. 保存最终结果 ---
    print("\n步骤 4: 正在保存已增补的最终结果...")
    output_csv_path = f'{output_prefix}_V4.csv'
    output_json_path = f'{output_prefix}_V4.json'

    master_df.to_csv(output_csv_path, index=False, float_format='%.4f')
    print(f"  - 已增补的CSV总表已保存至: {output_csv_path}")

    # 【核心修改】更新JSON生成逻辑以匹配新结构和命名要求
    timeseries_prefixes = ('era5_', 'cmip5_', 'gdp_', 'pop_', 'ccvi_', 'cli_', 'con_', 'NDGAIN_')
    static_columns = [col for col in master_df.columns if not col.startswith(timeseries_prefixes)]
    df_for_json = master_df.replace({np.nan: None})
    json_output_data = []

    for record in df_for_json.to_dict('records'):
        location_attributes = {key: record[key] for key in static_columns}
        timeseries_data = {
            "era5_temperature_celsius": {}, "cmip5_precipitation_mm_year": {},
            "ccvi_risk": {"ccvi_yearly_avg": {}, "cli_risk_yearly_avg": {}, "con_risk_yearly_avg": {}},
            "gdp": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}},
            "population": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}},
            # 新的、更清晰的NDGAIN结构
            "ndgain": {"ndgain_score": {}, "vulnerability_components": {}}
        }
        for col, value in record.items():
            if col in static_columns or value is None: continue
            
            if col.startswith('NDGAIN_'):
                parts = col.split('_')
                year = parts[-1]
                sub_variable = '_'.join(parts[1:-1]).lower()
                
                if sub_variable == 'gain':
                    # 满足您的命名要求
                    timeseries_data['ndgain']['ndgain_score'][year] = value
                else:
                    # 将所有子项放入新结构中
                    timeseries_data['ndgain']['vulnerability_components'][sub_variable] = timeseries_data['ndgain']['vulnerability_components'].get(sub_variable, {})
                    timeseries_data['ndgain']['vulnerability_components'][sub_variable][year] = value
            # ... 其他数据集的逻辑保持不变 ...
            else:
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
    MASTER_TABLE_PATH = './results/FINAL_MASTER_TABLE_V3.csv'
    GAIN_EXCEL_PATH = './Climate_NDGAIN_2015-2023/NDGAIN_gain_joined.xlsx'
    VULNERABILITY_EXCEL_PATH = './Climate_NDGAIN_2015-2023/NDGAIN_VulFull_joined.xlsx'
    FINAL_OUTPUT_PREFIX = './results/FINAL_MASTER_TABLE'

    final_enriched_dataframe = enrich_master_table_with_ndgain_final(MASTER_TABLE_PATH, GAIN_EXCEL_PATH, VULNERABILITY_EXCEL_PATH, FINAL_OUTPUT_PREFIX)

    if final_enriched_dataframe is not None:
        print("\n函数返回的最终增补DataFrame预览:")
        # 挑选几个关键列预览
        preview_cols = ['latitude', 'longitude'] + [col for col in final_enriched_dataframe.columns if 'NDGAIN_gain' in col]
        print(final_enriched_dataframe[preview_cols].head())


   

