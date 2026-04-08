import pandas as pd
import os
import time
from scipy.spatial import cKDTree
import json
import numpy as np

def enrich_master_table_with_sdisead_final(master_table_path, new_excel_path, output_prefix):
    """
    使用SDISEAD数据源来丰富和更新已有的总表，
    并在最后一步添加uid并重排列，生成最终交付版本。
    """
    start_time = time.time()

    # --- 1. 准备和清洗新数据 (SDISEAD) ---
    # ... (此部分与您提供的脚本完全相同，为简洁省略) ...
    try:
        print(f"步骤 1: 正在从Excel文件 '{new_excel_path}' 加载并清洗SDISEAD数据...")
        df_over = pd.read_excel(new_excel_path, sheet_name='OverReport')
        df_under = pd.read_excel(new_excel_path, sheet_name='UnderReport')
        sdisead_df = pd.concat([df_over, df_under], ignore_index=True)
        sdisead_df = sdisead_df.rename(columns={'X': 'longitude', 'Y': 'latitude'})
        rename_map = {
            'SSP1_1_9vsSSP2_4_5': 'sidsead_ssp1-1.9_vs_ssp2-4.5',
            'SSP2_4_5vsSSP5_8_5': 'sidsead_ssp2-4.5_vs_ssp5-8.5'
        }
        original_data_cols = list(rename_map.keys())
        new_prefixed_cols = list(rename_map.values())
        columns_to_keep = ['latitude', 'longitude'] + original_data_cols
        sdisead_df_clean = sdisead_df[columns_to_keep].copy()
        sdisead_df_clean = sdisead_df_clean.rename(columns=rename_map)
        print(f"  - 已提取并重命名新数据列为: {new_prefixed_cols}")
    except Exception as e:
        print(f"处理新数据Excel文件时发生错误: {e}")
        return None

    # --- 2. 加载总表 ---
    # ... (此部分与您提供的脚本完全相同) ...
    try:
        print(f"\n步骤 2: 正在加载当前的总表: {master_table_path}")
        master_df = pd.read_csv(master_table_path)
        print("  - 总表加载成功。")
    except FileNotFoundError:
        print(f"错误：找不到总表文件 '{master_table_path}'。")
        return None

    # --- 3. 执行空间邻近匹配 ---
    # ... (此部分与您提供的脚本完全相同) ...
    print("\n步骤 3: 正在执行空间邻近匹配...")
    tree = cKDTree(sdisead_df_clean[['latitude', 'longitude']].values)
    distances, indices = tree.query(master_df[['latitude', 'longitude']].values, k=1)
    matched_data = sdisead_df_clean.iloc[indices]
    master_df[new_prefixed_cols] = matched_data[new_prefixed_cols].values
    print("  - 匹配和增补完成！")
    
    # --- 4. 【核心修改】最终精修：添加UID和重排序列 ---
    print("\n步骤 4: 正在进行最终精修（添加uid，重排序列）...")
    
    # a) 添加uid (从0开始的整数)
    master_df.insert(0, 'uid', range(1, len(master_df) + 1))
    print("  - 已添加uid列。")
    
    # b) 重排序列
    desired_order = ['uid', 'longitude', 'latitude', 'report_type']
    other_columns = [col for col in master_df.columns if col not in desired_order]
    new_column_order = desired_order + other_columns
    master_df = master_df[new_column_order]
    print("  - 已按要求重排关键列。")

    # --- 5. 保存最终结果 ---
    print("\n步骤 5: 正在保存最终的交付版本结果...")
    
    output_csv_path = f'{output_prefix}_V5.csv'
    output_json_path = f'{output_prefix}_V5.json'

    # a) 保存为新的CSV
    master_df.to_csv(output_csv_path, index=False, float_format='%.4f')
    print(f"  - 最终版CSV总表已保存至: {output_csv_path}")

    # b) 更新JSON文件
    # ... (此部分与您提供的脚本完全相同，它会自动处理新的列顺序和uid) ...
    timeseries_prefixes = ('era5_', 'cmip5_', 'gdp_', 'pop_', 'ccvi_', 'cli_', 'con_', 'NDGAIN_')
    # 我们需要从新的列顺序中动态识别静态列
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
            "ndgain": {"ndgain_score": {}, "vulnerability_components": {}}
        }
        for col, value in record.items():
            if col in static_columns or value is None: continue
            if col.startswith('NDGAIN_'):
                parts = col.split('_'); year = parts[-1]; sub_variable = '_'.join(parts[1:-1]).lower()
                if sub_variable == 'gain':
                    timeseries_data['ndgain']['ndgain_score'][year] = value
                else:
                    if sub_variable not in timeseries_data['ndgain']['vulnerability_components']:
                        timeseries_data['ndgain']['vulnerability_components'][sub_variable] = {}
                    timeseries_data['ndgain']['vulnerability_components'][sub_variable][year] = value
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
    MASTER_TABLE_PATH = './results/FINAL_MASTER_TABLE_V4.csv'
    NEW_DATA_EXCEL_PATH = './SIDSEAD_joined.xlsx'
    FINAL_OUTPUT_PREFIX = './results/FINAL_MASTER_TABLE'

    final_dataframe = enrich_master_table_with_sdisead_final(MASTER_TABLE_PATH, NEW_DATA_EXCEL_PATH, FINAL_OUTPUT_PREFIX)
    
    if final_dataframe is not None:
        print("\n函数返回的最终DataFrame母表预览 (已添加uid并重排):")
        print(final_dataframe.head())

