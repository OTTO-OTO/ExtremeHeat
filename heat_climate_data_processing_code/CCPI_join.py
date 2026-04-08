import pandas as pd
import os
import time
from scipy.spatial import cKDTree
import json
import numpy as np

def enrich_master_table_with_ccpi(master_table_path, new_excel_path, output_prefix):
    """
    使用CCPI数据源来丰富和更新已有的总表。
    """
    start_time = time.time()

    # --- 1. 准备和清洗新数据 (CCPI) ---
    try:
        print(f"步骤 1: 正在从Excel文件 '{new_excel_path}' 加载并清洗CCPI数据...")
        
        # a) 读取两个sheets
        df_over = pd.read_excel(new_excel_path, sheet_name='OverReport')
        df_under = pd.read_excel(new_excel_path, sheet_name='UnderReport')
        
        # b) 合并为一个
        ccpi_df = pd.concat([df_over, df_under], ignore_index=True)
        print(f"  - 已合并两个Sheet，共 {len(ccpi_df)} 条记录。")

        # c) 清洗列名
        ccpi_df = ccpi_df.rename(columns={'X': 'longitude', 'Y': 'latitude'})
        
        # d) 【核心修改】精简数据，只保留“钥匙”和“新信息”
        columns_to_keep = ['latitude', 'longitude', 'CCPI_Score', 'CCPI_Rank']
        ccpi_df_clean = ccpi_df[columns_to_keep].copy()
        print("  - 已提取所需列: ", columns_to_keep)
        
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
    
    tree = cKDTree(ccpi_df_clean[['latitude', 'longitude']].values)
    distances, indices = tree.query(master_df[['latitude', 'longitude']].values, k=1)
    
    matched_data = ccpi_df_clean.iloc[indices]
    
    # 【核心修改】将新的数据列添加到总表中
    master_df['CCPI_Score'] = matched_data['CCPI_Score'].values
    master_df['CCPI_Rank'] = matched_data['CCPI_Rank'].values
    print("  - 匹配和增补完成！")

    # --- 4. 保存最终结果 ---
    print("\n步骤 4: 正在保存已增补的最终结果...")
    
    output_csv_path = f'{output_prefix}_V3.csv'
    output_json_path = f'{output_prefix}_V3.json'

    # a) 保存为新的CSV
    master_df.to_csv(output_csv_path, index=False, float_format='%.4f')
    print(f"  - 已增补的CSV总表已保存至: {output_csv_path}")

    # b) 更新JSON文件
    # ... (此处的JSON更新逻辑与上一版完全相同，因为它会自动识别新加入的静态列) ...
    timeseries_prefixes = ('era5_', 'cmip5_', 'gdp_', 'pop_', 'ccvi_', 'cli_', 'con_')
    static_columns = [col for col in master_df.columns if not col.startswith(timeseries_prefixes)]
    df_for_json = master_df.replace({np.nan: None})
    json_output_data = []
    for record in df_for_json.to_dict('records'):
        location_attributes = {key: record[key] for key in static_columns}
        timeseries_data = {
            "era5_temperature_celsius": {}, "cmip5_precipitation_mm_year": {},
            "ccvi_risk": {"ccvi_yearly_avg": {}, "cli_risk_yearly_avg": {}, "con_risk_yearly_avg": {}},
            "gdp": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}},
            "population": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}}
        }
        for col, value in record.items():
            if col in static_columns or value is None: continue
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
    # --- 请在这里配置您的路径 ---
    # 1. 我们上一轮生成的总表路径
    MASTER_TABLE_PATH = './results/FINAL_MASTER_TABLE_V2.csv'
    
    # 2. 新的CCPI Excel文件路径
    NEW_DATA_EXCEL_PATH = './Climate_CCPI_2025/CCPI_joined.xlsx' # 请替换为实际文件名
    
    # 3. 新总表的输出文件名前缀
    FINAL_OUTPUT_PREFIX = './results/FINAL_MASTER_TABLE'
    # ---------------------------------

    final_enriched_dataframe = enrich_master_table_with_ccpi(MASTER_TABLE_PATH, NEW_DATA_EXCEL_PATH, FINAL_OUTPUT_PREFIX)

    if final_enriched_dataframe is not None:
        print("\n函数返回的最终增补DataFrame预览:")
        # 打印时显示新加入的列
        key_cols = ['latitude', 'longitude', 'Country_ENG', 'CCPI_Score', 'CCPI_Rank']
        print(final_enriched_dataframe[key_cols].head())