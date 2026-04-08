import pandas as pd
import os
import time
from scipy.spatial import cKDTree
import json
import numpy as np
import rasterio # 确保 rasterio 已导入

def enrich_master_table_with_hist_gdp(master_table_path, new_tif_path, output_prefix):
    """
    使用多波段历史GDP GeoTIFF数据源来丰富和更新已有的总表。
    """
    start_time = time.time()
    dataset_prefix = 'gdp_hist' # 为新数据集定义一个清晰的前缀

    # --- 1. 加载总表 ---
    try:
        print(f"步骤 1: 正在加载当前的总表: {master_table_path}")
        master_df = pd.read_csv(master_table_path)
        print("  - 总表加载成功。")
    except FileNotFoundError:
        print(f"错误：找不到总表文件 '{master_table_path}'。")
        return None

    # --- 2. 一次性采样所有波段 ---
    try:
        print(f"\n步骤 2: 正在从GeoTIFF文件 '{new_tif_path}' 提取多波段数据...")
        
        # 准备坐标列表
        coords_list = list(zip(master_df['longitude'], master_df['latitude']))
        
        with rasterio.open(new_tif_path) as src:
            # 提取波段描述以确定年份
            band_descriptions = src.descriptions
            years = [desc.split('_')[-1] for desc in band_descriptions]
            print(f"  - 文件包含 {len(years)} 个年份的数据: {', '.join(years)}")

            # 一次性采样所有波段
            # sample() 返回一个生成器，每个元素是一个包含所有波段值的numpy数组
            sampled_values = list(src.sample(coords_list))
            
            # 将采样结果转换为一个DataFrame，每列代表一个波段
            hist_gdp_df = pd.DataFrame(np.vstack(sampled_values), columns=years)

        print("  - 数据提取完成。")

    except Exception as e:
        print(f"处理GeoTIFF文件时发生错误: {e}")
        return None

    # --- 3. 增补数据到总表 ---
    print("\n步骤 3: 正在将新数据增补到总表中...")
    
    # 为新列添加前缀
    new_column_names = {year: f"{dataset_prefix}_{year}" for year in years}
    hist_gdp_df = hist_gdp_df.rename(columns=new_column_names)
    
    # 将新数据列合并到总表中 (因为顺序一致，可以直接拼接)
    final_df = pd.concat([master_df, hist_gdp_df], axis=1)
    print("  - 增补完成！")
    
    # --- 4. 最终精修与保存 ---
    print("\n步骤 4: 正在保存最终版本的结果...")
    
    output_csv_path = f'{output_prefix}_V3_FINAL.csv'
    output_json_path = f'{output_prefix}_V3_FINAL.json'

    # a) 保存为新的CSV
    final_df.to_csv(output_csv_path, index=False, float_format='%.4f')
    print(f"  - 最终版CSV总表已保存至: {output_csv_path}")

    # b) 更新JSON文件
    timeseries_prefixes = ('era5_', 'cmip5_', 'gdp_', 'pop_', 'ccvi_', 'cli_', 'con_', 'worldpop_', 'gdp_hist_') # 新增前缀
    static_columns = [col for col in final_df.columns if not col.startswith(timeseries_prefixes)]
    df_for_json = final_df.replace({np.nan: None})
    json_output_data = []

    for record in df_for_json.to_dict('records'):
        location_attributes = {key: record[key] for key in static_columns}
        timeseries_data = {
            "era5_temperature_celsius": {}, "cmip5_precipitation_mm_year": {},
            "ccvi_risk": {"ccvi_yearly_avg": {}, "cli_risk_yearly_avg": {}, "con_risk_yearly_avg": {}},
            "gdp_ssp": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}},
            "population_ssp": {"ssp1": {}, "ssp2": {}, "ssp3": {}, "ssp4": {}, "ssp5": {}},
            "worldpop_population": {},
            "gdp_historical": {} # 新增历史GDP主键
        }
        for col, value in record.items():
            if col in static_columns or value is None: continue
            
            parts = col.split('_'); prefix = parts[0]
            
            if prefix == 'gdp' and parts[1] == 'hist': # 新增的历史GDP逻辑
                year = parts[2]
                timeseries_data['gdp_historical'][year] = float(value)
            elif prefix == 'era5':
                timeseries_data["era5_temperature_celsius"][parts[2]] = float(value)
            elif prefix == 'cmip5':
                timeseries_data["cmip5_precipitation_mm_year"][parts[2]] = float(value)
            elif prefix in ['ccvi', 'cli', 'con']:
                year = parts[-1]; var_name = '_'.join(parts[:-1]); dict_key = f'{var_name}_yearly_avg'
                if dict_key in timeseries_data["ccvi_risk"]: timeseries_data["ccvi_risk"][dict_key][year] = float(value)
            elif prefix == 'gdp':
                ssp = parts[1]; year = parts[2]; timeseries_data['gdp_ssp'][ssp][year] = float(value)
            elif prefix == 'pop':
                ssp = parts[1]; year = parts[2]; timeseries_data['population_ssp'][ssp][year] = float(value)
            elif prefix == 'worldpop':
                year = parts[1]
                timeseries_data['worldpop_population'][year] = float(value)
        
        json_output_data.append({'location_attributes': location_attributes, 'timeseries_data': timeseries_data})
        
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(json_output_data, f, ensure_ascii=False, indent=4)
    print(f"  - 最终版JSON总表已保存至: {output_json_path}")

    print("-" * 40)
    end_time = time.time()
    print(f"数据增补流程完毕！总耗时: {end_time - start_time:.2f} 秒。")

    return final_df

# --- 如何使用这个函数 ---
if __name__ == '__main__':
    # --- 请在这里配置您的路径 ---
    # 1. 我们上一轮生成的总表路径
    MASTER_TABLE_PATH = '/Users/zhengye/ye/2024Autumn/heatevent/heatdata/results/FINAL_MASTER_TABLE.csv'
    
    # 2. 新的历史GDP GeoTIFF文件路径
    NEW_TIF_PATH = 'rast_gdpTot_1990_2020_30arcsec.tif'
    
    # 3. 新总表的输出文件名前缀
    FINAL_OUTPUT_PREFIX = '/Users/zhengye/ye/2024Autumn/heatevent/heatdata/results/FINAL_MASTER_TABLE'
    # ---------------------------------

    final_enriched_dataframe = enrich_master_table_with_hist_gdp(MASTER_TABLE_PATH, NEW_TIF_PATH, FINAL_OUTPUT_PREFIX)

    if final_enriched_dataframe is not None:
        print("\n函数返回的最终增补DataFrame预览:")
        # 挑选几个关键列预览
        preview_cols = ['latitude', 'longitude', 'Country_ENG', 'gdp_hist_1990', 'gdp_hist_2020']
        print(final_enriched_dataframe[preview_cols].head())