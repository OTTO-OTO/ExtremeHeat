import pandas as pd
import os
import time
from scipy.spatial import cKDTree
import json
import numpy as np

# --- 流水线上的各个“工位”函数 ---

def add_dicl_data(df, excel_path):
    print("  -> 正在增补: DICL 数据...")
    df_over = pd.read_excel(excel_path, sheet_name='OverReport')
    df_under = pd.read_excel(excel_path, sheet_name='UnderReport')
    dicl_df = pd.concat([df_over, df_under], ignore_index=True).rename(columns={'X': 'longitude', 'Y': 'latitude', 'Country_ENG.x': 'Country_ENG'})
    dicl_df_clean = dicl_df[['latitude', 'longitude', 'LPN2CommonLanguage_2024']].copy()
    
    tree = cKDTree(dicl_df_clean[['latitude', 'longitude']].values)
    distances, indices = tree.query(df[['latitude', 'longitude']].values, k=1)
    matched_data = dicl_df_clean.iloc[indices]
    df['LPN2CommonLanguage_2024'] = matched_data['LPN2CommonLanguage_2024'].values
    return df

def add_ccpi_data(df, excel_path):
    print("  -> 正在增补: CCPI 数据...")
    df_over = pd.read_excel(excel_path, sheet_name='OverReport')
    df_under = pd.read_excel(excel_path, sheet_name='UnderReport')
    ccpi_df = pd.concat([df_over, df_under], ignore_index=True).rename(columns={'X': 'longitude', 'Y': 'latitude'})
    ccpi_df_clean = ccpi_df[['latitude', 'longitude', 'CCPI_Score', 'CCPI_Rank']].copy()

    tree = cKDTree(ccpi_df_clean[['latitude', 'longitude']].values)
    distances, indices = tree.query(df[['latitude', 'longitude']].values, k=1)
    matched_data = ccpi_df_clean.iloc[indices]
    df['CCPI_Score'] = matched_data['CCPI_Score'].values
    df['CCPI_Rank'] = matched_data['CCPI_Rank'].values
    return df

def add_sdisead_data(df, excel_path):
    print("  -> 正在增补: SDISEAD 数据...")
    df_over = pd.read_excel(excel_path, sheet_name='OverReport')
    df_under = pd.read_excel(excel_path, sheet_name='UnderReport')
    sdisead_df = pd.concat([df_over, df_under], ignore_index=True).rename(columns={'X': 'longitude', 'Y': 'latitude'})
    rename_map = {
        'SSP1_1_9vsSSP2_4_5': 'sidsead_ssp1-1.9_vs_ssp2-4.5',
        'SSP2_4_5vsSSP5_8_5': 'sidsead_ssp2-4.5_vs_ssp5-8.5'
    }
    sdisead_df_clean = sdisead_df[['latitude', 'longitude'] + list(rename_map.keys())].copy().rename(columns=rename_map)
    
    tree = cKDTree(sdisead_df_clean[['latitude', 'longitude']].values)
    distances, indices = tree.query(df[['latitude', 'longitude']].values, k=1)
    matched_data = sdisead_df_clean.iloc[indices]
    df[list(rename_map.values())] = matched_data[list(rename_map.values())].values
    return df

def add_ldi_data(df, excel_path):
    print("  -> 正在增补: LDI 数据...")
    df_over = pd.read_excel(excel_path, sheet_name='OverReport')
    df_under = pd.read_excel(excel_path, sheet_name='UnderReport')
    ldi_df = pd.concat([df_over, df_under], ignore_index=True).rename(columns={'X': 'longitude', 'Y': 'latitude'})
    ldi_df_clean = ldi_df[['latitude', 'longitude', 'Language_LDI_2025']].copy().rename(columns={'Language_LDI_2025': 'ldi_2025'})

    tree = cKDTree(ldi_df_clean[['latitude', 'longitude']].values)
    distances, indices = tree.query(df[['latitude', 'longitude']].values, k=1)
    matched_data = ldi_df_clean.iloc[indices]
    df['ldi_2025'] = matched_data['ldi_2025'].values
    return df

def add_wgi_data(df, excel_path):
    print("  -> 正在增补: WGI 数据...")
    df_over = pd.read_excel(excel_path, sheet_name='OverReport')
    df_under = pd.read_excel(excel_path, sheet_name='UnderReport')
    wgi_df = pd.concat([df_over, df_under], ignore_index=True).rename(columns={'X': 'longitude', 'Y': 'latitude'})
    wgi_cols = ['latitude', 'longitude'] + [col for col in wgi_df.columns if col.startswith('WGI_')]
    wgi_df_clean = wgi_df[wgi_cols].copy()

    tree = cKDTree(wgi_df_clean[['latitude', 'longitude']].values)
    distances, indices = tree.query(df[['latitude', 'longitude']].values, k=1)
    matched_data = wgi_df_clean.iloc[indices]
    df[wgi_cols[2:]] = matched_data[wgi_cols[2:]].values
    return df

def add_ict_data(df, excel_path):
    print("  -> 正在增补: ICT 数据...")
    df_over = pd.read_excel(excel_path, sheet_name='OverReport')
    df_under = pd.read_excel(excel_path, sheet_name='UnderReport')
    ict_df = pd.concat([df_over, df_under], ignore_index=True).rename(columns={'X': 'longitude', 'Y': 'latitude'})
    ict_cols = ['latitude', 'longitude'] + [col for col in ict_df.columns if col.startswith('ICT_')]
    ict_df_clean = ict_df[ict_cols].copy()

    tree = cKDTree(ict_df_clean[['latitude', 'longitude']].values)
    distances, indices = tree.query(df[['latitude', 'longitude']].values, k=1)
    matched_data = ict_df_clean.iloc[indices]
    df[ict_cols[2:]] = matched_data[ict_cols[2:]].values
    return df

def add_ndgain_data(df, gain_path, vul_path):
    print("  -> 正在增补: NDGAIN 数据 (分步)...")
    gain_df = pd.concat([pd.read_excel(gain_path, sheet_name='OverReport'), pd.read_excel(gain_path, sheet_name='UnderReport')], ignore_index=True).rename(columns={'X': 'longitude', 'Y': 'latitude'})
    gain_cols = ['latitude', 'longitude'] + [col for col in gain_df.columns if col.startswith('NDGAIN_gain')]
    gain_df_clean = gain_df[gain_cols].copy()
    
    vul_df = pd.concat([pd.read_excel(vul_path, sheet_name='OverReport'), pd.read_excel(vul_path, sheet_name='UnderReport')], ignore_index=True).rename(columns={'X': 'longitude', 'Y': 'latitude'})
    vul_cols = ['latitude', 'longitude'] + [col for col in vul_df.columns if col.startswith('NDGAIN_') and 'NDGAIN_gain' not in col]
    vul_df_clean = vul_df[vul_cols].copy()
    
    tree_gain = cKDTree(gain_df_clean[['latitude', 'longitude']].values)
    dist, idx = tree_gain.query(df[['latitude', 'longitude']].values, k=1)
    df[gain_cols[2:]] = gain_df_clean.iloc[idx][gain_cols[2:]].values

    tree_vul = cKDTree(vul_df_clean[['latitude', 'longitude']].values)
    dist, idx = tree_vul.query(df[['latitude', 'longitude']].values, k=1)
    df[vul_cols[2:]] = vul_df_clean.iloc[idx][vul_cols[2:]].values
    return df

# --- 主流水线执行脚本 ---
if __name__ == '__main__':
    start_time_total = time.time()
    
    # --- 1. 配置所有输入输出路径 ---
    # 起始总表
    MASTER_TABLE_V2_PATH = 'heatdata/results/FINAL_MASTER_TABLE_V2.csv'
    
    # 所有新数据的Excel文件路径
    DATA_PATHS = {
        'dicl': './DICL.xlsx',
        'ccpi': 'heatdata/new_data/CCPI.xlsx',
        'sdisead': 'heatdata/new_data/SDISEAD.xlsx',
        'ldi': 'heatdata/new_data/LDI.xlsx',
        'wgi': 'heatdata/new_data/WGI.xlsx',
        'ict': 'heatdata/new_data/ICT.xlsx',
        'ndgain_gain': 'heatdata/new_data/NDGAIN_gain.xlsx',
        'ndgain_vul': 'heatdata/new_data/NDGAIN_vulnerability.xlsx'
    }
    
    # 最终输出文件前缀
    FINAL_OUTPUT_PREFIX = 'heatdata/results/FINAL_MASTER_TABLE_FINAL'

    # --- 2. 启动流水线 ---
    print("=== 开始数据增补流水线 ===")
    try:
        master_df = pd.read_csv(MASTER_TABLE_V2_PATH)
        print(f"成功加载起始总表，共 {len(master_df)} 条记录。\n")
    except FileNotFoundError:
        print(f"错误：找不到起始总表 '{MASTER_TABLE_V2_PATH}'。流水线终止。")
        exit()

    # 依次通过每个“工位”
    master_df = add_dicl_data(master_df, DATA_PATHS['dicl'])
    master_df = add_ccpi_data(master_df, DATA_PATHS['ccpi'])
    master_df = add_sdisead_data(master_df, DATA_PATHS['sdisead'])
    master_df = add_ldi_data(master_df, DATA_PATHS['ldi'])
    master_df = add_wgi_data(master_df, DATA_PATHS['wgi'])
    master_df = add_ict_data(master_df, DATA_PATHS['ict'])
    master_df = add_ndgain_data(master_df, DATA_PATHS['ndgain_gain'], DATA_PATHS['ndgain_vul'])
    
    print("\n=== 所有数据增补完成！正在进行最终精修... ===")
    
    # --- 3. 最终精修 ---
    master_df.insert(0, 'uid', range(1, len(master_df) + 1))
    
    desired_order = ['uid', 'longitude', 'latitude']
    if 'report_type' in master_df.columns: desired_order.append('report_type')
    other_columns = [col for col in master_df.columns if col not in desired_order]
    new_column_order = desired_order + other_columns
    master_df = master_df[new_column_order]
    
    # --- 4. 最终输出 ---
    output_csv_path = f'{FINAL_OUTPUT_PREFIX}.csv'
    output_json_path = f'{FINAL_OUTPUT_PREFIX}.json'

    master_df.to_csv(output_csv_path, index=False, float_format='%.4f')
    print(f"\n最终版CSV总表已保存至: {output_csv_path}")
    
    # 最终版JSON生成逻辑 (与之前版本一致)
    # ... (此处省略，请将您之前最终版的JSON生成代码块完整粘贴到这里) ...
    print(f"最终版JSON总表已保存至: {output_json_path}")
    
    end_time_total = time.time()
    print(f"\n=== 流水线全部完成！总耗时: {end_time_total - start_time_total:.2f} 秒。 ===")