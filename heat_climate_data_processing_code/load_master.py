import pandas as pd
import json
import time

def load_master_json_to_dataframe(json_path):
    """
    读取我们项目生成的最终版嵌套JSON文件，并将其转换为一个扁平的、
    适合分析的Pandas DataFrame。

    Args:
        json_path (str): FINAL_MASTER_TABLE.json 文件的路径。

    Returns:
        pandas.DataFrame: 包含所有数据的扁平化DataFrame。
    """
    print(f"正在从 '{json_path}' 加载数据...")
    start_time = time.time()

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"错误：找不到JSON文件 '{json_path}'。请检查路径。")
        return None
    except Exception as e:
        print(f"读取或解析JSON文件时发生错误: {e}")
        return None

    flattened_records = []
    
    # 遍历JSON中的每一个地点记录
    for location_data in data:
        # 初始化一个扁平的行记录，首先填入所有静态属性
        flat_record = location_data['location_attributes'].copy()
        
        # 提取时间序列数据
        ts_data = location_data['timeseries_data']
        
        # 1. 处理ERA5
        for year, value in ts_data.get('era5_temperature_celsius', {}).items():
            flat_record[f'era5_t2m_{year}_C'] = value
            
        # 2. 处理CMIP5
        for year, value in ts_data.get('cmip5_precipitation_mm_year', {}).items():
            flat_record[f'cmip5_prAdjust_{year}_mm_year'] = value
            
        # 3. 处理CCVI
        for risk_type, yearly_values in ts_data.get('ccvi_risk', {}).items():
            var_name = risk_type.replace('_yearly_avg', '')
            for year, value in yearly_values.items():
                flat_record[f'{var_name}_{year}'] = value

        # 4. 处理GDP
        for ssp, yearly_values in ts_data.get('gdp', {}).items():
            for year, value in yearly_values.items():
                flat_record[f'gdp_{ssp}_{year}'] = value
        
        # 5. 处理人口
        for ssp, yearly_values in ts_data.get('population', {}).items():
            for year, value in yearly_values.items():
                flat_record[f'pop_{ssp}_{year}'] = value
        
        # 6. 处理NDGAIN
        ndgain_data = ts_data.get('ndgain', {})
        for year, value in ndgain_data.get('ndgain_score', {}).items():
            flat_record[f'NDGAIN_gain_{year}'] = value
        for component, yearly_values in ndgain_data.get('vulnerability_components', {}).items():
            # 将 'vul' -> 'NDGAIN_Vul', 'vulwater' -> 'NDGAIN_VulWater'
            col_name_part = ''.join([part.capitalize() for part in component.split('_')])
            for year, value in yearly_values.items():
                flat_record[f'NDGAIN_{col_name_part}_{year}'] = value
        
        flattened_records.append(flat_record)
        
    # 从扁平化的记录列表创建最终的DataFrame
    final_df = pd.DataFrame(flattened_records)
    
    end_time = time.time()
    print(f"数据加载和转换完成！耗时: {end_time - start_time:.2f} 秒。")
    
    return final_df

# --- 如何使用这个函数 ---
if __name__ == '__main__':
    # 下游用户只需要修改这个路径即可
    JSON_FILE_PATH = './results/FINAL_MASTER_TABLE_V5.json'
    
    # 调用函数加载数据
    master_df = load_master_json_to_dataframe(JSON_FILE_PATH)
    
    if master_df is not None:
        print("\n成功将JSON转换为DataFrame！预览如下:")
        print(master_df.head())
        print(f"\nDataFrame 结构信息:")
        master_df.info()