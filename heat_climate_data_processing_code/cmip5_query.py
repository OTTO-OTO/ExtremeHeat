import xarray as xr
import pandas as pd
import json
import glob
import time

def process_cmip5_data(coordinates_csv_path, cmip5_files_glob, output_prefix):
    """
    处理多个CMIP5 NetCDF文件，为给定的坐标点提取年度总降水量。
    输出CSV、JSON文件，并返回一个DataFrame。

    Args:
        coordinates_csv_path (str): 包含经纬度坐标的CSV文件路径。
        cmip5_files_glob (str): 匹配所有CMIP5 NetCDF文件的glob模式。
        output_prefix (str): 输出文件的前缀（例如 'results'）。

    Returns:
        pandas.DataFrame: 包含所有坐标点和其对应年度总降水量的DataFrame。
    """
    start_time = time.time()

    # --- 1. 定义配置和文件路径 ---
    dataset_prefix = 'cmip5'
    variable_name = 'prAdjust'
    unit_name = 'mm_year'

    file_list = sorted(glob.glob(cmip5_files_glob))
    if not file_list:
        print(f"错误：在路径 '{cmip5_files_glob}' 下没有找到任何文件。")
        return None
    print(f"找到 {len(file_list)} 个待处理的CMIP5文件。")

    output_csv_path = f'{output_prefix}_{dataset_prefix}_{variable_name}_yearly_wide.csv'
    output_json_path = f'{output_prefix}_{dataset_prefix}_{variable_name}_yearly.json'
    

    SECONDS_PER_DAY = 86400.0

    # --- 2. 加载坐标文件 ---
    try:
        coords_df = pd.read_csv(coordinates_csv_path)
        coords_df.columns = coords_df.columns.str.strip()
        print(f"成功从 '{coordinates_csv_path}' 加载 {len(coords_df)} 个坐标点。")
        print("-" * 40)
    except FileNotFoundError:
        print(f"错误：找不到坐标文件 '{coordinates_csv_path}'。")
        return None

    # --- 3. 矢量化提取与处理 ---
    try:
        with xr.open_mfdataset(file_list, combine='nested', concat_dim='time', parallel=True) as ds:
            print("所有NetCDF文件已成功合并为一个虚拟数据集。")
            
            xr_lats = xr.DataArray(coords_df['latitude'], dims="points")
            xr_lons = xr.DataArray(coords_df['longitude'], dims="points")
            
            print("正在执行矢量化查询...")
            data_for_all_points = ds[variable_name].sel(
                lat=xr_lats,
                lon=xr_lons,
                method='nearest'
            )
            
            print("数据提取完成！正在进行单位转换和年度聚合...")
            data_mm_day = data_for_all_points * SECONDS_PER_DAY
            long_df = data_mm_day.to_dataframe(name='pr_mm_day').reset_index()
            long_df['time'] = pd.to_datetime(long_df['time'])
            yearly_long_df = long_df.groupby(['lat', 'lon', long_df['time'].dt.year])['pr_mm_day'].sum().reset_index()
            yearly_long_df = yearly_long_df.rename(columns={'time': 'year', 'pr_mm_day': f'{dataset_prefix}_{variable_name}_{unit_name}'})
            
            # --- 4. 数据重塑与生成输出 ---
            print("聚合完成！正在重塑数据以生成输出文件...")
            
            final_df_wide = yearly_long_df.pivot_table(
                index=['lat', 'lon'], 
                columns='year', 
                values=f'{dataset_prefix}_{variable_name}_{unit_name}'
            ).reset_index()
            
            final_df_wide.columns = [
                col if isinstance(col, str) else f'{dataset_prefix}_{variable_name}_{col}_{unit_name}' 
                for col in final_df_wide.columns
            ]
            final_df_wide = final_df_wide.rename(columns={'lat': 'latitude', 'lon': 'longitude'})

            # a) 保存CSV文件
            final_df_wide.to_csv(output_csv_path, index=False, float_format='%.2f')
            print(f"宽格式年度CSV文件已保存至: {output_csv_path}")

            # b) 生成并保存JSON文件
            json_output_data = []
            for record in final_df_wide.to_dict('records'):
                lat = record.pop('latitude')
                lon = record.pop('longitude')
                yearly_data = {k.split('_')[2]: v for k, v in record.items()}
                json_output_data.append({
                    'latitude': lat,
                    'longitude': lon,
                    f'{dataset_prefix}_{variable_name}_{unit_name}': yearly_data
                })
            with open(output_json_path, 'w', encoding='utf-8') as f:
                json.dump(json_output_data, f, ensure_ascii=False, indent=4)
            print(f"结构化年度JSON文件已保存至: {output_json_path}")
            
           
            
            print("-" * 40)
            end_time = time.time()
            print(f"所有任务处理完毕！总耗时: {end_time - start_time:.2f} 秒。")
            
            # c) 返回最终的DataFrame
            return final_df_wide

    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        return None

# --- 如何使用这个函数 ---
if __name__ == '__main__':
    COORDS_PATH = '/Users/zhengye/ye/2024Autumn/heatevent/heatdata/master_coordinates_1015.csv'
    CMIP5_GLOB_PATTERN = 'prAdjust_*.nc'
    OUTPUT_FILE_PREFIX = 'results'

    final_dataframe = process_cmip5_data(COORDS_PATH, CMIP5_GLOB_PATTERN, OUTPUT_FILE_PREFIX)

    if final_dataframe is not None:
        print("\n函数返回的DataFrame结果预览:")
        print(final_dataframe.head())