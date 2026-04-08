import xarray as xr
import pandas as pd
import json
import time

def process_era5_data(coordinates_csv_path, era5_netcdf_path, output_prefix):
    """
    处理ERA5 NetCDF文件，为给定的坐标点提取年度平均气온。
    最终修正版，修复了在使用带属性的坐标文件时，生成JSON的错误。
    """
    start_time = time.time()

    # --- 1. 定义配置 ---
    dataset_prefix = 'era5'
    variable_name = 't2m'
    unit_name = 'C'

    output_csv_path = f'{output_prefix}_{dataset_prefix}_{variable_name}_yearly_wide.csv'
    output_json_path = f'{output_prefix}_{dataset_prefix}_{variable_name}_yearly.json'

    # --- 2. 加载坐标文件 ---
    try:
        # 【注意】这里加载的是我们新的、包含所有静态属性的坐标文件
        coords_df = pd.read_csv(coordinates_csv_path)
        coords_df.columns = coords_df.columns.str.strip()
        print(f"成功从 '{coordinates_csv_path}' 加载 {len(coords_df)} 个坐标点。")
        print("-" * 40)
    except FileNotFoundError:
        print(f"错误：找不到坐标文件 '{coordinates_csv_path}'。")
        return None

    # --- 3. 矢量化提取与处理 ---
    try:
        with xr.open_dataset(era5_netcdf_path) as ds:
            print(f"成功打开ERA5文件: {era5_netcdf_path}")

            lon_adjusted = coords_df['longitude'].apply(lambda lon: lon if lon >= 0 else lon + 360)
            
            xr_lats = xr.DataArray(coords_df['latitude'], dims="points")
            xr_lons = xr.DataArray(lon_adjusted, dims="points")
            
            print("正在执行矢量化查询...")
            data_for_all_points = ds[variable_name].sel(
                latitude=xr_lats,
                longitude=xr_lons,
                method='nearest'
            )
            
            print("数据提取完成！正在进行单位转换...")
            data_celsius = data_for_all_points - 273.15
            
            long_df = data_celsius.to_dataframe().reset_index()
            
            print("正在重塑数据以生成输出文件...")
            long_df['year'] = long_df['time'].dt.year
            # 使用pivot_table重塑，并直接将原始的coords_df合并进来
            final_df_wide = long_df.pivot_table(
                index='points', 
                columns='year', 
                values=variable_name
            ).reset_index(drop=True)

            final_df_wide = pd.concat([coords_df, final_df_wide], axis=1)

            final_df_wide.columns = [
                col if isinstance(col, str) else f'{dataset_prefix}_{variable_name}_{col}_{unit_name}' 
                for col in final_df_wide.columns
            ]

            final_df_wide.to_csv(output_csv_path, index=False, float_format='%.4f')
            print(f"宽格式年度CSV文件已保存至: {output_csv_path}")

            # --- c) 【核心修正】生成并保存JSON文件 ---
            json_output_data = []
            # 获取所有静态列的列表，以便在循环中跳过它们
            static_columns = coords_df.columns.tolist()

            for record in final_df_wide.to_dict('records'):
                lat = record.get('latitude')
                lon = record.get('longitude')
                
                yearly_data = {}
                # 遍历一行中的所有列
                for key, value in record.items():
                    # 如果是静态列，或者值为空，则跳过
                    if key in static_columns or pd.isna(value):
                        continue
                    
                    # 只处理符合我们时间序列格式的列
                    parts = key.split('_')
                    if len(parts) == 4 and parts[0] == dataset_prefix:
                        year = parts[2]
                        yearly_data[year] = float(round(value, 4))

                json_output_data.append({
                    'latitude': float(lat),
                    'longitude': float(lon),
                    f'{dataset_prefix}_{variable_name}_celsius': yearly_data
                })

            with open(output_json_path, 'w', encoding='utf-8') as f:
                json.dump(json_output_data, f, ensure_ascii=False, indent=4)
            print(f"结构化年度JSON文件已保存至: {output_json_path}")
            
            print("-" * 40)
            end_time = time.time()
            print(f"所有任务处理完毕！总耗时: {end_time - start_time:.2f} 秒。")
            
            return final_df_wide

    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        return None

# --- 如何使用这个函数 ---
if __name__ == '__main__':
    # 【注意】确保这里的输入是我们新的、合并后的坐标文件
    COORDS_PATH = '/Users/zhengye/ye/2024Autumn/heatevent/heatdata/master_coordinates_1015.csv'
    ERA5_NC_PATH = 't2m_mean_yearly_2015_2024_025degree_global.nc'
    OUTPUT_FILE_PREFIX = 'results'

    era5_final_dataframe = process_era5_data(COORDS_PATH, ERA5_NC_PATH, OUTPUT_FILE_PREFIX)

    if era5_final_dataframe is not None:
        print("\n函数返回的ERA5 DataFrame结果预览:")
        print(era5_final_dataframe.head())