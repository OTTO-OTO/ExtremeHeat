import pandas as pd
import os

def extract_coordinates_from_excel(excel_path, output_csv_path):
    """
    从一个Excel文件中读取第一个Sheet，提取经纬度列，并保存为CSV。

    Args:
        excel_path (str): 源Excel文件的路径。
        output_csv_path (str): 目标CSV文件的输出路径。
    """
    print(f"开始从Excel文件读取: {excel_path}")
    
    try:
        # 读取Excel文件的第一个sheet
        df = pd.read_excel(excel_path)
        
        # 检查经纬度列名是'X', 'Y'还是'longitude', 'latitude'，并进行统一
        if 'X' in df.columns and 'Y' in df.columns:
            print("检测到 'X', 'Y' 列，将重命名为 'longitude', 'latitude'。")
            df = df.rename(columns={'X': 'longitude', 'Y': 'latitude'})
        elif 'longitude' not in df.columns or 'latitude' not in df.columns:
            print("错误：在Excel文件中找不到经纬度列 ('X'/'Y' 或 'longitude'/'latitude')。")
            return

        # 只保留我们需要的经纬度两列
        coords_df = df[['latitude', 'longitude']]
        print(f"成功提取 {len(coords_df)} 个坐标点。")

        # 确保输出文件夹存在
        output_dir = os.path.dirname(output_csv_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # 保存为CSV，不包含索引列
        coords_df.to_csv(output_csv_path, index=False)
        print(f"已成功将经纬度坐标保存至: {output_csv_path}")
        
        print("\n文件预览 (前5行):")
        print(coords_df.head())

    except FileNotFoundError:
        print(f"错误：找不到Excel文件 '{excel_path}'。请检查路径。")
    except Exception as e:
        print(f"处理过程中发生错误: {e}")

# --- 如何使用这个函数 ---
if __name__ == '__main__':
    # --- 请在这里配置您的路径 ---
    # 1. 您同学给您的新的Excel文件路径
    NEW_EXCEL_FILE_PATH = 'Ratio_country_joined_result.xlsx' # 请替换为实际文件名
    
    # 2. 您希望生成的、只包含经纬度的CSV文件的存放路径
    OUTPUT_COORDS_CSV_PATH = 'new_coordinates.csv'
    # ---------------------------------

    extract_coordinates_from_excel(NEW_EXCEL_FILE_PATH, OUTPUT_COORDS_CSV_PATH)