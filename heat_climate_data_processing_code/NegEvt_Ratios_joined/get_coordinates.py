import pandas as pd
import os
import time

def prepare_base_files(excel_path, output_coords_path, output_attributes_path):
    """
    读取单个Excel文件，清洗列名，并分别保存为纯坐标文件和完整的属性文件。
    此版本不执行任何去重操作。

    Args:
        excel_path (str): 源Excel文件的路径。
        output_coords_path (str): 最终生成的纯坐标CSV文件的输出路径。
        output_attributes_path (str): 最终生成的完整属性CSV文件的输出路径。
    """
    print(f"--- 开始处理文件: {os.path.basename(excel_path)} ---")
    start_time = time.time()
    
    try:
        # 1. 读取Excel文件
        df = pd.read_excel(excel_path)
        print(f"成功读取 {len(df)} 条记录。")

        # 2. 清洗列名
        if 'X' in df.columns and 'Y' in df.columns:
            print("检测到 'X', 'Y' 列，将重命名为 'longitude', 'latitude'。")
            df = df.rename(columns={'X': 'longitude', 'Y': 'latitude'})
        elif 'longitude' not in df.columns or 'latitude' not in df.columns:
            print("错误：在Excel文件中找不到经纬度列 ('X'/'Y' 或 'longitude'/'latitude')。")
            return

        # 确保输出文件夹存在
        output_dir_coords = os.path.dirname(output_coords_path)
        output_dir_attrs = os.path.dirname(output_attributes_path)
        if output_dir_coords and not os.path.exists(output_dir_coords):
            os.makedirs(output_dir_coords)
        if output_dir_attrs and not os.path.exists(output_dir_attrs):
            os.makedirs(output_dir_attrs)

        # --- 3. 【核心修改】分别生成两个输出文件 (无去重) ---
        
        # a) 提取并保存纯坐标文件 (用于重新查询)
        coords_only_df = df[['latitude', 'longitude']]
        coords_only_df.to_csv(output_coords_path, index=False)
        print(f"\n已成功将【纯净坐标】保存至: {output_coords_path}")
        print(f"  -> 这个文件包含 {len(coords_only_df)} 行，将用于您后续所有的数据提取脚本。")

        # b) 保存完整的属性文件 (用于最终合并)
        # 直接使用原始读取的DataFrame，不进行去重
        df.to_csv(output_attributes_path, index=False)
        print(f"\n已成功将【完整基础属性表】保存至: {output_attributes_path}")
        print(f"  -> 这个文件包含 {len(df)} 行，将在您完成所有数据提取后，用于最终的总表合并。")

        end_time = time.time()
        print(f"\n流程完毕！耗时: {end_time - start_time:.2f} 秒。")

    except FileNotFoundError:
        print(f"错误：找不到Excel文件 '{excel_path}'。请检查路径。")
    except Exception as e:
        print(f"处理过程中发生错误: {e}")

# --- 如何使用这个脚本 ---
if __name__ == '__main__':
    # --- 请在这里配置您的路径 ---
    # 1. 您同学给的【单个】新的Excel文件路径
    NEW_EXCEL_FILE = 'NegEvt_Ratios_POP_joined.xlsx' # 请替换为实际文件名
    
    # 2. 定义两个输出文件的路径
    # 文件一：只包含经纬度，用于重新运行提取脚本
    OUTPUT_COORDS_CSV_PATH = 'master_coordinates_1015.csv'
    # 文件二：包含所有信息，用于最终合并
    OUTPUT_ATTRIBUTES_CSV_PATH = 'master_attributes_1015.csv'
    # ---------------------------------

    prepare_base_files(NEW_EXCEL_FILE, OUTPUT_COORDS_CSV_PATH, OUTPUT_ATTRIBUTES_CSV_PATH)

