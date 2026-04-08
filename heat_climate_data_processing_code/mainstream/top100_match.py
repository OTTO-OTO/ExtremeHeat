import pandas as pd
import numpy as np

def process_files():
    # 1. 文件路径配置 (请根据实际情况修改路径)
    file_clean = 'id_source_clean.csv'
    file_target = '03ft_g1_df250811_withsource.csv'
    
    # 输出文件名
    output_full = '03ft_g1_df250811_withsource_UPDATED.csv'
    output_subset = '03ft_g1_df250811_withsource_MATCHED_ONLY.csv'

    print("正在读取文件...")
    # 读取文件
    df_clean = pd.read_csv(file_clean)
    df_target = pd.read_csv(file_target)

    # 2. 校验行数是否一致 (安全检查)
    if len(df_clean) != len(df_target):
        print(f"警告：两个文件的行数不一致！")
        print(f"Clean文件: {len(df_clean)} 行")
        print(f"Target文件: {len(df_target)} 行")
        print("程序将继续执行，但基于行索引的匹配可能会错位，请务必检查数据源！")
    else:
        print(f"文件行数校验通过：共 {len(df_target)} 行")

    # 3. 定义目标域名列表 (白名单)
    # 使用 set 集合可以提高查询速度
    target_domains = {
        'people.com.cn',
        'washingtonpost.com',
        'apnews.com',
        'indiatimes.com',
        'corriere.it',
        'theguardian.com',
        'bbc.com',
        'people.cn'
    }

    print("正在匹配域名...")

    # 4. 核心处理逻辑
    # 提取 Clean 文件中的 source 列
    clean_source_col = df_clean['normalized_source']

    # 判断：如果该行的 source 在白名单中，保留原值；否则设为 NaN
    # np.where(条件, 条件为真时的值, 条件为假时的值)
    # 这里利用了行对齐，直接将计算结果赋值给 df_target
    df_target['is_top100'] = np.where(
        clean_source_col.isin(target_domains), # 条件
        clean_source_col,                      # 真：填入 normalized_source
        np.nan                                 # 假：填入 NaN
    )

    # 5. 统计信息
    matched_count = df_target['is_top100'].notna().sum()
    total_count = len(df_target)
    match_rate = (matched_count / total_count) * 100

    print("-" * 30)
    print("统计结果：")
    print(f"总行数: {total_count}")
    print(f"匹配成功行数: {matched_count}")
    print(f"匹配失败行数: {total_count - matched_count}")
    print(f"匹配率: {match_rate:.2f}%")
    print("-" * 30)

    # 6. 保存文件
    
    # (1) 保存包含所有行的新 Target 文件
    df_target.to_csv(output_full, index=False, encoding='utf-8-sig')
    print(f"全量文件已保存至: {output_full}")

    # (2) 保存只包含匹配成功行的子集文件
    df_subset = df_target[df_target['is_top100'].notna()]
    df_subset.to_csv(output_subset, index=False, encoding='utf-8-sig')
    print(f"匹配成功子集文件已保存至: {output_subset} (共 {len(df_subset)} 行)")

if __name__ == "__main__":
    process_files()