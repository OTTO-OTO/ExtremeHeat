import pandas as pd
from urllib.parse import urlparse
import sys

# --- 请修改这里的配置 ---
INPUT_CSV_FILE = '03ft_g1_df250811.csv'  # 替换成你的 CSV 文件路径
OUTPUT_CSV_FILE = '03ft_g1_df250811_post.csv'  # 这是处理后保存的文件名
ID_SOURCE_OUTPUT_FILE = 'id_source.csv'  # 这是 *只包含id和来源* 的新文件名
# -------------------------

def get_news_source(url):
    """
    从一个 URL 中提取其网络位置 (netloc)，即域名。
    """
    try:
        if pd.isna(url):
            return None
        parsed_url = urlparse(str(url))
        return parsed_url.netloc
    except Exception as e:
        print(f"处理 URL '{url}' 时出错: {e}")
        return None

def main():
    print(f"--- 1. 开始读取文件: {INPUT_CSV_FILE} ---")
    
    try:
        df = pd.read_csv(INPUT_CSV_FILE)
    except FileNotFoundError:
        print(f"错误：找不到文件 '{INPUT_CSV_FILE}'。请检查文件名和路径是否正确。")
        sys.exit(1)
    except Exception as e:
        print(f"读取 CSV 时发生错误: {e}")
        sys.exit(1)
        
    print("--- 2. 文件读取成功 ---")

    # --- 功能 1: 检查 'id' 缺失 ---
    print("\n--- 3. 检查 'id' 列的缺失情况 ---")
    
    if 'id' not in df.columns:
        print("警告：CSV 文件中未找到 'id' 列。无法执行 'id' 缺失检查和 'id_和新闻来源.csv' 的生成。")
        id_column_exists = False
    else:
        id_column_exists = True
        missing_id_mask = df['id'].isna()
        missing_id_count = missing_id_mask.sum()

        if missing_id_count == 0:
            print("真棒！ 'id' 列中没有发现缺失值。")
        else:
            print(f"警告：在 'id' 列中发现了 {missing_id_count} 个缺失值。")
            missing_indices = df[missing_id_mask].index.tolist()
            missing_lines = [i + 2 for i in missing_indices] # 索引+表头+从1开始
            
            if len(missing_lines) > 20:
                print(f"这些记录大约在文件的行号: {missing_lines[:20]}... (仅显示前20个)")
            else:
                print(f"这些记录大约在文件的行号: {missing_lines}")
    
    # --- 功能 2: 提取新闻来源 ---
    print("\n--- 4. 检查 'article_url' 列是否存在 ---")
    
    if 'article_url' not in df.columns:
        print("错误：CSV 文件中未找到 'article_url' 列。")
        print(f"找到的列是: {list(df.columns)}")
        print("无法继续提取新闻来源，脚本将退出。")
        sys.exit(1)

    print("--- 5. 正在从 'article_url' 提取新闻来源 (域名)... ---")
    df['news_source'] = df['article_url'].apply(get_news_source)

    print("--- 6. 提取完成，显示部分结果：---")
    print(df[['article_url', 'news_source']].head())
    
    print("\n--- 7. 统计新闻来源 (前 20 名)：---")
    print(df['news_source'].value_counts().head(20))

    # --- 新功能: 单独保存 'id' 和 'news_source' ---
    print(f"\n--- 8. 正在生成仅包含 'id' 和 'news_source' 的文件... ---")
    
    if not id_column_exists:
        print(f"警告：'id' 列不存在，无法生成 {ID_SOURCE_OUTPUT_FILE}。跳过此步骤。")
    else:
        try:
            # 选择 'id' 和 'news_source' 这两列
            id_source_df = df[['id', 'news_source']]
            
            # 保存到新的 CSV 文件
            id_source_df.to_csv(ID_SOURCE_OUTPUT_FILE, index=False, encoding='utf-8-sig')
            print(f"--- 9. 成功保存到: {ID_SOURCE_OUTPUT_FILE} ---")
        except Exception as e:
            print(f"保存 {ID_SOURCE_OUTPUT_FILE} 时出错: {e}")
    # --- 新功能结束 ---

    # --- 保存完整文件 ---
    try:
        print(f"\n--- 10. 正在将 *完整* 结果保存到: {OUTPUT_CSV_FILE} ---")
        df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')
        print(f"--- 11. 成功保存到: {OUTPUT_CSV_FILE} ---")
    except Exception as e:
        print(f"保存 {OUTPUT_CSV_FILE} 时出错: {e}")

    print("\n--- 所有处理已完成！ ---")

if __name__ == "__main__":
    main()