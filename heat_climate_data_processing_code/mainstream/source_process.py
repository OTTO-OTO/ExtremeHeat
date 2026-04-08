import pandas as pd
import tldextract
import sys

# --- 请修改这里的配置 ---
# 输入文件：你上次生成的 "id 和新闻来源" 文件
INPUT_ID_SOURCE_FILE = 'id_source.csv' 

# 输出文件 1: 一个新的、清理过的 ID-域名 映射表
OUTPUT_CLEANED_ID_SOURCE_FILE = 'id_source_clean.csv'

# 输出文件 2: 包含统计结果的文件
OUTPUT_STATS_FILE = 'media_source_stats.csv'
# -------------------------

def normalize_domain(source):
    """
    使用 tldextract 将子域名规范化为主域名。
    例如:
    - 'finance.people.com.cn' -> 'people.com.cn'
    - 'www.nytimes.com'       -> 'nytimes.com'
    - 'bbc.co.uk'             -> 'bbc.co.uk'
    """
    try:
        if pd.isna(source):
            return None
        
        # tldextract 会返回 (subdomain, domain, suffix)
        # 我们需要 'domain' 和 'suffix'
        extracted = tldextract.extract(str(source))
        
        # .domain 和 .suffix 不能为空
        if not extracted.domain or not extracted.suffix:
            return None # 无法解析
            
        return f"{extracted.domain}.{extracted.suffix}"
        
    except Exception as e:
        print(f"处理 '{source}' 时出错: {e}")
        return None

def main():
    print(f"--- 1. 开始读取文件: {INPUT_ID_SOURCE_FILE} ---")
    
    try:
        # 你上次生成的 CSV，假设它有 'id' 和 'news_source' 两列
        df = pd.read_csv(INPUT_ID_SOURCE_FILE)
    except FileNotFoundError:
        print(f"错误：找不到文件 '{INPUT_ID_SOURCE_FILE}'。请检查文件名是否正确。")
        sys.exit(1)
    except Exception as e:
        print(f"读取 CSV 时发生错误: {e}")
        sys.exit(1)

    # 检查 'news_source' 列是否存在
    if 'news_source' not in df.columns:
        print(f"错误：文件 '{INPUT_ID_SOURCE_FILE}' 中未找到 'news_source' 列。")
        print(f"找到的列是: {list(df.columns)}")
        sys.exit(1)

    print("--- 2. 正在规范化域名 (例如 'www.abc.com' -> 'abc.com')... ---")
    
    # 对 'news_source' 列中的每一个值应用 normalize_domain 函数
    # 将结果存储在一个名为 'normalized_source' 的新列中
    df['normalized_source'] = df['news_source'].apply(normalize_domain)

    print("--- 3. 规范化完成。---")

    # --- 保存清理后的 ID-Source 映射表 ---
    try:
        if 'id' in df.columns:
            # 创建一个新的 DataFrame，只包含 id 和清理后的域名
            df_cleaned = df[['id', 'normalized_source']]
            
            print(f"\n--- 4. 正在保存清理后的 ID-域名 映射表到: {OUTPUT_CLEANED_ID_SOURCE_FILE} ---")
            df_cleaned.to_csv(OUTPUT_CLEANED_ID_SOURCE_FILE, index=False, encoding='utf-8-sig')
            print("--- 5. 保存成功。 ---")
        else:
            print("\n--- 4. 警告: 未找到 'id' 列，将跳过保存 'id-域名' 映射表。 ---")
            df_cleaned = df # 后续统计将直接使用 df
            
    except Exception as e:
        print(f"保存 {OUTPUT_CLEANED_ID_SOURCE_FILE} 时出错: {e}")


    # --- 开始统计 ---
    print("\n--- 6. 正在统计已规范化的媒体来源... ---")
    
    # 对新生成的 'normalized_source' 列进行计数
    # .value_counts() 会自动统计每个唯一值出现了多少次
    stats = df_cleaned['normalized_source'].value_counts()
    
    # 获取独立媒体的总数
    total_unique_media = len(stats)
    
    print("\n================ 统计结果 ================")
    print(f"总共发现 {total_unique_media} 个独立媒体来源。")
    print("\n--- 频次最高的前 50 个媒体来源：---")
    
    # 打印前 50 名
    print(stats.head(50))
    print("============================================")

    # --- 保存统计结果到文件 ---
    try:
        print(f"\n--- 7. 正在将完整统计结果保存到: {OUTPUT_STATS_FILE} ---")
        
        # 将 stats (这是一个 Series) 转换成 DataFrame 以便保存
        stats_df = stats.reset_index()
        # 重命名列
        stats_df.columns = ['normalized_source', 'record_count']
        
        # 保存
        stats_df.to_csv(OUTPUT_STATS_FILE, index=False, encoding='utf-8-sig')
        
        print("--- 8. 统计文件保存成功！---")
        print("\n--- 所有处理已完成！ ---")
        
    except Exception as e:
        print(f"保存 {OUTPUT_STATS_FILE} 时出错: {e}")

if __name__ == "__main__":
    main()