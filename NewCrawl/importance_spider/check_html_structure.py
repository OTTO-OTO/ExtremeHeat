import urllib.request
import ssl
import os

# 禁用环境变量中的代理设置
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''

# 禁用SSL验证
context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE

# 测试新闻URL
news_url = "https://www.atb.com.bo/2026/02/21/mousse-de-limon-frio-para-refrescarte-en-epoca-de-calor/"

try:
    # 创建请求
    req = urllib.request.Request(news_url)
    req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36')
    
    # 发送请求
    with urllib.request.urlopen(req, context=context, timeout=30) as response:
        status_code = response.getcode()
        print(f"连接成功！状态码: {status_code}")
        
        # 读取响应内容
        content = response.read().decode('utf-8', errors='ignore')
        
        # 保存HTML内容到文件
        with open('news_page.html', 'w', encoding='utf-8') as f:
            f.write(content)
        print("HTML内容已保存到 news_page.html")
        
        # 查找标题相关的HTML结构
        print("\n查找标题相关的HTML结构:")
        # 查找所有h1标签
        import re
        h1_tags = re.findall(r'<h1[^>]*>(.*?)</h1>', content, re.DOTALL)
        print(f"找到 {len(h1_tags)} 个h1标签:")
        for i, h1 in enumerate(h1_tags, 1):
            print(f"{i}. {h1.strip()}")
        
        # 查找所有h2标签
        h2_tags = re.findall(r'<h2[^>]*>(.*?)</h2>', content, re.DOTALL)
        print(f"\n找到 {len(h2_tags)} 个h2标签:")
        for i, h2 in enumerate(h2_tags[:3], 1):  # 只显示前3个
            print(f"{i}. {h2.strip()}")
        
        # 查找entry-content标签
        print("\n查找entry-content标签:")
        entry_content = re.findall(r'<div class="entry-content">(.*?)</div>', content, re.DOTALL)
        if entry_content:
            print(f"找到 entry-content 标签，长度: {len(entry_content[0])} 字符")
            print(f"内容预览: {entry_content[0][:200]}...")
        
        # 查找title标签
        print("\n查找title标签:")
        title_tag = re.findall(r'<title>(.*?)</title>', content, re.DOTALL)
        if title_tag:
            print(f"找到 title 标签: {title_tag[0].strip()}")
            
except Exception as e:
    print(f"错误: {str(e)}")
