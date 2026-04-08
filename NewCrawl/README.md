<<<<<<< HEAD
# 多国家新闻爬虫项目

## 项目概述

本项目是一个多国家新闻爬虫系统，使用Python的feapder库实现对全球多个国家新闻网站的自动化爬取。项目旨在收集和分析各国新闻，特别是与特定关键词相关的新闻内容。

## 目录结构

```
├── 20250113/           # 按日期组织的爬虫文件
├── 20250114/
├── 20250118/
├── ...
├── feapder_spider/     # 主要爬虫代码
│   ├── venv/           # 虚拟环境
│   ├── feapder_China.py
│   ├── feapder_France_coral.py
│   ├── feapder_UK_theguardian.py
│   └── setting.py
├── importance_spider/   # 重要国家爬虫
│   ├── venv/           # 虚拟环境
│   ├── NewsItems.py    # 新闻数据模型
│   ├── feapder_Ecuador_lahora.py
│   ├── feapder_Ecuador_eltelegrafo.py
│   └── setting.py
├── other_country_spider_jiqun/  # 其他国家爬虫集群
├── NewsItems.py        # 通用新闻数据模型
└── README.md           # 项目说明文档
```

## 环境搭建

### 1. 安装Python

本项目需要Python 3.8或更高版本。请从[Python官网](https://www.python.org/downloads/)下载并安装。

### 2. 创建虚拟环境

```bash
# 在项目根目录创建虚拟环境
python -m venv venv

# 激活虚拟环境
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate
```

### 3. 安装依赖

```bash
pip install feapder
pip install requests
pip install lxml
pip install pymysql
```

## 使用方法

### 1. 配置数据库

修改 `setting.py` 文件中的数据库配置：

```python
# 数据库配置
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = "your_password"
DB_NAME = "news_db"
```

### 2. 运行爬虫

```bash
# 运行特定国家的爬虫
python feapder_spider/feapder_China.py

# 运行重要国家爬虫
python importance_spider/feapder_Ecuador_lahora.py
```

## 爬虫说明

### 1. 爬虫类型

- **feapder_spider/**: 主要国家的爬虫，使用feapder库实现
- **importance_spider/**: 重要国家的爬虫，包含更详细的配置和功能
- **other_country_spider_jiqun/**: 其他国家的爬虫集群

### 2. 爬虫功能

- 自动爬取指定国家的新闻网站
- 支持关键词过滤和搜索
- 多线程爬取，提高效率
- 反爬虫机制，避免被网站封锁
- 数据存储到MySQL数据库

### 3. 关键词配置

在爬虫文件中，您可以修改关键词列表来控制爬取的新闻内容：

```python
# 西班牙语关键词
keywords = ["Extremo", "Calor", "Alta", "Temperatura", "Lluvia", "Pesada", "Sequía", "Poder", "Corte", "de", "Electricidad", "debido", "al", "calor", "Incendio", "Contaminación", "del", "aire", "Cambio", "climático", "Reducción", "de", "los", "rendimientos", "agrícolas", "Hipoxia", "Ataque", "de", "calor", "Impacto", "del", "calor", "en", "el", "tráfico", "Desastre", "ecológico", "Impacto", "del", "cambio", "climático", "en", "la", "economía", "Ola", "de", "calor", "marina", "Contaminación", "relacionada", "con", "el", "calor", "Coral"]
```

## 数据库配置

### 1. 创建数据库

```sql
CREATE DATABASE news_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### 2. 创建表结构

爬虫会自动创建所需的表结构，包括：
- `news_items`: 存储新闻基本信息
- `news_content`: 存储新闻详细内容

## 注意事项

1. **反爬虫措施**：爬虫已实现基本的反爬虫机制，包括随机User-Agent、动态cookies和代理设置，但仍可能被网站封锁。

2. **频率控制**：建议不要过于频繁地运行爬虫，以免给目标网站带来过大压力。

3. **数据存储**：爬虫会将数据存储到MySQL数据库，请确保数据库服务正常运行。

4. **代理设置**：如果需要使用代理，请修改爬虫文件中的代理配置。

## 常见问题

### 1. 爬虫运行时报错

- **403错误**：可能是被网站封锁，请尝试更换代理或降低爬取频率。
- **数据库连接错误**：请检查数据库配置是否正确，数据库服务是否正常运行。

### 2. 爬取的新闻内容不完整

- 可能是目标网站的HTML结构发生变化，请检查并更新XPath选择器。

### 3. 爬虫运行速度慢

- 可以尝试增加线程数，修改爬虫文件中的`thread_count`参数。

## 联系方式

如果您在使用过程中遇到问题，请联系项目维护人员。

---

**免责声明**：本项目仅用于学习和研究目的，请勿用于任何商业或非法用途。使用本项目爬取数据时，请遵守目标网站的robots.txt规则和相关法律法规。
=======
# ExtremeHeat
>>>>>>> 82d8c6b549c0baf1cdd4484e42adb648bc26d5bf
