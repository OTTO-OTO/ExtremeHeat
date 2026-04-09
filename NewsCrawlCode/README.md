# News Crawler Project

## Project Introduction

This is a global news crawler project implemented using Python and the feapder library. It can crawl news data from news websites around the world. The project supports news sources from multiple countries and regions, providing a unified data structure for subsequent data analysis and processing.

## Technology Stack

- **Programming Language**: Python 3.8+
- **Crawler Framework**: feapder
- **Data Storage**: Supports multiple storage methods (depending on actual configuration)
- **Data Structure**: Unified news data model

## Project Structure

The project adopts a country-classified crawler script structure, with each country corresponding to one or more crawler scripts. The main files include:

- `NewsItems.py`: Defines the unified news data model
- `[Country]_[NewsSource].py`: News crawler scripts for each country
- `feapder_[Country]_[NewsSource].py`: Crawler scripts implemented using the feapder framework

## Data Model

The news data model is defined in the `NewsItems.py` file and includes the following fields:

| Field Name | Type | Description |
|------------|------|-------------|
| title | str | News title |
| author | str | Author |
| keyword | str | Keywords |
| content | str | News content |
| article_url | str | Article URL |
| pubtime | str | Publication time |
| country | str | Country |

## Installation and Configuration

### 1. Environment Preparation

```bash
# Create and activate virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
# or
# source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install feapder
```

### 2. Configuration Instructions

- **Crawler Configuration**: You can adjust crawling parameters in each crawler script, such as crawling frequency, concurrency, etc.
- **Storage Configuration**: Configure data storage methods according to actual needs, such as database, file, etc.

## Usage

### Run a Single Crawler

```bash
# Run a specific country's crawler
python feapder_China_cctv.py

# Or run a non-feapder version crawler
python China_cctv.py
```

### Batch Run Crawlers

You can create a batch script to run multiple crawlers:

```python
# batch_run.py
import os
import subprocess

# List all crawler scripts
spider_scripts = [f for f in os.listdir('.') if f.endswith('.py') and ('feapder_' in f or '_' in f and not f.startswith('__'))]

# Run each crawler
for script in spider_scripts:
    print(f"Running {script}...")
    subprocess.run(['python', script])
```

## Crawler Instructions

### Crawler Types

The project contains two types of crawler scripts:

1. **Standard Crawler Scripts**: Such as `China_cctv.py`, implemented using basic Python crawler libraries
2. **Feapder Crawler Scripts**: Such as `feapder_China_cctv.py`, implemented using the feapder framework, with better performance and reliability

### Anti-crawling Mechanisms

The project implements multiple anti-crawling strategies:

- Random User-Agent
- Reasonable request intervals
- Automatic captcha handling (for some websites)
- Distributed crawling support

### Supported Countries and Regions

The project supports news sources from multiple countries and regions around the world, including but not limited to:

- China
- United States
- United Kingdom
- Japan
- South Korea
- Russia
- France
- Germany
- India
- Brazil
- And many other countries...

## Data Storage

### Database Interface

The project supports multiple database storage methods. Here are recommended industrial-grade database configurations:

#### MySQL Configuration Example

```python
# Add database configuration in crawler script
from feapder import Item, Insert

class SpiderDataItem(Item):
    # Data model definition...
    
    def save(self):
        # Database insertion operation
        return Insert(
            table="news",
            data={
                "title": self.title,
                "author": self.author,
                "keyword": self.keyword,
                "content": self.content,
                "article_url": self.article_url,
                "pubtime": self.pubtime,
                "country": self.country
            }
        )
```

#### MongoDB Configuration Example

```python
# Add MongoDB configuration in crawler script
from pymongo import MongoClient

class SpiderDataItem(Item):
    # Data model definition...
    
    def save(self):
        # MongoDB insertion operation
        client = MongoClient('mongodb://localhost:27017/')
        db = client['news_db']
        collection = db['news']
        collection.insert_one({
            "title": self.title,
            "author": self.author,
            "keyword": self.keyword,
            "content": self.content,
            "article_url": self.article_url,
            "pubtime": self.pubtime,
            "country": self.country
        })
```

## Performance Optimization

1. **Concurrent Crawling**: Use feapder's concurrency mechanism to improve crawling efficiency
2. **Distributed Deployment**: Support multiple servers crawling simultaneously to further improve efficiency
3. **Data Deduplication**: Implement data deduplication through the `__unique_key__` attribute to avoid duplicate data
4. **Breakpoint Resumption**: Support breakpoint resumption to avoid repeated crawling due to network issues

## Maintenance and Expansion

### Add New Crawlers

1. Copy existing crawler scripts and modify them for new news sources
2. Adjust crawler rules to adapt to the new website structure
3. Test the crawler to ensure it can normally obtain data
4. Add to the batch running script

### Common Issues

1. **Crawler Blocked**: Adjust request intervals, use proxy IPs
2. **Data Acquisition Failure**: Check if the website structure has changed, update crawler rules
3. **Storage Failure**: Check database connection, ensure correct permissions

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Contribution

Welcome to submit Issues and Pull Requests to improve the project together.

## Contact

If you have any questions or suggestions, please contact the project maintainers.