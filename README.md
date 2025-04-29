# Reddit Content Aggregator

A powerful tool that monitors Reddit for valuable content from specific subreddits and keywords, aggregating posts and comments for analysis, inspiration, or market research.

## 🔍 Problem Solved

Content creators, marketers, and researchers need to stay on top of trending discussions and valuable content across Reddit's vast ecosystem. Manually monitoring multiple subreddits is time-consuming and inefficient. This tool automatically aggregates relevant content based on your criteria, saving hours of manual work.

## ✨ Features

- Monitor multiple subreddits simultaneously
- Filter content by keywords, upvote thresholds, and post age
- Extract top comments and discussions
- Identify trending topics within your niche
- Export data to CSV, JSON, or Markdown formats
- Generate summaries of key discussions
- Schedule regular scans for continuous monitoring

## 🛠️ Technologies Used

- Python 3.8+
- PRAW (Python Reddit API Wrapper)
- Pandas for data structuring and analysis
- NLTK for text analysis and topic extraction
- SQLite for data storage

## 📋 Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Set up your Reddit API credentials
cp reddit_config.example.json reddit_config.json
# Edit reddit_config.json with your API credentials

# Single subreddit scan
python scan_subreddit.py --subreddit "datascience" --top 25 --time "week"

# Multiple subreddit scan with keyword filtering
python scan_multiple.py --subreddits "marketing,socialmedia,digitalmarketing" --keywords "instagram,tiktok,strategy" --min-upvotes 50

# Export results to markdown files
python export_results.py --format markdown --output-dir "./content"
```
## 📊 Sample Output
The tool generates structured content collections that include:

Post title, URL, author, and upvote count  
Post content with formatting preserved  
Top comments and discussion threads  
Extracted key topics and sentiment analysis  
Related subreddits and posts  

## 📝 License
MIT
