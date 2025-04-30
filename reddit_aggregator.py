import praw
import pandas as pd
import json
import argparse
import os
import re
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from datetime import datetime, timedelta
import sqlite3
import time
from collections import Counter

class RedditContentAggregator:
    def __init__(self, config_path='reddit_config.json'):
        """Initialize the Reddit content aggregator"""
        self.load_config(config_path)
        self.setup_reddit_api()
        self.setup_database()
        self.setup_nltk()
        
    def load_config(self, config_path):
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            print(f"Config file {config_path} not found.")
            raise
            
    def setup_reddit_api(self):
        """Set up connection to Reddit API"""
        self.reddit = praw.Reddit(
            client_id=self.config['client_id'],
            client_secret=self.config['client_secret'],
            user_agent=self.config['user_agent'],
            username=self.config.get('username', ''),
            password=self.config.get('password', '')
        )
        print("Connected to Reddit API")
        
    def setup_database(self, db_path='reddit_content.db'):
        """Set up SQLite database for storing content"""
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # Create posts table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            title TEXT,
            author TEXT,
            subreddit TEXT,
            url TEXT,
            selftext TEXT,
            upvote_ratio REAL,
            score INTEGER,
            created_utc INTEGER,
            num_comments INTEGER,
            collected_at TEXT
        )
        ''')
        
        # Create comments table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            id TEXT PRIMARY KEY,
            post_id TEXT,
            author TEXT,
            body TEXT,
            score INTEGER,
            created_utc INTEGER,
            FOREIGN KEY (post_id) REFERENCES posts (id)
        )
        ''')
        
        self.conn.commit()
        
    def setup_nltk(self):
        """Download necessary NLTK resources"""
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            nltk.download('punkt')
            
        try:
            nltk.data.find('sentiment/vader_lexicon.zip')
        except LookupError:
            nltk.download('vader_lexicon')
        
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        
    def scan_subreddit(self, subreddit_name, limit=25, time_filter='week', 
                      min_upvotes=10, keywords=None):
        """
        Scan a subreddit for top content
        
        Args:
            subreddit_name (str): Name of the subreddit to scan
            limit (int): Maximum number of posts to retrieve
            time_filter (str): One of 'hour', 'day', 'week', 'month', 'year', 'all'
            min_upvotes (int): Minimum number of upvotes for a post to be included
            keywords (list): List of keywords to filter posts (optional)
            
        Returns:
            list: List of post dictionaries that match criteria
        """
        print(f"Scanning r/{subreddit_name} for top posts in the last {time_filter}...")
        
        subreddit = self.reddit.subreddit(subreddit_name)
        posts = []
        
        # Get top posts from this time period
        for post in subreddit.top(time_filter=time_filter, limit=limit):
            # Check if post meets upvote threshold
            if post.score < min_upvotes:
                continue
                
            # Check if post contains any of the keywords (if specified)
            if keywords:
                keyword_matches = any(
                    keyword.lower() in post.title.lower() or 
                    (post.selftext and keyword.lower() in post.selftext.lower())
                    for keyword in keywords
                )
                if not keyword_matches:
                    continue
            
            # Create post dictionary with basic info
            post_data = {
                'id': post.id,
                'title': post.title,
                'author': str(post.author),
                'subreddit': post.subreddit.display_name,
                'url': post.url,
                'selftext': post.selftext,
                'upvote_ratio': post.upvote_ratio,
                'score': post.score,
                'created_utc': post.created_utc,
                'num_comments': post.num_comments,
                'collected_at': datetime.now().isoformat()
            }
            
            # Add post to database
            self.add_post_to_db(post_data)
            
            # Get top comments for this post
            post_data['top_comments'] = self.get_top_comments(post, limit=10)
            
            posts.append(post_data)
            
            # Add a small delay to avoid rate limiting
            time.sleep(0.5)
            
        print(f"Found {len(posts)} posts matching criteria in r/{subreddit_name}")
        return posts
        
    def add_post_to_db(self, post_data):
        """Add post to database"""
        try:
            self.cursor.execute('''
            INSERT OR REPLACE INTO posts (
                id, title, author, subreddit, url, selftext, 
                upvote_ratio, score, created_utc, num_comments, collected_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                post_data['id'], post_data['title'], post_data['author'],
                post_data['subreddit'], post_data['url'], post_data['selftext'],
                post_data['upvote_ratio'], post_data['score'], 
                post_data['created_utc'], post_data['num_comments'],
                post_data['collected_at']
            ))
            self.conn.commit()
        except Exception as e:
            print(f"Error adding post to database: {e}")
    
    def get_top_comments(self, post, limit=10):
        """Get top comments for a post"""
        post.comment_sort = 'top'
        post.comments.replace_more(limit=0)  # Skip loading "more comments" links
        
        comments = []
        for comment in post.comments[:limit]:
            comment_data = {
                'id': comment.id,
                'author': str(comment.author),
                'body': comment.body,
                'score': comment.score,
                'created_utc': comment.created_utc
            }
            
            # Add sentiment analysis
            sentiment = self.sentiment_analyzer.polarity_scores(comment.body)
            comment_data['sentiment'] = sentiment
            
            # Add comment to database
            self.add_comment_to_db(post.id, comment_data)
            
            comments.append(comment_data)
            
        return comments
    
    def add_comment_to_db(self, post_id, comment_data):
        """Add comment to database"""
        try:
            self.cursor.execute('''
            INSERT OR REPLACE INTO comments (
                id, post_id, author, body, score, created_utc
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                comment_data['id'], post_id, comment_data['author'],
                comment_data['body'], comment_data['score'], 
                comment_data['created_utc']
            ))
            self.conn.commit()
        except Exception as e:
            print(f"Error adding comment to database: {e}")
            
    def scan_multiple_subreddits(self, subreddits, **kwargs):
        """
        Scan multiple subreddits and aggregate results
        
        Args:
            subreddits (list): List of subreddit names to scan
            **kwargs: Additional arguments to pass to scan_subreddit
            
        Returns:
            dict: Dictionary with subreddit names as keys and posts as values
        """
        results = {}
        
        for subreddit in subreddits:
            try:
                posts = self.scan_subreddit(subreddit, **kwargs)
                results[subreddit] = posts
            except Exception as e:
                print(f"Error scanning r/{subreddit}: {e}")
                
        return results
    
    def extract_trending_topics(self, posts, num_topics=5):
        """Extract trending topics from a collection of posts"""
        # Combine all text from titles and post content
        all_text = " ".join([
            p['title'] + " " + p['selftext']
            for p in posts if p['selftext']
        ])
        
        # Tokenize and remove common words
        tokens = nltk.word_tokenize(all_text.lower())
        tokens = [t for t in tokens if t.isalpha() and len(t) > 3]
        
        # Count frequency of words
        word_counts = Counter(tokens)
        
        # Return most common words
        return word_counts.most_common(num_topics)
    
    def export_to_markdown(self, posts, output_dir="./content"):
        """Export posts to markdown files"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        for post in posts:
            # Create filename based on post title
            filename = re.sub(r'[^\w\s-]', '', post['title'])
            filename = re.sub(r'[\s-]+', '-', filename).strip('-')
            filename = f"{filename[:50]}-{post['id']}.md"
            filepath = os.path.join(output_dir, filename)
            
            # Format created date
            created_date = datetime.fromtimestamp(post['created_utc'])
            
            with open(filepath, 'w', encoding='utf-8') as f:
                # Write post header
                f.write(f"# {post['title']}\n\n")
                f.write(f"**Author:** u/{post['author']}  \n")
                f.write(f"**Subreddit:** r/{post['subreddit']}  \n")
                f.write(f"**Date:** {created_date.strftime('%Y-%m-%d %H:%M:%S')}  \n")
                f.write(f"**Score:** {post['score']} (upvote ratio: {post['upvote_ratio']})  \n")
                f.write(f"**URL:** {post['url']}  \n\n")
                
                # Write post content
                f.write("## Content\n\n")
                if post['selftext']:
                    f.write(f"{post['selftext']}\n\n")
                else:
                    f.write("*[Link post - no text content]*\n\n")
                
                # Write top comments
                f.write("## Top Comments\n\n")
                for i, comment in enumerate(post['top_comments'], 1):
                    comment_date = datetime.fromtimestamp(comment['created_utc'])
                    sentiment = comment['sentiment']['compound']
                    sentiment_label = (
                        "Positive" if sentiment > 0.05 else
                        "Negative" if sentiment < -0.05 else
                        "Neutral"
                    )
                    
                    f.write(f"### Comment {i} by u/{comment['author']}\n")
                    f.write(f"**Score:** {comment['score']}  \n")
                    f.write(f"**Date:** {comment_date.strftime('%Y-%m-%d %H:%M:%S')}  \n")
                    f.write(f"**Sentiment:** {sentiment_label} ({sentiment:.2f})  \n\n")
                    f.write(f"{comment['body']}\n\n")
                    f.write("---\n\n")
            
            print(f"Exported post to {filepath}")
    
    def export_to_csv(self, posts, output_file="reddit_posts.csv"):
        """Export posts to CSV file"""
        # Create DataFrame from posts
        df_posts = pd.DataFrame([
            {k: v for k, v in post.items() if k != 'top_comments'}
            for post in posts
        ])
        
        # Convert UTC timestamps to readable dates
        if 'created_utc' in df_posts.columns:
            df_posts['created_date'] = df_posts['created_utc'].apply(
                lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')
            )
        
        # Export to CSV
        df_posts.to_csv(output_file, index=False)
        print(f"Exported {len(df_posts)} posts to {output_file}")
        
        # Create and export comments to separate CSV
        comments_data = []
        for post in posts:
            for comment in post['top_comments']:
                comment_data = comment.copy()
                comment_data['post_id'] = post['id']
                comment_data['post_title'] = post['title']
                
                # Convert sentiment dict to separate columns
                if 'sentiment' in comment_data:
                    for k, v in comment_data['sentiment'].items():
                        comment_data[f'sentiment_{k}'] = v
                    del comment_data['sentiment']
                    
                comments_data.append(comment_data)
                
        if comments_data:
            df_comments = pd.DataFrame(comments_data)
            
            # Convert UTC timestamps to readable dates
            if 'created_utc' in df_comments.columns:
                df_comments['created_date'] = df_comments['created_utc'].apply(
                    lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')
                )
                
            comments_file = output_file.replace('.csv', '_comments.csv')
            df_comments.to_csv(comments_file, index=False)
            print(f"Exported {len(df_comments)} comments to {comments_file}")
    
    def export_to_json(self, posts, output_file="reddit_posts.json"):
        """Export posts to JSON file"""
        # Convert datetime objects to strings for JSON serialization
        def json_serial(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        # Export to JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(posts, f, default=json_serial, indent=2)
            
        print(f"Exported {len(posts)} posts to {output_file}")
    
    def generate_summary(self, posts, output_file=None):
        """Generate a summary of the collected posts"""
        if not posts:
            print("No posts to summarize.")
            return
            
        # Count posts by subreddit
        subreddit_counts = Counter([p['subreddit'] for p in posts])
        
        # Get average scores
        avg_score = sum(p['score'] for p in posts) / len(posts)
        
        # Get time range
        post_times = [p['created_utc'] for p in posts]
        oldest = datetime.fromtimestamp(min(post_times))
        newest = datetime.fromtimestamp(max(post_times))
        
        # Extract trending topics
        trending = self.extract_trending_topics(posts, num_topics=10)
        
        # Format summary
        summary = [
            "# Reddit Content Summary",
            "",
            f"**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Total Posts Collected:** {len(posts)}",
            f"**Date Range:** {oldest.strftime('%Y-%m-%d')} to {newest.strftime('%Y-%m-%d')}",
            f"**Average Score:** {avg_score:.1f}",
            "",
            "## Posts by Subreddit",
            ""
        ]
        
        for subreddit, count in subreddit_counts.most_common():
            summary.append(f"- r/{subreddit}: {count} posts")
            
        summary.extend([
            "",
            "## Trending Topics",
            ""
        ])
        
        for word, count in trending:
            summary.append(f"- {word}: {count} occurrences")
            
        summary.extend([
            "",
            "## Top Posts",
            ""
        ])
        
        # Add top 5 posts by score
        top_posts = sorted(posts, key=lambda x: x['score'], reverse=True)[:5]
        for i, post in enumerate(top_posts, 1):
            summary.append(f"### {i}. {post['title']}")
            summary.append(f"**Score:** {post['score']} | r/{post['subreddit']} | u/{post['author']}")
            summary.append(f"**Link:** {post['url']}")
            summary.append("")
            
        # Output summary
        summary_text = "\n".join(summary)
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(summary_text)
            print(f"Summary saved to {output_file}")
        
        return summary_text
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='Reddit Content Aggregator')
    parser.add_argument('--subreddits', type=str, required=True, 
                        help='Comma-separated list of subreddits to scan')
    parser.add_argument('--limit', type=int, default=25,
                        help='Maximum number of posts per subreddit')
    parser.add_argument('--time', type=str, default='week',
                        choices=['hour', 'day', 'week', 'month', 'year', 'all'],
                        help='Time filter for posts')
    parser.add_argument('--min-upvotes', type=int, default=10,
                        help='Minimum upvotes for posts')
    parser.add_argument('--keywords', type=str, default=None,
                        help='Comma-separated keywords to filter by')
    parser.add_argument('--output-format', type=str, default='markdown',
                        choices=['markdown', 'csv', 'json', 'all'],
                        help='Output format')
    parser.add_argument('--output-dir', type=str, default='./content',
                        help='Output directory for content')
    parser.add_argument('--config', type=str, default='reddit_config.json',
                        help='Path to Reddit API configuration file')
    parser.add_argument('--summary', action='store_true',
                        help='Generate summary report')
    
    args = parser.parse_args()
    
    # Initialize aggregator
    try:
        aggregator = RedditContentAggregator(config_path=args.config)
    except FileNotFoundError:
        print(f"Error: Config file '{args.config}' not found.")
        print("Please create a config file with your Reddit API credentials.")
        print("Example: cp reddit_config.example.json reddit_config.json")
        return
    
    # Process subreddits and keywords arguments
    subreddits = [s.strip() for s in args.subreddits.split(',')]
    keywords = None
    if args.keywords:
        keywords = [k.strip() for k in args.keywords.split(',')]
    
    # Create output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    try:
        # Scan subreddits
        results = aggregator.scan_multiple_subreddits(
            subreddits,
            limit=args.limit,
            time_filter=args.time,
            min_upvotes=args.min_upvotes,
            keywords=keywords
        )
        
        # Flatten results
        all_posts = [post for subreddit_posts in results.values() 
                    for post in subreddit_posts]
        
        if not all_posts:
            print("No posts found matching your criteria.")
            return
            
        print(f"Found {len(all_posts)} posts matching criteria.")
        
        # Export results based on format
        if args.output_format in ['markdown', 'all']:
            markdown_dir = os.path.join(args.output_dir, 'markdown')
            aggregator.export_to_markdown(all_posts, output_dir=markdown_dir)
            
        if args.output_format in ['csv', 'all']:
            csv_file = os.path.join(args.output_dir, 'reddit_posts.csv')
            aggregator.export_to_csv(all_posts, output_file=csv_file)
            
        if args.output_format in ['json', 'all']:
            json_file = os.path.join(args.output_dir, 'reddit_posts.json')
            aggregator.export_to_json(all_posts, output_file=json_file)
            
        # Generate summary if requested
        if args.summary:
            summary_file = os.path.join(args.output_dir, 'summary.md')
            summary = aggregator.generate_summary(all_posts, output_file=summary_file)
            print("\nSummary:")
            print(summary[:500] + "..." if len(summary) > 500 else summary)
    
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close connection
        aggregator.close()

if __name__ == "__main__":
    main()
