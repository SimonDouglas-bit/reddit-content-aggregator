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

def main():
    parser = argparse.ArgumentParser(description='Reddit Content Aggregator')
    parser.add_argument('--subreddits', type=str, required=True, 
                        help='Comma-separated list of subreddits to scan')
    parser.add_argument('--limit', type=int, default=25,
                        help='Maximum number of posts per subreddit')
    parser.add_argument('--time', type=str, default='week',
                        choices=['hour', 'day', 'week',
