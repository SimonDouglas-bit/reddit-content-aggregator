import argparse
import sys
import sqlite3
import os
import json
import pandas as pd
from reddit_aggregator import RedditContentAggregator

def main():
    parser = argparse.ArgumentParser(description='Export saved Reddit content')
    parser.add_argument('--format', type=str, default='markdown',
                       choices=['markdown', 'csv', 'json'],
                       help='Export format')
    parser.add_argument('--output-dir', type=str, default='./content',
                       help='Directory for exported content')
    parser.add_argument('--subreddit', type=str, default='',
                       help='Filter by subreddit (optional)')
    parser.add_argument('--min-upvotes', type=int, default=0,
                       help='Filter by minimum upvotes (optional)')
    parser.add_argument('--keywords', type=str, default='',
                       help='Filter by keywords (optional)')
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
        
    try:
        # Connect to database
        conn = sqlite3.connect('reddit_content.db')
        cursor = conn.cursor()
        
        # Build query
        query = "SELECT * FROM posts"
        conditions = []
        params = []
        
        if args.subreddit:
            conditions.append("subreddit = ?")
            params.append(args.subreddit)
            
        if args.min_upvotes > 0:
            conditions.append("score >= ?")
            params.append(args.min_upvotes)
            
        if args.keywords:
            keywords = [k.strip() for k in args.keywords.split(',')]
            keyword_conditions = []
            for keyword in keywords:
                keyword_conditions.append("(title LIKE ? OR selftext LIKE ?)")
                params.append(f"%{keyword}%")
                params.append(f"%{keyword}%")
                
            conditions.append("(" + " OR ".join(keyword_conditions) + ")")
            
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        # Execute query
        cursor.execute(query, params)
        posts = cursor.fetchall()
        
        # Get column names
        columns = [description[0] for description in cursor.description]
        
        # Convert to list of dictionaries
        post_dicts = []
        for post in posts:
            post_dict = dict(zip(columns, post))
            
            # Get comments for this post
            cursor.execute("SELECT * FROM comments WHERE post_id = ?", (post_dict['id'],))
            comments = cursor.fetchall()
            comment_columns = [description[0] for description in cursor.description]
            
            post_dict['top_comments'] = [
                dict(zip(comment_columns, comment))
                for comment in comments
            ]
            
            post_dicts.append(post_dict)
            
        # Export based on format
        if args.format == 'markdown':
            # Initialize aggregator for markdown export
            aggregator = RedditContentAggregator()
            aggregator.export_to_markdown(post_dicts, args.output_dir)
            
        elif args.format == 'csv':
            # Export posts to CSV
            df = pd.DataFrame(post_dicts)
            csv_path = os.path.join(args.output_dir, 'reddit_posts.csv')
            df.to_csv(csv_path, index=False)
            
            # Export comments to separate CSV
            comments_data = []
            for post in post_dicts:
                for comment in post['top_comments']:
                    comment_with_post = comment.copy()
                    comment_with_post['post_title'] = post['title']
                    comments_data.append(comment_with_post)
                    
            if comments_data:
                comments_df = pd.DataFrame(comments_data)
                comments_csv = os.path.join(args.output_dir, 'reddit_comments.csv')
                comments_df.to_csv(comments_csv, index=False)
                
            print(f"Exported {len(post_dicts)} posts to {csv_path}")
            
        elif args.format == 'json':
            # Export to JSON
            json_path = os.path.join(args.output_dir, 'reddit_content.json')
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(post_dicts, f, indent=2)
                
            print(f"Exported {len(post_dicts)} posts to {json_path}")
            
        print(f"Successfully exported {len(post_dicts)} posts in {args.format} format")
        
    except Exception as e:
        print(f"Error exporting data: {e}")
        sys.exit(1)
        
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
