import argparse
import sys
from reddit_aggregator import RedditContentAggregator

def main():
    parser = argparse.ArgumentParser(description='Scan multiple subreddits for content')
    parser.add_argument('--subreddits', type=str, required=True, 
                       help='Comma-separated list of subreddits to scan')
    parser.add_argument('--top', type=int, default=25,
                       help='Number of top posts to scan per subreddit')
    parser.add_argument('--time', type=str, default='week',
                       choices=['hour', 'day', 'week', 'month', 'year', 'all'],
                       help='Time filter for posts')
    parser.add_argument('--min-upvotes', type=int, default=10,
                       help='Minimum upvotes for posts')
    parser.add_argument('--keywords', type=str, default='',
                       help='Comma-separated keywords to filter posts')
    parser.add_argument('--export', action='store_true',
                       help='Export results to markdown')
    parser.add_argument('--output-dir', type=str, default='./content',
                       help='Directory for exported content')
    
    args = parser.parse_args()
    
    # Parse subreddits
    subreddits = [s.strip() for s in args.subreddits.split(',')]
    
    # Parse keywords if provided
    keywords = None
    if args.keywords:
        keywords = [k.strip() for k in args.keywords.split(',')]
    
    try:
        # Initialize aggregator
        aggregator = RedditContentAggregator()
        
        # Track total posts found
        total_posts = 0
        all_posts = []
        
        # Scan each subreddit
        for subreddit in subreddits:
            print(f"\nScanning r/{subreddit}...")
            posts = aggregator.scan_subreddit(
                subreddit,
                limit=args.top,
                time_filter=args.time,
                min_upvotes=args.min_upvotes,
                keywords=keywords
            )
            
            print(f"Found {len(posts)} posts in r/{subreddit}")
            total_posts += len(posts)
            all_posts.extend(posts)
            
        # Show summary
        print(f"\nTotal posts found across {len(subreddits)} subreddits: {total_posts}")
        
        # Show trending topics from all posts
        if len(all_posts) >= 5:
            trending = aggregator.extract_trending_topics(all_posts)
            print("\nTrending Topics Across All Subreddits:")
            for topic, count in trending:
                print(f"- {topic} ({count} mentions)")
                
        # Export if requested
        if args.export and all_posts:
            aggregator.export_to_markdown(all_posts, args.output_dir)
            print(f"\nExported {len(all_posts)} posts to {args.output_dir}")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
