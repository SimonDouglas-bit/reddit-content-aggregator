import argparse
import sys
from reddit_aggregator import RedditContentAggregator

def main():
    parser = argparse.ArgumentParser(description='Scan a single subreddit for content')
    parser.add_argument('--subreddit', type=str, required=True, 
                       help='Subreddit name to scan (without r/)')
    parser.add_argument('--top', type=int, default=25,
                       help='Number of top posts to scan')
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
    
    # Parse keywords if provided
    keywords = None
    if args.keywords:
        keywords = [k.strip() for k in args.keywords.split(',')]
    
    try:
        # Initialize aggregator
        aggregator = RedditContentAggregator()
        
        # Scan subreddit
        posts = aggregator.scan_subreddit(
            args.subreddit,
            limit=args.top,
            time_filter=args.time,
            min_upvotes=args.min_upvotes,
            keywords=keywords
        )
        
        if not posts:
            print(f"No posts found matching your criteria in r/{args.subreddit}")
            sys.exit(0)
            
        # Print summary
        print(f"\nFound {len(posts)} posts in r/{args.subreddit}")
        
        # Show trending topics
        if len(posts) >= 5:
            trending = aggregator.extract_trending_topics(posts)
            print("\nTrending Topics:")
            for topic, count in trending:
                print(f"- {topic} ({count} mentions)")
                
        # Export if requested
        if args.export:
            aggregator.export_to_markdown(posts, args.output_dir)
            print(f"\nExported {len(posts)} posts to {args.output_dir}")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
