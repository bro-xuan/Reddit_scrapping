import praw
import pandas as pd
from datetime import datetime

# Initialize Reddit instance
reddit = praw.Reddit(
    client_id="k0wXdLqY8Xdv0t-oCSVCtg",
    client_secret="SCvqZRv0mil0Zv9plsHc4qJVRQxQRw",
    user_agent="Scrapping"
)

# Subreddit to scrape
subreddit = reddit.subreddit('VisitingIceland')

# File to store results
filename = "puffin_2nbatch.csv"

# Define the start date (January 1, 2022) and end date (February 28, 2025)
start_date = datetime(2022, 1, 1).timestamp()
end_date = datetime(2025, 2, 28).timestamp()

def scrape_puffin_posts():
    data = []
    
    try:
        # Search for posts with "puffin" in the title within the date range
        posts = subreddit.search(
            query="title:puffin",  # Only posts with "puffin" in the title
            sort="new",
            time_filter="all",  # Search all time (but we'll filter by date)
            limit=1000  # Maximum allowed by Reddit API
        )

        for post in posts:
            # Filter posts to only include those within the date range
            if start_date <= post.created_utc <= end_date:
                # Skip posts with no comments or score <= 1
                if post.num_comments == 0 or post.score <= 1:
                    continue

                # Fetch up to 10 comments for the post
                post.comments.replace_more(limit=0)  # Limit comment depth
                comments = [comment for comment in post.comments.list() if comment.score > 1][:10]  # Filter comments with score > 1

                # Add post data
                data.append({
                    'Type': 'Post',
                    'Post_id': post.id,
                    'Title': post.title,
                    'Author': post.author.name if post.author else 'Unknown',
                    'Timestamp': datetime.utcfromtimestamp(post.created_utc),
                    'Text': post.selftext,
                    'Score': post.score,
                    'Total_comments': post.num_comments,
                    'URL': f"https://reddit.com{post.permalink}"
                })

                # Add comment data (only comments with score > 1)
                for comment in comments:
                    data.append({
                        'Type': 'Comment',
                        'Post_id': post.id,
                        'Author': comment.author.name if comment.author else 'Unknown',
                        'Timestamp': datetime.utcfromtimestamp(comment.created_utc),
                        'Text': comment.body,
                        'Score': comment.score,
                        'URL': f"https://reddit.com{post.permalink}"
                    })

        if data:
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False)
            print(f"✅ Saved {len(df)} items (posts and comments) to {filename}")
        else:
            print("⚠️ No posts found from 2022 to February 2025 with 'puffin' in the title, comments, and score > 1")

    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")

# Run the function
scrape_puffin_posts()