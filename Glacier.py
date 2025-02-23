import praw
import pandas as pd
from datetime import datetime, timedelta
import os
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Reddit instance
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT")
)

# Subreddit to scrape
subreddit = reddit.subreddit('VisitingIceland')

# File to store results
filename = "puffin_20250222.csv"

# Define the start date (5 years ago) and end date (now)
end_date = datetime.now().timestamp()
start_date = (datetime.now() - timedelta(days=1*365)).timestamp()

def scrape_puffin_posts():
    data = []
    
    try:
        # Search for posts with "puffin" in the title (case-insensitive)
        posts = subreddit.search(
            query="title:puffin",  # Only posts with "puffin" in the title
            sort="new",
            time_filter="year",
            limit=1000
        )

        for post in posts:
            # Filter posts to only include those within the date range
            if start_date <= post.created_utc <= end_date:
                # Skip posts with no comments or score <= 1
                if post.num_comments == 0 or post.score <= 1:
                    continue

                # Fetch up to 20 comments for the post
                post.comments.replace_more(limit=0)
                comments = [comment for comment in post.comments.list()][:20]

                # Add post data
                data.append({
                    'Type': 'Post',
                    'Title': post.title,
                    'Author': post.author.name if post.author else 'Unknown',
                    'Timestamp': datetime.utcfromtimestamp(post.created_utc),
                    'Text': post.selftext,
                    'Score': post.score,
                    'Total_comments': post.num_comments,
                })

                # Add comment data
                for comment in comments:
                    data.append({
                        'Type': 'Comment',
                        'Author': comment.author.name if comment.author else 'Unknown',
                        'Timestamp': datetime.utcfromtimestamp(comment.created_utc),
                        'Text': comment.body,
                        'Score': comment.score,
                    })

            time.sleep(2)  # Add delay to avoid rate limiting

        if data:
            df = pd.DataFrame(data)
            if os.path.exists(filename):
                df.to_csv(filename, mode='a', header=False, index=False)  # Append to existing file
            else:
                df.to_csv(filename, index=False)  # Create new file
            print(f"✅ Saved {len(df)} items (posts and comments) to {filename}")
        else:
            print("⚠️ No posts found from the past 5 years with 'puffin' in the title, comments, and score > 1")

    except praw.exceptions.PRAWException as e:
        print(f"❌ PRAW Error occurred: {str(e)}")
    except Exception as e:
        print(f"❌ Unexpected error occurred: {str(e)}")

# Run the function
scrape_puffin_posts()