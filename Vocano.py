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
    user_agent="Scrapping"
)

# Subreddit to scrape
subreddit = reddit.subreddit('VisitingIceland')

# File to store results
filename = "puffin_updated.csv"

# Define the start date (1 year ago) and end date (now)
end_date = datetime.now().timestamp()
start_date = (datetime.now() - timedelta(days=365)).timestamp()

# Batch size and delay between requests
BATCH_SIZE = 5  # Number of posts to retrieve per batch
DELAY_BETWEEN_REQUESTS = 2  # Delay in seconds between batches

def get_last_post_id():
    """Retrieve the last post ID from the existing CSV file."""
    if os.path.exists(filename):
        df = pd.read_csv(filename)
        if not df.empty:
            return df.iloc[-1]['Post_id']  # Return the last post ID
    return None

def scrape_puffin_posts():
    data = []
    after = None  # Used for pagination
    last_post_id = get_last_post_id()  # Get the last post ID from the previous run

    try:
        # Search for posts with "puffin" in the title (case-insensitive)
        posts = subreddit.search(
            query="title:puffin",  # Only posts with "puffin" in the title
            sort="new",
            time_filter="year",  # Get posts from the last year
            limit=BATCH_SIZE,
            params={"after": after}  # Pagination parameter
        )

        for post in posts:
            # Stop if we've reached the last post ID from the previous run
            if last_post_id and post.id == last_post_id:
                break

            # Manually filter posts to only include those within the exact 1-year range
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

            # Update the 'after' parameter for pagination
            after = post.fullname

        if data:
            df = pd.DataFrame(data)
            if os.path.exists(filename):
                df.to_csv(filename, mode='a', header=False, index=False)  # Append to existing file
            else:
                df.to_csv(filename, index=False)  # Create new file
            print(f"✅ Saved {len(data)} items (posts and comments) to {filename}")
        else:
            print("⚠️ No new posts found since the last run.")

    except praw.exceptions.PRAWException as e:
        print(f"❌ PRAW Error occurred: {str(e)}")
    except Exception as e:
        print(f"❌ Unexpected error occurred: {str(e)}")

# Run the function
scrape_puffin_posts()