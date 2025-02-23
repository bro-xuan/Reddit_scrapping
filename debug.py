import praw
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Validate environment variables
if not all([os.getenv("REDDIT_CLIENT_ID"), os.getenv("REDDIT_CLIENT_SECRET")]):
    raise ValueError("Missing Reddit API credentials in .env file")

# Initialize Reddit instance
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent="Scraping"
)

# Subreddit to scrape
subreddit = reddit.subreddit('VisitingIceland')

# Fetch posts containing the keyword "puffin"
data = []
batch_size = 100  # Number of posts to fetch in each batch
total_posts_to_fetch = 500  # Total number of posts to fetch
rest_time = 2  # Initial rest time in seconds

post_count = 0  # Track the number of posts fetched
comment_count = 0  # Track the number of comments fetched

try:
    for submission in subreddit.search("puffin", sort="new", limit=total_posts_to_fetch, time_filter="all"):
        title_contains_puffin = "puffin" in submission.title.lower()

        # Only process posts with "puffin" in the title
        if title_contains_puffin:
            # Append post data
            data.append({
                'Type': 'Post',
                'Title': submission.title,
                'Author': submission.author.name if submission.author else 'Unknown',
                'Timestamp': datetime.utcfromtimestamp(submission.created_utc),
                'Text': submission.selftext,
                'Score': submission.score,
                'Total_comments': submission.num_comments
            })
            post_count += 1

            # Fetch and filter comments
            submission.comments.replace_more(limit=0)  # Load all top-level comments
            current_comment_count = 0

            for comment in submission.comments.list():
                if "puffin" in comment.body.lower():
                    if current_comment_count >= 20:  # Limit to 20 comments per post
                        break

                    data.append({
                        'Type': 'Comment',
                        'Author': comment.author.name if comment.author else 'Unknown',
                        'Timestamp': datetime.utcfromtimestamp(comment.created_utc),
                        'Text': comment.body,
                        'Score': comment.score
                    })
                    current_comment_count += 1
                    comment_count += 1

            # Check if a batch is complete
            if post_count % batch_size == 0:
                print(f"Fetched {post_count} posts and {comment_count} comments. Taking a rest...")
                time.sleep(rest_time)  # Rest for the specified time

                # Check remaining API requests and adjust rest time
                limits = reddit.auth.limits
                remaining_requests = limits['remaining']
                print(f"Remaining API requests: {remaining_requests}")

                if remaining_requests > 500:  # If requests are very high, reduce rest time
                    rest_time = 1
                elif remaining_requests > 100:  # If requests are moderate, use moderate rest time
                    rest_time = 10
                else:  # If requests are low, increase rest time
                    rest_time = 40

    # Log the total number of posts and comments fetched
    print(f"Total posts fetched: {post_count}")
    print(f"Total comments fetched: {comment_count}")

except Exception as e:
    print(f"An error occurred: {e}")

# Convert to DataFrame
df = pd.DataFrame(data)

# Ensure consistent column order
columns = ['Type', 'Title', 'Author', 'Timestamp', 'Text', 'Score', 'Total_comments']
df = df.reindex(columns=columns)

# Save to CSV
if not df.empty:
    df.to_csv("puffin.csv", index=False)
    print("Data saved to csv")
else:
    print("No data to save.")
    