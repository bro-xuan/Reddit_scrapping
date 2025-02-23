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
batch_size = 100  # Increase batch size to 50
total_posts_to_fetch =500  # Total number of posts to fetch
rest_time = 2  # Initial rest time in seconds

try:
    for i, submission in enumerate(subreddit.search("puffin", sort="new", limit=total_posts_to_fetch)):
        title_contains_puffin = "puffin" in submission.title.lower()

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

        # Fetch and filter comments
        submission.comments.replace_more(limit=0)  # Load all top-level comments
        comment_count = 0

        for comment in submission.comments.list():
            if title_contains_puffin or "puffin" in comment.body.lower():
                if comment_count >= 20:  # Limit to 20 comments per post
                    break

                data.append({
                    'Type': 'Comment',
                    'Author': comment.author.name if comment.author else 'Unknown',
                    'Timestamp': datetime.utcfromtimestamp(comment.created_utc),
                    'Text': comment.body,
                    'Score': comment.score
                })
                comment_count += 1

        # Check if a batch is complete
        if (i + 1) % batch_size == 0:
            print(f"Fetched {i + 1} posts. Taking a rest...")
            time.sleep(rest_time)  # Rest for the specified time

            # Check remaining API requests and adjust rest time
            limits = reddit.auth.limits
            remaining_requests = limits['remaining']
            print(f"Remaining API requests: {remaining_requests}")

            if remaining_requests > 500:  # If requests are very high, reduce rest time
                rest_time = 1
            elif remaining_requests > 100:  # If requests are moderate, use moderate rest time
                rest_time = 2
            else:  # If requests are low, increase rest time
                rest_time = 10

except Exception as e:
    print(f"An error occurred: {e}")

# Convert to DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv("puffin.csv", index=False)

print("Data saved to csv")