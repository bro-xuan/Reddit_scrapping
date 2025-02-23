import praw
import pandas as pd
import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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
keyword = "puffin"
fetch_limit = 40  # Total posts to fetch
pause_interval = 20  # Number of posts before a pause
pause_duration = 30  # Pause time in seconds

# Track progress
processed_posts = 0

for submission in subreddit.search(keyword, sort="new", limit=fetch_limit):
    processed_posts += 1
    title_contains_keyword = re.search(r"\bpuffin\b", submission.title, re.IGNORECASE)

    data.append({
        'Type': 'Post',
        'Title': submission.title,
        'Author': submission.author.name if submission.author else 'Unknown',
        'Timestamp': datetime.utcfromtimestamp(submission.created_utc),
        'Text': submission.selftext,
        'Score': submission.score,
        'Total_comments': submission.num_comments
    })

    submission.comments.replace_more(limit=0)  # Load all top-level comments
    comments = submission.comments.list()

    filtered_comments = []
    if title_contains_keyword:
        # If the post title has "puffin", include all comments (limit to 20)
        filtered_comments = comments[:20]
    else:
        # Otherwise, include only comments that mention "puffin"
        filtered_comments = [c for c in comments if re.search(r"\bpuffin\b", c.body, re.IGNORECASE)]

    for comment in filtered_comments:
        data.append({
            'Type': 'Comment',
            'Author': comment.author.name if comment.author else 'Unknown',
            'Timestamp': datetime.utcfromtimestamp(comment.created_utc),
            'Text': comment.body,
            'Score': comment.score
        })

    # Pause after every 20 posts to respect API rate limits
    if processed_posts % pause_interval == 0:
        print(f"Processed {processed_posts} posts. Pausing for {pause_duration} seconds...")
        time.sleep(pause_duration)

# Convert to DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv("puffin_30sec_interval.csv", index=False)

print("Data saved to csv")
