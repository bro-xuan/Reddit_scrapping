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

try:
    for submission in subreddit.search("puffin", sort="new", limit=300):  # Adjust limit as needed
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

        # Add a delay after processing each post
        time.sleep(1)  # Adjust delay as needed

except Exception as e:
    print(f"An error occurred: {e}")

# Convert to DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv("puffin_bro.csv", index=False)

print("Data saved to csv")