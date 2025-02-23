import praw
import pandas as pd
import os
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

for submission in subreddit.search("puffin", sort="new", limit=100):
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
    for comment in submission.comments.list():
        data.append({
            'Type': 'Comment',
            'Author': comment.author.name if comment.author else 'Unknown',
            'Timestamp': datetime.utcfromtimestamp(comment.created_utc),
            'Text': comment.body,
            'Score': comment.score
        })

# Convert to DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv("puffin_newest.csv", index=False)

print("Data saved to puffin_newest.csv")
