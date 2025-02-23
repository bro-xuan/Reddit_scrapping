import praw
import pandas as pd
import time
import os

# Initialize Reddit instance
reddit = praw.Reddit(client_id="k0wXdLqY8Xdv0t-oCSVCtg",
                     client_secret="SCvqZRv0mil0Zv9plsHc4qJVRQxQRw",
                     user_agent="Scrapping")

# Subreddit to scrape
subreddit = reddit.subreddit('VisitingIceland')

# Display the name of the Subreddit
print("Display Name:", subreddit.display_name)
 
# Display the title of the Subreddit
print("Title:", subreddit.title)
 
# Display the description of the Subreddit
print("Description:", subreddit.public_description)

# File to store results
filename = "puffin_posts.csv"

batch_size = 500

 
# Function to scrape posts in batches
def scrape_puffin_posts():
    data = []

    # Scraping posts & Comments
    ## for post in subreddit.search("Puffin", sort="new", time_filter="all", limit= batch_size): 
    for post in subreddit.search("Puffin", sort="new", time_filter="all", limit= batch_size):
            if post.score > 1: 
                data.append({
                    'Type': 'Post',
                    'Post_id': post.id,
                    'Title': post.title,
                    'Author': post.author.name if post.author else 'Unknown',
                    'Timestamp': post.created_utc,
                    'Text': post.selftext,
                    'Score': post.score,
                    'Total_comments': post.num_comments,
                })

            # Check if the post has comments
                if post.num_comments > 0:
                    # Scraping comments for each post
                    post.comments.replace_more(limit= 5)
                    for comment in post.comments.list():
                        if comment.score > 1:
                            data.append({
                                'Type': 'Comment',
                                'Post_id': post.id,
                                'Title': post.title,
                                'Author': comment.author.name if comment.author else 'Unknown',
                                'Timestamp': pd.to_datetime(comment.created_utc, unit='s'),
                                'Text': comment.body,
                                'Score': comment.score,
                                'Total_comments': 0, #Comments don't have this attribute
                            })

    time.sleep(2)

   # Convert to DataFrame
    df = pd.DataFrame(data)

    # Append new data to CSV
    if os.path.exists(filename):
        df.to_csv(filename, mode='a', header=False, index=False)  # Append mode
    else:
        df.to_csv(filename, mode='w', index=False)  # Create new file

    print(f"✅ Scraped {batch_size} posts and saved to {filename}")

# Run the function
scrape_puffin_posts()