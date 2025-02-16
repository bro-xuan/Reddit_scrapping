import praw
import pandas as pd
import time

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

# Define lists to store data
data = []

# Scraping posts & Comments
for post in subreddit.search("Puffin", limit= 5): 
    data.append({
        'Type': 'Post',
        'Post_id': post.id,
        'Title': post.title,
        'Author': post.author.name if post.author else 'Unknown',
        'Timestamp': post.created_utc,
        'Text': post.selftext,
        'Score': post.score,
        'Total_comments': post.num_comments,
        'Post_URL': post.url
    })

# Check if the post has comments
    if post.num_comments > 0:
        # Scraping comments for each post
        post.comments.replace_more(limit= 5)
        for comment in post.comments.list():
            data.append({
                'Type': 'Comment',
                'Post_id': post.id,
                'Title': post.title,
                'Author': comment.author.name if comment.author else 'Unknown',
                'Timestamp': pd.to_datetime(comment.created_utc, unit='s'),
                'Text': comment.body,
                'Score': comment.score,
                'Total_comments': 0, #Comments don't have this attribute
                'Post_URL': None  #Comments don't have this attribute
                
            })

time.sleep(2)

# Create pandas DataFrame for posts and comments
puffin_data = pd.DataFrame(data)

# Save to CSV (optional)
puffin_data.to_csv("puffin_posts.csv", index=False)

# Display first few rows
print(puffin_data.head())



