import praw
import pandas as pd


def fetch_reddit_data():
    reddit = praw.Reddit(
        client_id="YOUR_CLIENT_ID",
        client_secret="YOUR_CLIENT_SECRET",
        user_agent="YOUR_USER_AGENT",
    )

    subreddit = reddit.subreddit("artificialintelligence")
    posts = []

    for post in subreddit.hot(limit=100):
        posts.append([post.title, post.selftext, post.created_utc])

    df = pd.DataFrame(posts, columns=["title", "body", "created_utc"])
    df.to_csv("/tmp/reddit_posts.csv", index=False)
