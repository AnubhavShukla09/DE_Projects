import pandas as pd


def preprocess_data():
    df = pd.read_csv("/tmp/reddit_posts.csv")
    df["body"] = df["body"].fillna("")  # Replace NaNs with empty strings
    df["text"] = (df["title"] + " " + df["body"]).str.lower()
    df.drop(columns=["title", "body"], inplace=True)
    df.to_csv("/tmp/reddit_posts_cleaned.csv", index=False)
