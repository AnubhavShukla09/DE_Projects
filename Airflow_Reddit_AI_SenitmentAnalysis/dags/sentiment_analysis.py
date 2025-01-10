from textblob import TextBlob
import pandas as pd


def perform_sentiment_analysis():
    df = pd.read_csv("/tmp/reddit_posts_cleaned.csv")

    def analyze_sentiment(text):
        blob = TextBlob(text)
        return blob.sentiment.polarity

    df["sentiment"] = df["text"].apply(analyze_sentiment)
    df.to_csv("/tmp/reddit_posts_with_sentiment.csv", index=False)
