import requests
from newspaper import Article
from transformers import pipeline

# Load Hugging Face summarization model
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

def extract_text_from_url(url):
    """Fetch and extract text content from a news URL using newspaper3k."""
    try:
        article = Article(url)
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        print("Error extracting article:", str(e))
        return None

def summarize_article(url):
    """Summarize the article from the given URL into one sentence."""
    text = extract_text_from_url(url)
    if not text or len(text) < 100:  # Ensure article has enough content
        return "Failed to extract content from the given URL."

    # Summarize text using the model
    summary = summarizer(text, max_length=50, min_length=20, do_sample=False)
    return summary[0]['summary_text']

# Example usage
news_url = "https://www.cbsnews.com/news/time-change-daylight-saving-spring-2025/"
summary = summarize_article(news_url)
print("Summary:", summary)
