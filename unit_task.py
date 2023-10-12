from datetime import datetime
from textblob import TextBlob
import logging
import time
import tiktoken

# Initialize logging
logging.basicConfig(filename='fetch_post.log', level=logging.INFO)

# Function to count tokens
def count_tokens(text):
    # Assuming you have a function for counting tokens
    encoding = tiktoken.encoding_for_model('gpt-3.5-turbo')
    return len(encoding.encode(text))

# Main fetching function
def fetch_post(db, subreddit_name, reddit_client, post_id, client_index):
    """Fetch a single post from Reddit and store it in MongoDB.

    Args:
        db (database): MongoDB database instance.
        subreddit_name (str): The subreddit name.
        reddit_client: PRAW Reddit API client.
        post_id (str): Reddit post ID.
        client_index (int): Reddit client index for logging.
    """
    
    try:
        logging.info(f"Thread using Reddit client with index: {client_index} started.")
        
        # Fetch Reddit post
        submission = reddit_client.submission(id=post_id)
        timestamp_str = datetime.utcfromtimestamp(submission.created_utc).strftime('%y%m%d%H')

        # Filter based on timestamp
        if timestamp_str[:2] not in ['20', '21', '22', '23']:
            return

        # Populate post data
        post_data = {
            'title': submission.title,
            'author': str(submission.author),
            'upvotes': submission.score,
            'created_utc': submission.created_utc,
            'comments_count': submission.num_comments,
            'url': submission.url,
            'text': submission.selftext,
            'text_sentiment': TextBlob(submission.selftext).sentiment.polarity,
            'text_tokens': count_tokens(submission.selftext),
            'comments': [],
            'metadata': {}
        }
        submission.comments.replace_more(limit=None)
        all_comments = submission.comments.list()
        
        total_tokens = post_data['text_tokens']
        weighted_sentiment = post_data['upvotes'] * post_data['text_sentiment']
        total_upvotes = post_data['upvotes']
        
        for comment in all_comments:
            sentiment = TextBlob(comment.body).sentiment.polarity
            tokens = count_tokens(comment.body)
            comment_data = {
                'author': str(comment.author),
                'body': comment.body,
                'upvotes': comment.score,
                'created_utc': comment.created_utc,
                'sentiment': sentiment,
                'tokens': tokens
            }
            post_data['comments'].append(comment_data)
            
            total_tokens += tokens
            weighted_sentiment += comment.score * sentiment
            total_upvotes += comment.score

        post_data['metadata']['tokens'] = total_tokens
        post_data['metadata']['weighted_sentiment'] = weighted_sentiment
        post_data['metadata']['importance'] = total_upvotes
        doc_name = f"{timestamp_str}_{submission.title}"
        db[subreddit_name].insert_one({doc_name: post_data})
        
        logging.info(f"Fetched and inserted data for post ID: {post_id}")

    except Exception as e:
        error_msg = f"An error occurred for DB: {db.name}, Subreddit: {subreddit_name}, Post ID: {post_id}. Error: {e}"
        logging.error(error_msg)
        
    logging.info(f"Thread using Reddit client with index: {client_index} completed.")
