import time  # for sleep

# from data_insertion import insert_data
from error_handling import handle_error
from logging_config import configure_logging
import logging

from textblob import TextBlob
from datetime import datetime
import tiktoken


def count_tokens(text):
    encoding = tiktoken.encoding_for_model('gpt-3.5-turbo')
    return len(encoding.encode(text))


def filter_data(timestamp_str):
    return timestamp_str[:2] in ['20', '21', '22', '23']


def populate_post_data(submission):
    post_data = {
        'title': submission.title,
        'author': str(submission.author),
        'upvotes': submission.score,
        'created_utc': submission.created_utc,
        'created_at': datetime.utcfromtimestamp(submission.created_utc).strftime('%y%m%d%H%M%S'),
        'comments_count': submission.num_comments,
        'url': submission.url,
        'text': submission.selftext,
        'text_sentiment': TextBlob(submission.selftext + submission.title).sentiment.polarity,
        'text_tokens': count_tokens(submission.selftext + submission.title),
        'comments': [],
    }
    return post_data


from concurrent.futures import ThreadPoolExecutor, as_completed
from praw.models import MoreComments


def process_single_comment(comment):
    sentiment = TextBlob(comment.body).sentiment.polarity
    tokens = count_tokens(comment.body)
    # print(type(comment),comment)
    comment_data = {
        'author': str(comment.author),
        'body': comment.body,
        'upvotes': comment.score,
        'created_utc': comment.created_utc,
        'created_at': datetime.utcfromtimestamp(comment.created_utc).strftime('%y%m%d%H%M%S'),
        'sentiment': sentiment,
        'tokens': tokens
    }
    return comment_data


def process_comment_tree(comment, processed_comments, depth=0):
    # Process the top-level comment
    comment_data = process_single_comment(comment)
    comment_data['replies'] = []  # Prepare to hold replies
    processed_comments.append(comment_data)

    # Now recursively process the replies to this comment
    for reply in comment.replies:
        process_comment_tree(reply, comment_data['replies'], depth + 1)


# def process_comments(submission, post_data):
#     submission.comments.replace_more(limit=0)
#
#     # Start with an empty list to hold processed comments
#     processed_comments = []
#
#     # Use threading to process top-level comments in parallel
#     with ThreadPoolExecutor() as executor:
#         # Prepare a future for each top-level comment
#         futures = {executor.submit(process_comment_tree, top_level_comment, processed_comments): top_level_comment for top_level_comment in submission.comments}
#
#         # Wait for all futures to complete
#         for future in as_completed(futures):
#             pass  # The work has already been done in the futures
#
#     # Assign the processed comments to the post_data
#     post_data['comments'] = processed_comments


def calculate_metadata(post_data):
    total_tokens = post_data['text_tokens']
    weighted_sentiment = post_data['upvotes'] * post_data['text_sentiment']
    total_upvotes = post_data['upvotes']

    for comment_data in post_data['comments']:
        total_tokens += comment_data['tokens']
        weighted_sentiment += comment_data['upvotes'] * comment_data['sentiment']
        total_upvotes += comment_data['upvotes']

    post_data['tokens'] = total_tokens
    post_data['weighted_sentiment'] = weighted_sentiment
    post_data['importance'] = total_upvotes


def data_transform(submission):
    # Filter Data based on timestamp
    timestamp_str = datetime.utcfromtimestamp(submission.created_utc).strftime('%y%m%d%H')

    if not filter_data(timestamp_str):  # doneTODO skip this instead of returning empty
        return "", {}

    # Populate post_data dictionary
    post_data = populate_post_data(submission)

    submission.comments.replace_more(limit=0)

    # Start with an empty list to hold processed comments
    processed_comments = []

    # Use threading to process top-level comments in parallel
    with ThreadPoolExecutor() as executor:
        # Prepare a future for each top-level comment
        futures = {executor.submit(process_comment_tree, top_level_comment, processed_comments): top_level_comment for
                   top_level_comment in submission.comments}

        # Wait for all futures to complete
        for future in as_completed(futures):
            pass  # The work has already been done in the futures
    post_data['comments'] = processed_comments

    return post_data

# Initialize logging
configure_logging()

def fetch_post(client, db, subreddit_name, reddit_client, post_id, client_index):
    retries = 5  # Number of retries
    delay = 5  # Delay in seconds
    success = False  # Flag to check if the operation was successful

    for i in range(retries):
        try:
            # Fetch the data
            raw_data = reddit_client.submission(id=post_id)
            # Transform the data
            transformed_data = data_transform(raw_data)
            post_data = transformed_data
            # insert only when post_data is not {}
            if post_data == {}:
                break
            # Insert the data into MongoDB
            client[db][subreddit_name].insert_one(post_data)
            logging.info(f"Fetched and inserted data for post ID: {post_id}")

            success = True  # Mark as successful
            break  # Exit the loop

        except Exception as e:
            error_msg = f"An error occurred for DB: {db}, Subreddit: {subreddit_name}, Post ID: {post_id}. Error: {e}"
            
            # Specific handling for 403 error
            if "403" in str(e):
                logging.error(f"403 Forbidden error. Retry {i + 1}/{retries}.")
                time.sleep(delay * (i + 1))  # Exponential backoff
            else:
                handle_error(error_msg, db, subreddit_name, post_id)
                break  # If it's not a 403 error, no retry, just handle the error and break

    if not success:
        with open('notdone.log', 'a') as f:
            f.write(f"{db},{subreddit_name},{post_id}\n")

    logging.info(f"Thread using Reddit client with index: {client_index} completed.")
