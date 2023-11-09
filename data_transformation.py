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
        'created_after': comment.created_utc - submission.created_utc,
        'sentiment': sentiment,
        'tokens': tokens
    }
    return comment_data

def process_comment_tree(comment, processed_comments, depth=0):
    # Skip MoreComments
    if isinstance(comment, MoreComments):
        return

    # Process the top-level comment
    comment_data = process_single_comment(comment)
    comment_data['replies'] = []  # Prepare to hold replies
    processed_comments.append(comment_data)

    # Now recursively process the replies to this comment
    for reply in comment.replies:
        process_comment_tree(reply, comment_data['replies'], depth + 1)

def process_comments(submission, post_data):
    # Ensure all MoreComments objects are replaced
    submission.comments.replace_more(limit=None)

    # Start with an empty list to hold processed comments
    processed_comments = []

    # Use threading to process top-level comments in parallel
    with ThreadPoolExecutor() as executor:
        # Prepare a future for each top-level comment
        futures = {executor.submit(process_comment_tree, top_level_comment, processed_comments): top_level_comment for top_level_comment in submission.comments}

        # Wait for all futures to complete
        for future in as_completed(futures):
            pass  # The work has already been done in the futures

    # Assign the processed comments to the post_data
    post_data['comments'] = processed_comments


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

def generate_doc_name(timestamp_str, title):
    return f"{timestamp_str}_{title}"

def data_transform(submission):
    # Filter Data based on timestamp
    timestamp_str = datetime.utcfromtimestamp(submission.created_utc).strftime('%y%m%d%H')
    
    if not filter_data(timestamp_str): #doneTODO skip this instead of returning empty
        return "", {}
    
    # Populate post_data dictionary
    post_data = populate_post_data(submission)
    
    # Process comments and update post_data
    process_comments(submission, post_data)
    
    # Calculate and populate metadata in post_data
    calculate_metadata(post_data)
    
    # Generate a name for the document to be inserted into MongoDB
    doc_name = generate_doc_name(timestamp_str, submission.title)
    
    return doc_name, post_data