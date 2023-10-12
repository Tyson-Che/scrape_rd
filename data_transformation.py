from textblob import TextBlob
import datetime
def filter_data(timestamp_str):
    return timestamp_str[:2] in ['20', '21', '22', '23']

def populate_post_data(submission, count_tokens):
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
    return post_data

def process_comments(submission, post_data, count_tokens):
    submission.comments.replace_more(limit=None)
    all_comments = submission.comments.list()
    
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

def calculate_metadata(post_data):
    total_tokens = post_data['text_tokens']
    weighted_sentiment = post_data['upvotes'] * post_data['text_sentiment']
    total_upvotes = post_data['upvotes']
    
    for comment_data in post_data['comments']:
        total_tokens += comment_data['tokens']
        weighted_sentiment += comment_data['upvotes'] * comment_data['sentiment']
        total_upvotes += comment_data['upvotes']
    
    post_data['metadata']['tokens'] = total_tokens
    post_data['metadata']['weighted_sentiment'] = weighted_sentiment
    post_data['metadata']['importance'] = total_upvotes

def generate_doc_name(timestamp_str, title):
    return f"{timestamp_str}_{title}"

def data_transform(submission, count_tokens):
    # Filter Data based on timestamp
    timestamp_str = datetime.utcfromtimestamp(submission.created_utc).strftime('%y%m%d%H')
    if not filter_data(timestamp_str):
        return None
    
    # Populate post_data dictionary
    post_data = populate_post_data(submission, count_tokens)
    
    # Process comments and update post_data
    process_comments(submission, post_data, count_tokens)
    
    # Calculate and populate metadata in post_data
    calculate_metadata(post_data)
    
    # Generate a name for the document to be inserted into MongoDB
    doc_name = generate_doc_name(timestamp_str, submission.title)
    
    return doc_name, post_data