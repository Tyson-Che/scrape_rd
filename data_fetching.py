from datetime import datetime

def fetch_reddit_data(reddit_client, post_id):
    submission = reddit_client.submission(id=post_id)
    timestamp_str = datetime.utcfromtimestamp(submission.created_utc).strftime('%y%m%d%H')
    return submission, timestamp_str

