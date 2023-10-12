import logging

def handle_error(e, db, subreddit_name, post_id):
    error_msg = f"An error occurred for DB: {db.name}, Subreddit: {subreddit_name}, Post ID: {post_id}. Error: {e}"
    logging.error(error_msg)

