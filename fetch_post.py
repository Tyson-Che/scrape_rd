from data_fetching import fetch_data
from data_transformation import transform_data
from data_insertion import insert_data
from error_handling import handle_error
from logging_config import configure_logging
import logging

# Initialize logging
configure_logging()

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
        # Fetch the data
        raw_data = fetch_data(reddit_client, post_id)

        # Transform the data
        transformed_data = transform_data(raw_data)

        # Insert the data into MongoDB
        insert_data(db, subreddit_name, transformed_data)

        logging.info(f"Fetched and inserted data for post ID: {post_id}")

    except Exception as e:
        error_msg = f"An error occurred for DB: {db.name}, Subreddit: {subreddit_name}, Post ID: {post_id}. Error: {e}"
        handle_error(error_msg)
        
    logging.info(f"Thread using Reddit client with index: {client_index} completed.")
