from data_fetching import fetch_reddit_data
import time  # for sleep
from data_transformation import data_transform
# from data_insertion import insert_data
from error_handling import handle_error
from logging_config import configure_logging
import logging

# Initialize logging
configure_logging()

def fetch_post(client, db, subreddit_name, reddit_client, post_id, client_index):
    retries = 5  # Number of retries
    delay = 5  # Delay in seconds
    success = False  # Flag to check if the operation was successful

    for i in range(retries):
        try:
            # Fetch the data
            raw_data = fetch_reddit_data(reddit_client, post_id)

            # Transform the data
            transformed_data = data_transform(raw_data)
            doc_str, post_data = transformed_data

            # Insert the data into MongoDB
            client[db][subreddit_name].insert_one({doc_str: post_data})
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
