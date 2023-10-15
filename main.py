#import modules
from init_config import init_mongodb, init_reddit_clients
from fetch_post import fetch_post
#import packages
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import csv
from tqdm import tqdm  # Import the tqdm package for progress bars
from pymongo import MongoClient


# Initialize logging
logging.basicConfig(filename='main.log', level=logging.INFO)

# Read the list of tasks (futures)
csv_file_path = 'todos.csv'

def read_csv_tasks(file_path):
    tasks = []
    with open(file_path, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row
        for row in reader:
            tasks.append({
                'database': row[0],
                'collection': row[1],
                'post_id': row[2]
            })
    return tasks

def main(limit=None):
    # Initialize MongoDB and Reddit clients
    # init_mongodb()
    client = MongoClient("mongodb+srv://mac_m1:damnit@serverlessinstance0.2w1ndw2.mongodb.net/")
    
    
    reddit_clients = init_reddit_clients()

    # Read tasks
    tasks = read_csv_tasks(csv_file_path)
    
    # Limit the number of tasks if the limit parameter is set
    # Limit the number of tasks if the limit parameter is set
    if limit:
        tasks = tasks[:limit]

    # Prepare for multi-threading
    future_to_task = {}
    with ThreadPoolExecutor(max_workers=len(reddit_clients)) as executor:
        client_index = 0
        for task in tasks:
            future = executor.submit(
                fetch_post,
                client,
                task['database'],
                task['collection'],
                reddit_clients[client_index % len(reddit_clients)],
                task['post_id'],
                client_index
            )
            future_to_task[future] = task
            client_index += 1

        # Process completed futures with tqdm for progress bar
        for future in tqdm(as_completed(future_to_task), total=len(future_to_task), desc="Processing tasks"):
            task = future_to_task[future]
            try:
                data = future.result()
            except Exception as e:
                logging.error(f"An exception occurred during the processing of task {task}: {e}")
            else:
                logging.info(f"Successfully processed task {task}")

if __name__ == "__main__":
    # Run with only the first 10 tasks for the pilot test
    main(limit=5)
