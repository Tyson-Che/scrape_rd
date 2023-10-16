#import modules
from init_config import init_mongodb, init_reddit_clients
from fetch_post import fetch_post
#import packages
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import csv
from tqdm import tqdm  # Import the tqdm package for progress bars
from pymongo import MongoClient
# @todo divide the todos.csv into several pieces
# @body 6 leagues, 12 csvs

# Initialize logging
logging.basicConfig(filename='main.log', level=logging.INFO)

# Read the list of tasks (futures)
# csv_file_path = 'todos.csv'

# def read_csv_tasks(file_path):
#     tasks = []
#     with open(file_path, mode='r', newline='', encoding='utf-8') as f:
#         reader = csv.reader(f)
#         next(reader)  # Skip header row
#         for row in reader:
#             tasks.append({
#                 'database': row[0],
#                 'collection': row[1],
#                 'post_id': row[2]
#             })
#     return tasks
import time  # for sleep

# Initialize a list of CSV files to be processed
csv_files = [f'todos_chunk_{i}.csv' for i in range(3, 51)]

def main():
    client = MongoClient("mongodb+srv://mac_m1:damnit@serverlessinstance0.2w1ndw2.mongodb.net/")
    reddit_clients = init_reddit_clients()
    
    while csv_files:  # Keep running until all CSV files are processed
        csv_file_path = csv_files.pop(0)  # Take the first CSV file
        tasks = read_csv_tasks(csv_file_path)
        
        # The rest of your code remains largely the same
        future_to_task = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
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

            for future in tqdm(as_completed(future_to_task), total=len(future_to_task), desc="Processing tasks"):
                task = future_to_task[future]
                try:
                    data = future.result()
                except Exception as e:
                    logging.error(f"An exception occurred during the processing of task {task}: {e}")
                else:
                    logging.info(f"Successfully processed task {task}")

        # Sleep for 10 minutes before processing the next CSV file
        time.sleep(600)

if __name__ == "__main__":
    main()
