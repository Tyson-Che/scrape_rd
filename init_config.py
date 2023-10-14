from pymongo import MongoClient
import os
import json
import praw


def init_mongodb():
    # Connect to the MongoDB database
    client = MongoClient("mongodb+srv://mac_m1:damnit@serverlessinstance0.2w1ndw2.mongodb.net/")

def init_reddit_clients():
    # Initialize Reddit clients
    reddit_clients = []
    user_agent = "Moin"

    with open('rd_threads_all.json', 'r') as f:
        all_creds = json.load(f)['credentials']

    for cred in all_creds:
        reddit = praw.Reddit(
            client_id=cred['client_id'],
            client_secret=cred['client_secret'],
            user_agent=user_agent
        )
        reddit_clients.append(reddit)

    return reddit_clients
