from pymongo import MongoClient
import os
import json
import praw


def init_mongodb():
    # Connect to the MongoDB database
    client = MongoClient("mongodb+srv://mac_m1:damnit@serverlessinstance0.2w1ndw2.mongodb.net/")


from requests import Session

def init_reddit_clients(proxy_list):
    reddit_clients = []
    user_agent = "Moin"
    
    with open('rd_threads_all.json', 'r') as f:
        all_creds = json.load(f)['credentials']
        
    for idx, cred in enumerate(all_creds):
        session = Session()
        session.proxies = {
            'http': proxy_list[idx % len(proxy_list)]['http'],
            'https': proxy_list[idx % len(proxy_list)]['https']
        }
        reddit = praw.Reddit(
            client_id=cred['client_id'],
            client_secret=cred['client_secret'],
            user_agent=user_agent,
            requestor_kwargs={"session": session}  # pass the custom Session instance
        )
        reddit_clients.append(reddit)
    
    return reddit_clients