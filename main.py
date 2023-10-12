from init_config import init_mongodb, init_reddit_clients
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from fetch_post import fetch_post

db = init_mongodb()
reddit_clients = init_reddit_clients()
