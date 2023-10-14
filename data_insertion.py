def insert_data(db, subreddit_name, post_data, doc_name):
    client[db][subreddit_name].insert_one({doc_name: post_data})

