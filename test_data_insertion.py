# test_data_insertion.py
import pytest
from data_insertion import insert_data

def test_insert_data(mocker):
    # Create a mock for the MongoDB collection
    mock_collection = mocker.Mock()
    
    # Create a mock for the db object
    mock_db = mocker.Mock()

    # Explicitly set the __getitem__ method on the mock object
    mock_db.__getitem__ = mocker.Mock()
    mock_db.__getitem__.return_value = mock_collection

    # The data you'll use for testing
    subreddit_name = "test_subreddit"
    post_data = {"key": "value"}
    doc_name = "test_doc"

    # Call the function with the mock db and the test data
    insert_data(mock_db, subreddit_name, post_data, doc_name)

    # Check if the function called the insert_one method on the mock collection
    # with the expected document
    mock_collection.insert_one.assert_called_with({doc_name: post_data})

    # Alternatively, check if __getitem__ was called on the mock db with the subreddit name
    mock_db.__getitem__.assert_called_with(subreddit_name)
