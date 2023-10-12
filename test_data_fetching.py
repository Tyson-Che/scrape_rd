# test_data_fetching.py
from datetime import datetime
import pytest
from data_fetching import fetch_reddit_data

def test_fetch_reddit_data(mocker):
    # Mock the submission object and set its created_utc attribute
    mock_submission = mocker.Mock()
    mock_submission.created_utc = 1633640400  # A sample Unix timestamp

    # Configure the mock Reddit client
    mock_reddit_client = mocker.Mock()
    mock_reddit_client.submission.return_value = mock_submission

    # Execute the function and capture its return value
    returned_submission, returned_timestamp_str = fetch_reddit_data(mock_reddit_client, "some_post_id")
    
    # Your expected result based on the mock_submission's created_utc value
    expected_timestamp_str = datetime.utcfromtimestamp(1633640400).strftime('%y%m%d%H')
    
    # Assertions
    assert returned_submission == mock_submission
    assert returned_timestamp_str == expected_timestamp_str
