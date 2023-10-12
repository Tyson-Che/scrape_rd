# test_error_handling.py
from error_handling import handle_error
import pytest
import logging
def test_handle_error(mocker, caplog):
    mock_db = mocker.Mock()
    mock_db.name = "TestDB"
    subreddit_name = "TestSubreddit"
    post_id = "12345"
    error_message = "SomeError"
    
    with caplog.at_level(logging.ERROR):
        handle_error(error_message, mock_db, subreddit_name, post_id)

    assert len(caplog.records) == 1  # Assert that one error was logged
    assert "An error occurred for DB: TestDB, Subreddit: TestSubreddit, Post ID: 12345. Error: SomeError" in caplog.text