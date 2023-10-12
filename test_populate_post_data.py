from data_transformation import populate_post_data
from textblob import TextBlob

def test_populate_post_data(mocker):
    # Mock submission and count_tokens
    mock_submission = mocker.Mock()
    mock_submission.title = "Test Title"
    mock_submission.author = "Test Author"
    mock_submission.score = 10
    mock_submission.created_utc = 1633640400
    mock_submission.num_comments = 5
    mock_submission.url = "http://example.com"
    mock_submission.selftext = "Test Text"

    mock_count_tokens = mocker.Mock(return_value=50)

    # Execute the function
    post_data = populate_post_data(mock_submission, mock_count_tokens)

    # Build expected result
    expected_post_data = {
        'title': "Test Title",
        'author': "Test Author",
        'upvotes': 10,
        'created_utc': 1633640400,
        'comments_count': 5,
        'url': "http://example.com",
        'text': "Test Text",
        'text_sentiment': TextBlob("Test Text").sentiment.polarity,
        'text_tokens': 50,
        'comments': [],
        'metadata': {}
    }

    # Assertion
    assert post_data == expected_post_data
