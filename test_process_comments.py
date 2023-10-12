from textblob import TextBlob
from data_transformation import process_comments

def test_process_comments(mocker):
    # Mock count_tokens function
    mock_count_tokens = mocker.Mock()
    mock_count_tokens.return_value = 10  # Mocking token count for simplicity

    # Mock TextBlob for sentiment analysis
    mock_textblob = mocker.Mock(spec=TextBlob)
    mock_sentiment = mocker.Mock()
    mock_sentiment.polarity = 0.5  # Mocking sentiment polarity
    mock_textblob.return_value.sentiment = mock_sentiment

# Make sure to patch TextBlob in the namespace where it's used
    mocker.patch('data_transformation.TextBlob', new=mock_textblob)

    # Mock comments and their attributes
    mock_comment1 = mocker.Mock()
    mock_comment1.author = "Author1"
    mock_comment1.body = "Comment1"
    mock_comment1.score = 1
    mock_comment1.created_utc = 1633640400

    mock_comment2 = mocker.Mock()
    mock_comment2.author = "Author2"
    mock_comment2.body = "Comment2"
    mock_comment2.score = 2
    mock_comment2.created_utc = 1633640500

    # Mock submission and its comments attribute
    mock_submission = mocker.Mock()
    mock_submission.comments.list.return_value = [mock_comment1, mock_comment2]

    # Prepare initial post_data
    post_data = {'comments': []}

    # Execute the function
    process_comments(mock_submission, post_data, mock_count_tokens)

    # Expected post_data after processing
    expected_post_data = {
        'comments': [
            {
                'author': 'Author1',
                'body': 'Comment1',
                'upvotes': 1,
                'created_utc': 1633640400,
                'sentiment': 0.5,  # Mocked sentiment
                'tokens': 10       # Mocked token count
            },
            {
                'author': 'Author2',
                'body': 'Comment2',
                'upvotes': 2,
                'created_utc': 1633640500,
                'sentiment': 0.5,  # Mocked sentiment
                'tokens': 10       # Mocked token count
            }
        ]
    }

    # Assertion
    assert post_data == expected_post_data
