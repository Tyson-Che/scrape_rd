from data_transformation import process_comments, calculate_metadata  # Replace 'your_module' with the actual module name

def test_integration(mocker):
    # Mock TextBlob for sentiment analysis
    mock_textblob = mocker.Mock()
    mock_sentiment = mocker.Mock()
    mock_sentiment.polarity = 0.5  # Mocking sentiment polarity
    mock_textblob.return_value.sentiment = mock_sentiment
    mocker.patch('data_transformation.TextBlob', new=mock_textblob)  # Replace 'your_module' with the actual module name

    # Mock count_tokens function
    mock_count_tokens = mocker.Mock()
    mock_count_tokens.return_value = 10  # Mocking token count for simplicity

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
    post_data = {
        'text_tokens': 100,
        'upvotes': 10,
        'text_sentiment': 0.5,
        'comments': [],
        'metadata': {}
    }

    # Execute process_comments
    process_comments(mock_submission, post_data, mock_count_tokens)

    # Execute calculate_metadata
    calculate_metadata(post_data)

    # Expected metadata
    expected_metadata = {
        'tokens': 120,  # 100 (text_tokens) + 2 * 10 (from comments)
        'weighted_sentiment': 6.5,  # 5 (from text) + 2 (from comments)
        'importance': 13  # 10 (from text) + 3 (from comments)
    }

    # Assertion: check if post_data['metadata'] matches expected_metadata
    assert post_data['metadata'] == expected_metadata