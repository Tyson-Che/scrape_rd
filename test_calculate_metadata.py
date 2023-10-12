from data_transformation import calculate_metadata
def test_calculate_metadata():
    # Prepare initial post_data with dummy values
    post_data = {
        'text_tokens': 100,
        'upvotes': 10,
        'text_sentiment': 0.5,
        'comments': [
            {'tokens': 50, 'upvotes': 5, 'sentiment': 0.4},
            {'tokens': 30, 'upvotes': 3, 'sentiment': 0.3}
        ],
        'metadata': {}
    }

    # Execute the function
    calculate_metadata(post_data)

    # Calculate expected metadata manually
    expected_total_tokens = 100 + 50 + 30  # 180
    expected_weighted_sentiment = (10 * 0.5) + (5 * 0.4) + (3 * 0.3)  # 5 + 2 + 0.9 = 7.9
    expected_total_upvotes = 10 + 5 + 3  # 18

    # Expected metadata
    expected_metadata = {
        'tokens': expected_total_tokens,
        'weighted_sentiment': expected_weighted_sentiment,
        'importance': expected_total_upvotes
    }

    # Assertion: check if post_data['metadata'] matches expected_metadata
    assert post_data['metadata'] == expected_metadata
