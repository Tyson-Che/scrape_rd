# test_token_counting.py
from token_counting import count_tokens

def test_count_tokens():
    assert count_tokens("Hello world!") == 3  # Assuming "Hello", "world", and "!" each are a token.

