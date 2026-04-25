from ytb_history.utils.batching import chunked


def test_chunked_splits_expected_size() -> None:
    items = ["a", "b", "c", "d", "e"]
    assert chunked(items, 2) == [["a", "b"], ["c", "d"], ["e"]]


def test_chunked_rejects_non_positive_size() -> None:
    try:
        chunked(["a"], 0)
        assert False, "expected ValueError"
    except ValueError:
        assert True
