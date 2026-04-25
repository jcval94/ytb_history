from ytb_history.utils.hashing import fingerprint_tags, fingerprint_text


def test_fingerprint_text_is_stable() -> None:
    assert fingerprint_text("abc") == fingerprint_text("abc")


def test_fingerprint_tags_order_independent() -> None:
    assert fingerprint_tags(["b", "a"]) == fingerprint_tags(["a", "b"])
