from basin3d.core.access import get_url, post_url


def test_get_url():
    """Test get url"""

    response = get_url("http://www.google.com")
    assert response
    assert response.status_code == 200


def test_post_url():
    """Test get post"""

    response = post_url("http://www.google.com")
    assert not response
    assert response.status_code == 405
