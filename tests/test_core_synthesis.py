import pytest

from basin3d.core.synthesis import extract_id, extract_query_param_ids, filter_query_param_values


def test_extract_id():
    """Test the synthesis of an id to the datasource id"""

    assert extract_id("F-438935") == "438935"


@pytest.mark.parametrize("values, filtered_params",
                         [({"foo": ["F-9237"], "b": "7-i"}, {'foo': ['F-9237']}),
                          ({"foo": ["F-9237", "R-8e38e8", "F-00000"], "b": "7-i"}, {'foo': ['F-9237', 'F-00000']}),
                          ({"foo": "R-9237", "b": "7-i"}, {'foo': []})],
                         ids=["single", "multiple", "none"])
def test_filter_query_param_values(values, filtered_params):
    """Filtering of query arguments"""

    query_params = {}
    filter_query_param_values("foo", "F", query_params, **values)
    assert query_params == filtered_params


@pytest.mark.parametrize("values, filtered_params",
                         [({"foo": ["F-9237"], "b": "7-i"}, {'foo': ['9237']}),
                          ({"foo": ["F-9237", "R-8e38e8", "F-00000"], "b": "7-i"}, {'foo': ['9237', '00000']}),
                          ({"foo": "R-9237", "b": "7-i"}, {'foo': []})],
                         ids=["single", "multiple", "none"])
def test_extract_query_param_ids(values, filtered_params):
    """Filtering of query arguments"""

    query_params = {}
    extract_query_param_ids("foo", "F", query_params, **values)
    assert query_params == filtered_params
