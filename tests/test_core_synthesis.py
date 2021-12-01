import pytest

from basin3d.core.synthesis import _synthesize_query_identifiers


@pytest.mark.parametrize("values, filtered_params",
                         [(["F-9237"], ['9237']),
                          (["F-9237", "R-8e38e8", "F-00000"], ['9237', '00000']),
                          ("R-9237", [])],
                         ids=["single", "multiple", "none"])
def test_extract_query_param_ids(values, filtered_params):
    """Filtering of query arguments"""

    synthesized_values = _synthesize_query_identifiers(values, "F")
    assert synthesized_values == filtered_params
