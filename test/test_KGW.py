import pandas as pd
from pandas.testing import assert_frame_equal
from src.KGW_final import initialdFFilter, filterSet

def test_initialdFFilter():
    """This test makes sure the generation of the 'state column' is created and that only a specfic state is selected."""
    sample_df = pd.DataFrame({
        'LICNFELD': ['a', 'b', 'c', 'a'],
        'REPTYPE': [301, 302, 400, 302],
        'LICNSTAT': ['OR', '', 'WA', ''],
        'WORKSTAT': ['', 'OR', '', 'WA'],
        'HOMESTAT': ['', '', 'ID', ''],
        'PRACTNUM': ['1', '2', '3', '4']
    })
    practitioners = ['a', 'b']
    reptype = [301, 302]
    result = initialdFFilter('OR', sample_df, practitioners, reptype)
    expected_df = pd.DataFrame({
        'LICNFELD': ['a', 'b'],
        'REPTYPE': [301, 302],
        'LICNSTAT': ['OR', ''],
        'WORKSTAT': ['', 'OR'],
        'HOMESTAT': ['', ''],
        'PRACTNUM': ['1', '2'],
        'state': ['OR', 'OR']
    })
    assert_frame_equal(result, expected_df)


def test_filterSet():
    # This test makes sure our reptype is filtered correctly
    sample_df = pd.DataFrame({
        'LICNFELD': ['a', 'b', 'c', 'a'],
        'REPTYPE': [301, 302, 400, 302],
        'LICNSTAT': ['OR', '', 'WA', ''],
        'WORKSTAT': ['', 'OR', '', 'WA'],
        'HOMESTAT': ['', 'WA', '', ''],
        'PRACTNUM': ['1', '2', '3', '4']
    })
    practitioners = ['a', 'b']
    reptype = [301, 302]
    dataset = initialdFFilter('OR', sample_df, practitioners, reptype)
    result = filterSet(sample_df, dataset, reptype)
    expected_df = pd.DataFrame({
        'LICNFELD': ['a', 'b'],
        'REPTYPE': [301, 302],
        'LICNSTAT': ['OR', ''],
        'WORKSTAT': ['', 'OR'],
        'HOMESTAT': ['', 'WA'],
        'PRACTNUM': ['1', '2']
    })
    assert_frame_equal(result, expected_df)




