import pandas as pd
from pandas.testing import assert_frame_equal
from src.KGW_final import initialPractitionerFilter, filterData

def test_initialdPractitionerFilter():
    """This test makes sure the generation of the 'state column' is created and that only a specfic state is selected."""
    sample_df = pd.DataFrame({
        'LICNFELD': ['a', 'b', 'c', 'd'],
        'REPTYPE': [301, 302, 400, 302],
        'LICNSTAT': ['OR', '', 'WA', ''],
        'WORKSTAT': ['', 'OR', '', 'WA'],
        'HOMESTAT': ['', '', 'ID', ''],
        'PRACTNUM': ['1', '2', '3', '4'],
        'BASISCD1': ['30', '38', '42', '39'],
        'BASISCD2': ['30', '26', '42', '39'],
        'BASISCD3': ['30', '37', '42', '39'],
        'BASISCD4': ['30', '55', '42', '39'],
        'BASISCD5': ['30', '43', '42', '39']
    })
    practitioners = ['a', 'b','c', 'd']
    reptype = [301, 302]
    result = initialPractitionerFilter('OR', sample_df, practitioners, reptype, basis='39')
    print(result)
    expected_df = pd.DataFrame({
        'LICNFELD': ['a', 'b'],
        'REPTYPE': [301, 302],
        'LICNSTAT': ['OR', ''],
        'WORKSTAT': ['', 'OR'],
        'HOMESTAT': ['', ''],
        'PRACTNUM': ['1', '2'],
        'BASISCD1': ['30', '38'],
        'BASISCD2': ['30', '26'],
        'BASISCD3': ['30', '37'],
        'BASISCD4': ['30', '55'],
        'BASISCD5': ['30', '43'],
        'state': ['OR', 'OR']
        
    })
    assert_frame_equal(result, expected_df)


def test_filterData():
    # This test makes sure our reptype is filtered correctly
    sample_df = pd.DataFrame({
        'LICNFELD': ['a', 'b', 'c', 'd'],
        'REPTYPE': [301, 302, 400, 302],
        'LICNSTAT': ['OR', '', 'WA', 'OR'],
        'WORKSTAT': ['', 'OR', '', 'WA'],
        'HOMESTAT': ['', 'WA', '', 'OR'],
        'PRACTNUM': ['1', '2', '3', '4'],
        'BASISCD1': ['30', '38', '42', '40'],
        'BASISCD2': ['30', '26', '42', '38'],
        'BASISCD3': ['30', '39', '42', '56'],
        'BASISCD4': ['30', '55', '42', '45'],
        'BASISCD5': ['30', '43', '42', '17']
    })
    practitioners = ['a', 'b', 'c', 'd']
    reptype = [301, 302]
    dataset = initialPractitionerFilter('OR', sample_df, practitioners, reptype, basis='39')
    result = filterData(sample_df, dataset, reptype, basis='39')
    
    expected_df = pd.DataFrame({
        'LICNFELD': ['a', 'd'],
        'REPTYPE': [301, 302],
        'LICNSTAT': ['OR', 'OR'],
        'WORKSTAT': ['', 'WA'],
        'HOMESTAT': ['', 'OR'],
        'PRACTNUM': ['1', '4'],
        'BASISCD1': ['30', '40'],
        'BASISCD2': ['30', '38'],
        'BASISCD3': ['30', '56'],
        'BASISCD4': ['30', '45'],
        'BASISCD5': ['30', '17']
        
    })
    # Drop the index so that D is now at index 1 and not 3 like in the original dataframe
    assert_frame_equal(result.reset_index(drop=True), expected_df)




