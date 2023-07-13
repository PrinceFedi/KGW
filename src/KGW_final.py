import os

import pandas as pd
import pyreadstat as prs
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np




if not os.path.isfile('NPDB2301.parquet'):
    df, meta = prs.read_por("NpdbPublicUseDataSpss/NPDB2301.POR")
    # Convert to parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 'NPDB2301.parquet')
else:
    # Load data from parquet
    table = pq.read_table('NPDB2301.parquet')
    df = table.to_pandas()


def initialPractitionerFilter(state: str, dataframe, practitioners: list, reptype, basis):
    """
    Filters a dataframe based on the specified parameters to obtain records of particular practitioners 
    who have specific report types and are practicing in a specified state, excluding those with a certain basis code. 

    The function processes as follows:

    1. Filters for the given list of practitioners.
    2. Filters for practitioners who have at least one of the specified report types.
    3. Generates a 'state' column, assigning the practitioner's state based on the 'LICNSTAT', 'WORKSTAT', or 'HOMESTAT' fields.
    4. Excludes practitioners with the specified basis code.
    5. Further filters practitioners who are practicing in the specified state.

    Args:
        state (str): The target state to filter practitioners by.
        dataframe (pd.DataFrame): The source dataframe containing practitioners' records.
        practitioners (list): A list of practitioner fields to filter by.
        reptype (list): A list of report types to filter by.
        basis (str): The basis code to filter out.

    Returns:
        pd.DataFrame: The dataframe containing records of the filtered practitioners.
    """
  
    # Filter for doctors
    doctors = dataframe[dataframe['LICNFELD'].isin(practitioners)]

    # we want to filter for individuals that have at least 1 license report
    doctors = doctors[doctors["REPTYPE"].isin(reptype)]

    # filter out basis saction code 39
    # the .any checks any row against each of specified columns
    
    # Select practitioners with at least one report for a given state
    # Rewrite as a for loop for each state
    # Notice that the states have blanks, not NAs
    doctors['state'] = doctors.apply(lambda row: row['LICNSTAT'] if row['LICNSTAT'] != "" else
    (row['WORKSTAT'] if row['WORKSTAT'] != "" else
     (row['HOMESTAT'] if row['HOMESTAT'] != "" else None)), axis=1)
    
    # Filter out basis saction code 39
    filter_columns = ['BASISCD1', 'BASISCD2', 'BASISCD3', 'BASISCD4', 'BASISCD5']
    for column in filter_columns:
        doctors = doctors[doctors[column] != basis]

    doctors_in_state = doctors[(doctors['state'] == state) | (doctors['LICNSTAT'] == state)]

    
    return doctors_in_state


# ### Notes on code below:
# 
# - In Python, the isin() function is used for filtering the dataframe with a condition 
# - pd.concat().drop_duplicates(keep=False) is used to find the difference between two dataframes
# - del is used to remove variables

def filterData(dataframe, dataset, reptype, basis):
    """
    Filters a dataframe based on the specified parameters to obtain records of practitioners 
    within a specific dataset who have specific report types and excludes those with a certain basis code.

    The function processes as follows:

    1. Filters practitioners in the dataframe who are also present in the provided dataset.
    2. Excludes practitioners with the specified basis code.
    3. Further filters for practitioners who have at least one of the specified report types.

    Args:
        dataframe (pd.DataFrame): The source dataframe containing practitioners' records.
        dataset (pd.DataFrame): The dataset that contains the practitioners to filter by.
        reptype (list): A list of report types to filter by.
        basis (str): The basis code to filter out.

    Returns:
        pd.DataFrame: The dataframe containing records of the filtered practitioners.
    """

    
    # Filter whole data set for the specified state doctors
    doctors_state_plus = dataframe[dataframe['PRACTNUM'].isin(dataset['PRACTNUM'])]
    
    # Filter out basis saction code 39
    filter_columns = ['BASISCD1', 'BASISCD2', 'BASISCD3', 'BASISCD4', 'BASISCD5']
    for column in filter_columns:
        doctors_state_plus = doctors_state_plus[doctors_state_plus[column] != basis]
    # This is a specific filter for only the 301 and 302 license action rows rather than just doctors that have 301
    # and 302 and something else
    doctors_state_plus = doctors_state_plus[doctors_state_plus['REPTYPE'].isin(reptype)]
    
    

  
    return doctors_state_plus


def createTable(state: str, filtered_set):
    """
    Constructs a table from the provided DataFrame, containing details about doctor's practice state and
    the corresponding actions taken against them, along with certain features like the order in which the 
    state has taken disciplinary action, occurrence counts for state disciplinary actions, and their total actions.

    The function involves the following steps:

    1. Create 'state', 'year', 'numbering' and 'state_order' columns in the dataframe.
    2. Sort and group the dataframe based on 'PRACTNUM' and 'year'.
    3. Filter out rows where 'PRACTNUM' appears more than once.
    4. Filter out rows where all 'state' values for a 'PRACTNUM' are from the specified state.
    5. Create a 'total_actions' column and sort the dataframe again.

    Args:
        state (str): The target state for which the disciplinary actions are being tracked.
        filtered_set (pd.DataFrame): The source dataframe containing doctor's records.

    Returns:
        pd.DataFrame: A dataframe containing processed information about disciplinary actions.
    """

    # Creating 'state' column
    doctors_state_first = filtered_set.copy()
    doctors_state_first['state'] = doctors_state_first['LICNSTAT'].where(doctors_state_first['LICNSTAT'] != "",
                                                                         doctors_state_first['WORKSTAT'].where(
                                                                             doctors_state_first['WORKSTAT'] != "",
                                                                             doctors_state_first['HOMESTAT'].where(
                                                                                 doctors_state_first['HOMESTAT'] != "",
                                                                                 np.nan)))

    # Creating 'year' column since year isn't listed in the same place for all entries
    doctors_state_first['year'] = doctors_state_first['AAEFYEAR'].fillna(
        doctors_state_first['MALYEAR1'].fillna(doctors_state_first['MALYEAR2'].fillna(doctors_state_first['ORIGYEAR'])))

    # Sorting and numbering by 'PRACTNUM' and 'year'
    doctors_state_first.sort_values(['PRACTNUM', 'year'], inplace=True)
    doctors_state_first['numbering'] = doctors_state_first.groupby('PRACTNUM').cumcount() + 1

    # Creating 'state_order' column
    doctors_state_first['state_order'] = np.where(
        (doctors_state_first['numbering'] == 1) & (doctors_state_first['state'] == state),
        f"First disciplined in {state} ",
        np.where((doctors_state_first['numbering'] == 1) & (
                doctors_state_first['state'] != state), f"Not first discipline in {state} ", "check"))

    # Selecting columns
    doctors_state_first = doctors_state_first[['PRACTNUM', 'REPTYPE', 'year', 'state', 'numbering', 'state_order']]

    # Grouping by 'PRACTNUM' and 'state', then sorting by 'PRACTNUM' and 'year'
    doctors_state_first.sort_values(['PRACTNUM', 'year'], inplace=True)

    # Creating 'state_count' column
    doctors_state_first['state_count'] = doctors_state_first.groupby(['PRACTNUM', 'state']).cumcount() + 1

    # Delete practicum numbers that appear more than once:

    # `groupby('PRACTNUM')` groups our data set by only one column and then filters using the
    # lambda function checks if the number of rows in the group len(x) is greater than 1, and if so, it returns True,
    # otherwise it will filter the false values out

    doctors_state_first = doctors_state_first.groupby('PRACTNUM').filter(lambda x: len(x) > 1)

    # mask:

    # The `groupby` function splits the DataFrame into several DataFrames, one for each unique 'PRACTNUM'. Each 
    # group or mini-DataFrame contains all the rows from the original DataFrame that have that 'PRACTNUM'. Then, 
    # apply applies a function to each group. In this case, the function is a lambda function which takes a group as 
    # an input (represented as a pandas Series object x), and returns True if all 'state' values in the group are 
    # "OR" and False otherwise. 

    mask = doctors_state_first.groupby('PRACTNUM')['state'].apply(lambda x: (x == state).all())

    # When you use mask to index itself like mask[mask],
    # you're telling pandas to select only the elements of mask where the corresponding value in mask is True.
    # With this line we get all the indexes where the mask is true

    only_one_state = mask[mask].index

    # Use the ~ operator to invert the mask, then use this mask to filter the DataFrame
    doctors_state_first = doctors_state_first[~doctors_state_first['PRACTNUM'].isin(only_one_state)]

    # Creating 'total_actions' column
    doctors_state_first['total_actions'] = doctors_state_first.groupby('PRACTNUM')['state'].transform('count')

    doctors_state_first.sort_values(['PRACTNUM', 'year'], inplace=True)

    return doctors_state_first


def practitionerOccurence(df_table):
    """
    Counts the occurrences of each type of 'state_order' in the provided dataframe. The 'state_order' indicates 
    if a doctor had their first state licensure violation in the specified state, or had a violation in another 
    state before coming to the specified state.

    Args:
        df_table (pd.DataFrame): The dataframe containing 'state_order' details for each doctor.

    Returns:
        pd.Series: A series object with the count of occurrences for each 'state_order' type.
    """
    counts = df_table['state_order'].value_counts()
    return counts


if __name__ == '__main__':
     # Read the data from por file

    states = ["OR", "WA", "ID"]

    # KGW gave us the values of the specifc job categories they want to track
    practitioners = [10, 15, 20, 25, 642, 645, 600]

    # State licensure actions are defined in our data file as - 301, 302
    reptype = [301, 302]

    basis = '39'
    

    for i in states:
        if i == "OR":
            print(f"THIS IS OREGON'S DATA:\n")
        elif i == "WA":
            print(f"THIS IS WASHINGTON'S DATA:\n")
        else:
            print(f"THIS IS IDAHO'S DATA:\n")
        dataset = initialPractitionerFilter(i, df, practitioners, reptype, basis)
        # print(f"{set(dataset)}\n") commented out to just sshow dataframe

        set_filter = filterData(df, dataset, reptype, basis)
        print(f"{set_filter}\n")
        

        final_table = createTable(i, set_filter)
        print(f"{final_table}\n")
        
        count = practitionerOccurence(final_table)
        print(f"{count}\n")

        

