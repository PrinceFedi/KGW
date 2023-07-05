# Python Script Documentation

This script performs several data processing steps, including reading data, filtering a dataframe based on various conditions, and summarizing results. It leverages several Python libraries such as pandas, pyreadstat, pyarrow, and numpy.

## Prerequisites

Before executing the script, ensure you have the following:

- Python 3.6 or higher
- `NPDB2301.POR`: This script requires the NPDB2301.POR dataset file to be present in the same directory as the Python script. This file should be located in a directory named 'NpdbPublicUseDataSpss'. If you don't have this file, you'll need to download it or otherwise acquire it. Please ensure the dataset is legitimate and respects all applicable laws and regulations for data usage.

## Reading the Data

The script first checks if a parquet file 'NPDB2301.parquet' already exists. If it doesn't, it reads an SPSS file 'NPDB2301.POR' using `pyreadstat.read_por`, converts it to a parquet file using `pyarrow`, and writes it to 'NPDB2301.parquet'. If 'NPDB2301.parquet' does exist, it simply reads the file. Parquet files are faster to read and are more efficient for storage, so this step could significantly improve performance if the data is being read multiple times.


`initialdFFilter`
    
    This function takes in our inital DataFrame and filters it to return practitioners who have at least one report of a certain type, either in a specific state or with a license in a specific state.

`filterSet`

    This method filters a DataFrame for specific report types and practitioners within a specific dataset. It takes a DataFrame, a dataset (which is another DataFrame that contains practitioners), and a list of report types as input. It first filters the DataFrame for practitioners in the given dataset and then filters the result further for specific report types.

`createTable`

    This function creates a table with specific columns and sorting from a given filtered DataFrame. It takes a state and a DataFrame as input. The function generates several new columns based on conditions and manipulations of the existing columns. Then it performs various sorts and filters. Finally, it groups the DataFrame by certain columns and performs additional transformations, resulting in a final table which is returned.

`occurenceCount`

    This method counts the occurrences of how many doctors had state licensure violations in the specified state first or not in another state then came to the specified state first. It takes a DataFrame as input and returns a Series object which contains the counts of each state order.

## Main Execution

In the main execution part of the script, several states, practitioners, and report types are defined. Then, for each state, the `initialdFFilter`, `filterSet`, `createTable`, and `occurenceCount` functions are applied in sequence, and their results are printed to the console.

## Testing

This script includes tests for validating the functionality of some of its core functions. The tests are contained in a separate file test_KGW.py within the test directory.

`test_initialdFFilter`

    This test validates the function initialdFFilter. The purpose of this function is to generate a 'state' column and to select practitioners based on specific conditions. The test creates a sample DataFrame, applies the function to it, and compares the resulting DataFrame to the expected DataFrame.

`test_filterSet`

    This test is designed to verify the function filterSet. The function filters a DataFrame based on specific report types and practitioners. The test creates a sample DataFrame and a dataset, applies the function, and compares the resulting DataFrame to an expected DataFrame.

## Running the scipt

You can exectute the main script using the following command in your terminal, 

```bash
python src/KGW_Final.py
```
To execute the test file run the following in the test directory:
```bash
pytest test/test_KGW_Final.py
```
### Note: 
    Make sure you run these commands at the root level of your project.
## Libraries

The script starts by importing necessary Python libraries which can be all found in the requirements.txt file.

- `os`: This module provides a way of using operating system dependent functionality. Here, it's used to check if a file exists in the filesystem.
- `pandas (as pd)`: This library offers data structures and operations for manipulating numerical tables and time series. 
- `pyreadstat (as prs)`: This module is used to read SPSS data files.
- `pyarrow (as pa) and pyarrow.parquet (as pq)`: These are used for reading and writing parquet files, a columnar storage file format.
- `numpy (as np)`: This package is fundamental for scientific computing with Python and it's used here for data manipulation.

Run the following command to install the necessary Python libraries:

   ```bash
   pip3 install -r requirements.txt
   ```

### Note:

If you do not want to install these requirements to your local machine gloably, you can set up a vitrual environment by running:

```bash
python3 -m venv venv
```
And then activating it via the following command(s):

On macOS and Linux:
```bash
source venv/bin/activate
```
On Windows:
```bash
.\venv\Scripts\activate
```

## Author Contact Info 

 - Linkedin: https://www.linkedin.com/in/fedi-aniefuna-a78a67242/
 - Email: fedianiefuna@gmail.com




This script is a useful tool for analyzing data on practitioners based on their reports and their states. It provides a robust way to filter, sort, and transform the data into a format that is useful for analysis.

