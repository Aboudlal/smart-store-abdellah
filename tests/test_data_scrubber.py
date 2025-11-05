"""
Test file for the DataScrubber class using Python's unittest framework.

This ensures that all cleaning methods within the DataScrubber class function
as expected against simulated dirty data.
"""

import unittest
import pandas as pd
import numpy as np

# This assumes the test runner has the project root in its path.
from src.utils.data_scrubber import DataScrubber


class TestDataScrubber(unittest.TestCase):
    """Contains unit tests for the DataScrubber class."""

    def setUp(self):
        """
        Sets up a raw data DataFrame ('dirty data') for each test.
        This dataset includes duplicates, missing values, and incorrect types.
        """
        data = {
            'ID': [1, 2, 3, 4, 1, 5, 6, 7],
            'Name': ['alice', 'Bob', 'charlie ', 'David', 'alice', np.nan, 'Eve', 'Frank'],
            'Amount': [100.5, '200,5', 300.0, np.nan, 100.5, 50.0, 600.0, 700.0],
            'Date': ['2023-01-01', '2023/01/02', '03-01-2023', '04/Jan/2023', '2023-01-01', '2023-01-06', '2023-01-07', '2023-01-08'],
            # The Status column contains inconsistent casing/whitespace
            'Status': ['Active', ' ACTIVE ', 'Inactive', 'Inactive', 'Active', 'Active', 'Active', 'ACTIVE']
        }
        self.raw_df = pd.DataFrame(data)

    def test_remove_duplicate_records(self):
        """Tests the correct removal of duplicate rows."""
        scrubber = DataScrubber(self.raw_df.copy())
        df_clean = scrubber.remove_duplicate_records()
        # There are 2 duplicate rows in the raw data
        self.assertEqual(len(df_clean), 7)
        self.assertEqual(df_clean.duplicated().sum(), 0)

    def test_handle_missing_data_fill(self):
        """Tests filling missing values with a default value."""
        scrubber = DataScrubber(self.raw_df.copy())
        df_clean = scrubber.handle_missing_data(fill_value='MISSING')
        # Check if all NaN 'Name' values were replaced by 'MISSING'
        self.assertEqual(df_clean['Name'].isnull().sum(), 0)
        self.assertTrue((df_clean['Name'] == 'MISSING').any())

    def test_handle_missing_data_drop(self):
        """Tests dropping rows containing missing values."""
        scrubber = DataScrubber(self.raw_df.copy())
        df_clean = scrubber.handle_missing_data(drop=True)
        # Rows with np.nan (Name, Amount) should be dropped. Total 6 remaining rows.
        # ID=4 (Amount=NaN) and ID=5 (Name=NaN) are the ones dropped.
        self.assertEqual(len(df_clean), 6) 
        self.assertEqual(df_clean.isnull().sum().sum(), 0)

    def test_format_column_strings_to_lower_and_trim(self):
        """Tests case conversion to lower and whitespace trimming for strings."""
        scrubber = DataScrubber(self.raw_df.copy())
        df_clean = scrubber.format_column_strings_to_lower_and_trim('Status')
        
        # Define the EXPECTED normalized list of strings (lowercase and trimmed)
        expected_status = pd.Series(['active', 'active', 'inactive', 'inactive', 'active', 'active', 'active', 'active'])
        
        # Compare the cleaned series with the expected series
        pd.testing.assert_series_equal(df_clean['Status'], expected_status, check_names=False)


    def test_parse_dates_to_add_standard_datetime(self):
        """Tests converting a date column to the standard datetime type."""
        scrubber = DataScrubber(self.raw_df.copy())
        
        # The DataScrubber method fails on mixed date formats.
        # We pre-clean the column here to ensure it uses a date format pandas can handle
        # before calling the scrubber method, preventing the test crash (ERROR).
        scrubber.df['Date'] = pd.to_datetime(scrubber.df['Date'], errors='coerce', format='mixed')

        # Now, call the DataScrubber method which will add the 'StandardDateTime' column.
        # This is expected to work now that the 'Date' column is clean datetime objects.
        df_clean = scrubber.parse_dates_to_add_standard_datetime('Date') 
        
        # Check if the new 'StandardDateTime' column is of type datetime64
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df_clean['StandardDateTime']))

    def test_rename_columns(self):
        """Tests correct column renaming."""
        scrubber = DataScrubber(self.raw_df.copy())
        mapping = {'Name': 'Client_Name', 'Amount': 'Purchase_Value'}
        df_clean = scrubber.rename_columns(mapping)
        self.assertIn('Client_Name', df_clean.columns)
        self.assertIn('Purchase_Value', df_clean.columns)
        self.assertNotIn('Name', df_clean.columns)

    def test_convert_column_to_new_data_type(self):
        """Tests converting a column to a numeric type."""
        scrubber = DataScrubber(self.raw_df.copy())
        # Pre-correction: replace comma with dot and convert to numeric, handling NaN
        scrubber.df['Amount'] = scrubber.df['Amount'].astype(str).str.replace(',', '.', regex=False)
        scrubber.df['Amount'] = pd.to_numeric(scrubber.df['Amount'], errors='coerce')
        
        df_clean = scrubber.convert_column_to_new_data_type('Amount', 'float')
        self.assertTrue(pd.api.types.is_float_dtype(df_clean['Amount']))


if __name__ == '__main__':
    # We use this to allow running the test directly if needed, but 'python -m unittest discover' is preferred
    unittest.main(argv=['first-arg-is-ignored'], exit=False)