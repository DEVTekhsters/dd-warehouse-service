# Description: This file contains the function to process the uploaded CSV file and convert it to a pandas DataFrame.
import pandas as pd
from fastapi import UploadFile
import csv

def process_csv(file: UploadFile, sep: str = ";") -> pd.DataFrame:
    """
    Process the uploaded CSV file and convert it to a pandas DataFrame.
    """
    # Read a few lines to detect the delimiter

    df = pd.read_csv(file.file, sep=sep,index_col=False)
    print(df)

    # Convert the 'timestamp' column to a readable date-time format
    # df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    
    return df
