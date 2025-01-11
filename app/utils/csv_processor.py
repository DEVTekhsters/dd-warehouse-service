# Description: This file contains the function to process the uploaded CSV file and convert it to a pandas DataFrame.
import pandas as pd
from fastapi import UploadFile
import csv

def process_csv(file: UploadFile) -> pd.DataFrame:
    """
    Process the uploaded CSV file and convert it to a pandas DataFrame.
    """
    # Read a few lines to detect the delimiter
    sample = file.file.read(1024).decode('utf-8')
    file.file.seek(0)
    sniffer = csv.Sniffer()
    delimiter = sniffer.sniff(sample).delimiter
    
    df = pd.read_csv(file.file, sep=delimiter)

    # Convert the 'timestamp' column to a readable date-time format
    # df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    
    print(df)
    
    return df
