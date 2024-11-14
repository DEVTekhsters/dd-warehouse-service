import pandas as pd
from fastapi import UploadFile

def process_csv(file: UploadFile, sep:str = ";") -> pd.DataFrame:
    """
    Process the uploaded CSV file and convert it to a pandas DataFrame.
    """
    df = pd.read_csv(file.file, sep=sep)
    print(df)

    
    return df
