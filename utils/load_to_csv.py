import os
import pandas as pd

def load_to_csv(dir,filename,df):
    output_dir = "/opt/airflow/Data/"+dir

        # Define the file path
    file_path = os.path.join(output_dir, filename)

        # Check if the CSV file exists
    if os.path.exists(file_path):
          # Read the existing data
        existing_data = pd.read_csv(file_path)
          # Append new data to existing data
        df = pd.concat([existing_data, df], ignore_index=True)
    else:
        pass

        # Save the combined DataFrame to the CSV file
    df.to_csv(file_path, index=False)