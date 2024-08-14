import pandas as pd

# # Define the file path for the main CSV file and the output directory
input_file_path = '/Users/pruthvipatel/Documents/projects/Loan Defaulter ETL/Data/entire_data.csv'
# output_dir = '/Users/pruthvipatel/Documents/projects/Loan Defaulter ETL/Data/'




# # Load the CSV file into a DataFrame
df = pd.read_csv(input_file_path)

print ("total rows", len(df))

# check null

# # Check for null values in the DataFrame
null_values = df.isnull().sum()
# convert to df null values
df = pd.DataFrame(null_values, columns=['null_values'])
df

# find max value of amonf null 
# # Find the column with the maximum number of null values
max_null_column = df['null_values'].idxmax()

print(f"The column with the maximum number of null values is: {max_null_column}")
print(f"The number of null values in this column is: {df.loc[max_null_column, 'null_values']}")

# # Total number of rows in the DataFrame
# total_rows = len(df)

# # Define split sizes
# json_size = 5000
# parquet_csv_size = (total_rows - json_size) // 2

# # Split the DataFrame
# df_json = df.iloc[:json_size]
# df_remaining = df.iloc[json_size:]

# # Split remaining data into CSV and Parquet
# df_csv = df_remaining.iloc[:parquet_csv_size]
# df_parquet = df_remaining.iloc[parquet_csv_size:]

# # Save the DataFrames to the specified formats
# df_json.to_json(output_dir + 'data.json', orient='records', lines=True)
# df_csv.to_csv(output_dir + 'data.csv', index=False)
# df_parquet.to_parquet(output_dir + 'data.parquet', index=False, engine='pyarrow')

# print("Data has been successfully split and saved.")


# # compare vales between csv and parquet and look for duplicates

# # Load the CSV and Parquet files into DataFrames
# df_csv = pd.read_csv(output_dir + 'data.csv')
# df_parquet = pd.read_parquet(output_dir + 'data.parquet')

# # Find the number of duplicates in the CSV and Parquet DataFrames
# num_duplicates_csv = df_csv.duplicated().sum()
# num_duplicates_parquet = df_parquet.duplicated().sum()

# # Find the number of rows in the CSV and Parquet DataFrames
# num_rows_csv = len(df_csv)
# num_rows_parquet = len(df_parquet)

# # Find the number of unique rows in the CSV and Parquet DataFrames
# num_unique_rows_csv = len(df_csv.drop_duplicates())
# num_unique_rows_parquet = len(df_parquet.drop_duplicates())


# import pandas as pd

# # Define file paths
# csv_file_path = '/Users/pruthvipatel/Documents/projects/Loan Defaulter ETL/Data/data.csv'
# parquet_file_path = '/Users/pruthvipatel/Documents/projects/Loan Defaulter ETL/Data/data.parquet'

# # Load the CSV and Parquet files into DataFrames
# df_csv = pd.read_csv(csv_file_path)
# df_parquet = pd.read_parquet(parquet_file_path)

# # Check for duplicates between the two DataFrames
# # Concatenate the DataFrames and find duplicates
# combined_df = pd.concat([df_csv, df_parquet], ignore_index=True)

# # Find duplicate rows
# duplicates = combined_df[combined_df.duplicated(keep=False)]

# # If there are duplicates, display them
# if not duplicates.empty:
#     print("Duplicates found between CSV and Parquet files:")
#     print(duplicates)
# else:
#     print("No duplicates found between CSV and Parquet files.")