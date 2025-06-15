import pandas as pd
import os

# Define the folder path where your CSV files are located
folder_path = r"C:\Users\dario\Downloads"

# List of your CSV file names
file_names = [
    "Shipment_Data_1976-1995_1999.csv",
    "Shipment_Data_1977_1995.csv",
    "Shipment_Data_2000_2024.csv"
]

# Create an empty list to store dataframes
all_dataframes = []

# Loop through each file, read it into a pandas DataFrame, and append to the list
for file_name in file_names:
    file_path = os.path.join(folder_path, file_name)
    try:
        df = pd.read_csv(file_path)
        all_dataframes.append(df)
        print(f"Successfully loaded {file_name}")
    except FileNotFoundError:
        print(f"Error: File not found - {file_name}")
    except Exception as e:
        print(f"An error occurred while reading {file_name}: {e}")

# Concatenate all dataframes into a single dataframe
if all_dataframes:
    merged_df = pd.concat(all_dataframes, ignore_index=True)

    # Define the output path for the merged CSV file
    output_file_path = os.path.join(folder_path, "Merged_Shipment_Data.csv")

    # Save the merged dataframe to a new CSV file
    merged_df.to_csv(output_file_path, index=False)
    print(f"\nSuccessfully merged {len(file_names)} files into {output_file_path}")
    print(f"Merged file has {len(merged_df)} rows and {len(merged_df.columns)} columns.")
else:
    print("No dataframes were loaded, so no merging was performed.")