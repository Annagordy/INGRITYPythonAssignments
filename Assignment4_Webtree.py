import pandas as pd
import os

# Read the input data CSV file into a Pandas DataFrame

df = pd.read_csv("S:\INGRITY\Python\Input_data.csv", header=0)

# Display the first 5 rows
pd.set_option('display.max_columns', None)  # Display all columns in the dataframe
print("\nFirst 5 Rows of the Dataset:")
print(df.head())

print(df.columns)


# Initialize an empty dictionary to store Web_TREE for each unique BRAND_ID
web_tree_dict = {}


def generate_web_tree(brand_id, parent_category_id):
    # If the brand_id is not in the dictionary, create an empty list for it
    if brand_id not in web_tree_dict:
        web_tree_dict[brand_id] = []

    # Append the parent_category_id to the list of categories for the given brand_id
    web_tree_dict[brand_id].append(parent_category_id)
    # Return the concatenated string of categories for the brand, separated by an underscore
    return "_".join(web_tree_dict[brand_id])


# Apply the function to create Web_TREE column
df['Web_TREE'] = df.apply(lambda row: generate_web_tree(row['BRAND_ID'], row['PARENT_CATEGORY_ID']), axis=1)

# Rename the DIVISION_ID column
df.rename(columns={'DVISION_ID': 'DIVISION_IDS'}, inplace=True)

# Define output directory
output_dir = "./out/json"
os.makedirs(output_dir, exist_ok=True)

# Write the dataframe to JSON format
df.to_json(os.path.join(output_dir, "df_out.json"), orient="records", lines=True)

# Write the dataframe to Parquet format
df.to_parquet(os.path.join(output_dir, "df_out.parquet"), index=False)

# Write the dataframe to CSV format
df.to_csv(os.path.join(output_dir, "df_out.csv"), index=False)

print("Processing complete. JSON and Parquet files saved.")


# Printing the Output files:

print("First few rows of Json File")
file_path = "C:/Users/Anna Roy/PycharmProjects/PythonProject2/out/json/df_out.json"  # Path to the Json file

df = pd.read_json(file_path, lines=True)
print(df.head())  # Display first few rows


print("First few rows of Parquet File")
# Path to the Parquet file
file_path = "C:/Users/Anna Roy/PycharmProjects/PythonProject2/out/json/df_out.parquet"

# Read the Parquet file into a DataFrame
df_parquet = pd.read_parquet(file_path)

# Display the first few rows of the DataFrame
print(df_parquet.head())


print("First few rows of CSV File")
# Path to the Parquet file
file_path = "C:/Users/Anna Roy/PycharmProjects/PythonProject2/out/json/df_out.csv"

# Read the Parquet file into a DataFrame
df_parquet = pd.read_csv(file_path)

# Display the first few rows of the DataFrame
print(df_parquet.head())