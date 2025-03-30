import pandas as pd
import numpy as np

# Load dataset
df = pd.read_csv("S:\INGRITY\Python\dataset2.csv")

# Print the shape (number of rows and columns)
print("Dataset Shape (Rows, Columns):")
print(df.shape)

# Display the first 5 rows
pd.set_option('display.max_columns', None)  # Display all columns in the dataframe
print("\nFirst 5 Rows of the Dataset:")
print(df.head())

# I. Advanced Data Cleaning (Pandas)
# 1. Handle duplicate rows (keep first valid entry)
df = df.drop_duplicates()

# 2. Fix Product typos (map variations to canonical names: ProdA/Product A → "Product A")
df["Product"] = df["Product"].replace({"ProdA": "Product A"})

# Validate numericals:
# 3. Replace negative quantity with 1
df["Quantity"] = df["Quantity"].apply(lambda x: max(x, 1))

# 4. Drop rows with negative Price
df = df[df["Price"] > 0]

# 5. Fix Category inconsistencies (all electronics-related typos → "Electronics")
print("\nUnique Categories Before Cleaning:")
print(df["Category"].unique())  # Prints all unique categories

df["Category"] = df["Category"].str.lower()  # Convert to lowercase
df["Category"] = df["Category"].replace({"eletronics": "electronics"})  # Replace 'eletronics' with 'electronics'
df["Category"] = df["Category"].str.capitalize()  # Capitalize first letter

# Print the result
print("\nUnique Categories After Cleaning:")
print(df["Category"].unique())
print("\nFirst 5 Rows After Category Cleaning:")
print(df.head())

# 5. Impute missing Regions using CustomerID's most common region
most_common_region = df.groupby("CustomerID")["Region"].apply(lambda x: x.mode().iloc[0] if not x.mode().empty else "Unknown")
df["Region"] = df.apply(lambda row: most_common_region[row["CustomerID"]] if pd.isnull(row["Region"]) else row["Region"], axis=1)

# 6. Create IsPromo flag from PromoCode
df["IsPromo"] = df["PromoCode"].notna().astype(int)

print("\nFirst 5 Rows After Imputing Regions and Adding Promo Flag:")
print(df.head())

# II. Complex DateTime Operations

# 1. Convert all dates to UTC datetime
df["OrderDate"] = pd.to_datetime(df["OrderDate"], errors="coerce")  # Convert to datetime
if df["OrderDate"].dt.tz is None:
    df["OrderDate"] = df["OrderDate"].dt.tz_localize("UTC")  # Localize to UTC if naive
else:
    df["OrderDate"] = df["OrderDate"].dt.tz_convert("UTC")  # Convert to UTC if already timezone-aware

print("\nOrderDate Column After Conversion to UTC:")
print(df["OrderDate"].head())

# Check missing values in the entire dataset
missing_values = df.isna().sum()
print("\nMissing Values in the Dataset:")
print(missing_values)

# Impute missing dates with the median date
median_order_date = df["OrderDate"].median()
df["OrderDate"] = df["OrderDate"].fillna(median_order_date)

# Check if missing values are handled
print("\nMissing Values After Imputation:")
print(df.isna().sum())
print("\nFirst 5 Rows After Imputation of Missing Dates:")
print(df.head())

# 2. Calculate order processing time (assume returns happen 7 days after order)
df["ReturnDate"] = df["OrderDate"] + pd.Timedelta(days=7)  # Add 7 days to the order date to simulate return
df["ProcessingTime"] = (df["ReturnDate"] - df["OrderDate"]).dt.days  # Calculate the difference in days

print("\nFirst 5 Rows with Order and Return Dates and Processing Time:")
print(df[["OrderDate", "ReturnDate", "ProcessingTime"]].head())

# 3. Find weekly sales trends for electronics vs home goods

# Convert OrderDate to weekly period
df["Week"] = df["OrderDate"].dt.to_period("W")  # Convert to weekly periods

# Group by week and category, then sum the Price to get weekly sales trends
weekly_sales = df.groupby(["Week", "Category"])["Price"].sum().unstack().fillna(0)

# Filter for Electronics and Home categories
weekly_sales_trends = weekly_sales[["Electronics", "Home"]]

print("\nWeekly Sales Trends for Electronics vs Home Goods:")
print(weekly_sales_trends)

# 4. Identify customers with >2 orders in any 14-day window

# Sort by CustomerID and OrderDate
df = df.sort_values(by=["CustomerID", "OrderDate"])

# Set OrderDate as the index (this helps with rolling calculations)
df.set_index("OrderDate", inplace=True)

# Create a new column to store the rolling count of orders in a 14-day window for each customer
df["OrderCountInWindow"] = df.groupby("CustomerID").rolling('14D').count()["Price"].reset_index(drop=True)

# Reset the index to get the dataframe back into its original form
df.reset_index(inplace=True)

# Filter customers who have more than 2 orders in a 14-day window
customers_with_multiple_orders = df[df["OrderCountInWindow"] > 2]

# Display the customers with more than 2 orders in a 14-day window
print("\nCustomers with More Than 2 Orders in a 14-Day Window:")
print(customers_with_multiple_orders[["CustomerID", "OrderDate", "OrderCountInWindow"]].drop_duplicates())

# III. Advanced Collections & Optimization

from collections import defaultdict, Counter
from itertools import islice

# 1. Build nested dictionary: `{CustomerID: {"total_spent": X, "favorite_category": Y}}`
customer_summary = {
    cid: {
        "total_spent": group["Price"].sum(),
        "favorite_category": group["Category"].mode()[0] if not group["Category"].mode().empty else "Unknown"
    }
    for cid, group in df.groupby("CustomerID")
}

print("\nCustomer Summary (Total Spent & Favorite Category):")
print(customer_summary)

# 2. Use `defaultdict` to track return rates by region: `{"North": 0.25, ...}`
return_rates = defaultdict(lambda: 0)
returns = df[df["ReturnFlag"] == 1].groupby("Region").size()
orders = df.groupby("Region").size()
for region in orders.index:
    return_rates[region] = returns.get(region, 0) / orders[region]

print("\nReturn Rates by Region:")
print(return_rates)

# 3. Find most common promo code sequence using `itertools` and `Counter`
promo_sequences = Counter(zip(df["PromoCode"], islice(df["PromoCode"], 1, None)))
most_common_promo = promo_sequences.most_common(1)[0] if promo_sequences else None

print("\nMost Common Promo Code Sequence:")
print(most_common_promo)

# 4. Optimize memory usage by downcasting numerical columns
for col in ["Quantity", "Price"]:
    df[col] = pd.to_numeric(df[col], downcast="float")  # Converts to float32 for memory efficiency

print("\nData Types After Downcasting for Memory Optimization:")
print(df.dtypes)

# IV. Bonus

# 1. Create UDF to flag "suspicious orders" (multiple returns + high value)
def is_suspicious(order):
    return order["ReturnFlag"] == 1 and order["Price"] > 500 and df[df["CustomerID"] == order["CustomerID"]]["ReturnFlag"].sum() > 2

df["SuspiciousOrder"] = df.apply(is_suspicious, axis=1)

print("\nFirst 5 Rows with Suspicious Order Flag:")
print(df[["CustomerID", "OrderDate", "Price", "SuspiciousOrder"]].head())

# 2. Calculate rolling 7-day average sales using efficient windowing
# Ensure OrderDate is in datetime format
df["OrderDate"] = pd.to_datetime(df["OrderDate"])

# Sort by OrderDate within each Category
df = df.sort_values(by=["Category", "OrderDate"])

# Calculate the rolling 7-day average sales
df["RollingSales"] = df.groupby("Category")["Price"].transform(lambda x: x.rolling(window=7, min_periods=1).mean())

print("\nFirst 5 Rows with Rolling 7-Day Average Sales:")
print(df[["Category", "OrderDate", "Price", "RollingSales"]].head())
