import pandas as pd
from collections import Counter

# Load the dataset
df = pd.read_csv("S:\INGRITY\Python\dataset1.csv")

# Ensure column names are correct
df.columns = df.columns.str.strip()

# Display the first 5 rows
print("First 5 Rows of the Dataset:")
print(df.head())

# Check if there are any missing values
print("\nMissing Values in Each Column:")
print(df.isnull().sum())

# Category-Based Analysis:
# Find the category with the highest average rating
category_avg_rating = df.groupby("Category")["Rating"].mean()
highest_avg_rating_category = category_avg_rating.idxmax()


# Find the total stock available for each category
category_stock = df.groupby("Category")["Stock"].sum()

# Discounted Price Calculation:
#Create a new column Final_Price
df["Final_Price"] = df["Price"] - (df["Price"] * df["Discount"] / 100)

# Find the top 3 most discounted products
top_discounted_products = df.nlargest(3, "Discount")

# Supplier Analysis:
# Find the supplier with the highest average price of products
supplier_avg_price = df.groupby("Supplier")["Price"].mean()
highest_avg_price_supplier = supplier_avg_price.idxmax()

# Find the total number of unique suppliers
unique_suppliers_count = df["Supplier"].nunique()

# Collections Question:
# Count the occurrences of each category using collections
category_counts = Counter(df["Category"])

# Find the most common category
most_common_category = category_counts.most_common(1)[0]

# Output results
print("Category with highest average rating:", highest_avg_rating_category)
print("Total stock for each category:\n", category_stock)
print("Top 3 most discounted products:\n", top_discounted_products)
print("Supplier with highest average price:", highest_avg_price_supplier)
print("Total number of unique suppliers:", unique_suppliers_count)
print("Category counts using Counter:", category_counts)
print("Most common category:", most_common_category)



