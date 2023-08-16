#Step0 : Import polars and create conection with sqlite3 
import sqlite3
import polars as pl

#Step 1: Extract
# Load the data from parquet files
sales_df = pl.read_parquet("sales.parquet")
products_df = pl.read_parquet("products.parquet")

#Step2: Transform Merge data  
combined_df = sales_df.join(products_df, on="product_id").select([
                         "sale_id", "product_id", "product_name", 
                         "quantity_sold", "unit_price", "sale_date"  ])
#Step2.1:print Merge data with out any calculation yet 
print(combined_df) 

#Step2.2:Creation of new data table with new calculation of the program #With.columns adding new column with calculation of new field & creating new data frame 
combined_df1 = combined_df.with_columns([(pl.col("quantity_sold") * pl.col("unit_price")).alias("total_revenue")])
print(combined_df1)

# Step3:Load, Save the transformed data into a new parquet file
combined_df1.write_parquet("combined_sales.parquet")

#Step3.1:Load data into SQLite new database (sales)
conn = sqlite3.connect("sales.db")

#Step3.2 Convert data frame from  pandas in order to bring the new table now called (combined sales) to sql 
combined_df1.to_pandas().to_sql("combined_sales", conn, if_exists="replace", index=False)

#Step3.3: In order to consult from sql, extract all the data executing a conection 
query = "SELECT * FROM combined_sales ;"
result = conn.execute(query).fetchall() 
conn.close()

#Extra step in order to confirm data extract the data  to make it easir to check a simple product
for row in result: 
    print(row)
