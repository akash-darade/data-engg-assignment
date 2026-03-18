import sqlite3
import pandas as pd

try:
    # Connect DB
    conn = sqlite3.connect("Data Engineer_ETL Assignment.db")

    # SQL
    query = """
    SELECT 
        c.customer_id,
        c.age,
        i.item_name,
        o.quantity
    FROM customer c
    JOIN sales s ON c.customer_id = s.customer_id
    JOIN orders o ON s.sales_id = o.sales_id
    JOIN items i ON o.item_id = i.item_id
    WHERE c.age BETWEEN 18 AND 35
    """

    # Load into DataFrame
    df = pd.read_sql_query(query, conn)

    # Aggregate
    df = df.groupby(
        ["customer_id", "age", "item_name"]
    )["quantity"].sum().reset_index()

    # Filter
    df = df[df["quantity"] > 0]

    # Rename columns properly
    df = df.rename(columns={
        "customer_id": "customer",
        "item_name": "item"
    })

    # Save CSV
    df.to_csv("output_pandas.csv", index=False)

    print("✅ Pandas output generated")

except Exception as e:
    print("❌ Error:", e)

finally:
    if conn:
        conn.close()
