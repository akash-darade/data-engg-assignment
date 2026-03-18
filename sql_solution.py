import sqlite3
import csv

try:
    # Connect to DB
    conn = sqlite3.connect("Data Engineer_ETL Assignment.db")
    cursor = conn.cursor()

    # SQL Query
    query = """
    SELECT 
        c.customer_id AS customer,
        c.age AS age,
        i.item_name AS item,
        SUM(o.quantity) AS quantity
    FROM customer c
    JOIN sales s ON c.customer_id = s.customer_id
    JOIN orders o ON s.sales_id = o.sales_id
    JOIN items i ON o.item_id = i.item_id
    WHERE c.age BETWEEN 18 AND 35
    GROUP BY c.customer_id, c.age, i.item_name
    HAVING SUM(o.quantity) > 0
    """

    # Execute query
    cursor.execute(query)
    rows = cursor.fetchall()

    # Write to CSV
    with open("output_sql.csv", "w", newline="") as fp:
        writer = csv.writer(fp, delimiter=",")
        writer.writerow(["customer", "age", "item", "quantity"])
        writer.writerows(rows)

    print("Data exported successfully")

except Exception as e:
    print("Error:", e)

finally:
    if conn:
        conn.close()
