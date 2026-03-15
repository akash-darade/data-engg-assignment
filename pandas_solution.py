import sqlite3
import pandas as pd

conn=sqlite3.connect("Data Engineer_ETL Assignment.db")

query="""
      select c.customer_id,
      c.age,
      i.item_name,
      o.quantity
      from customers c
      join sales s on c.customer_id =s.customer_id
      join orders o on s.sales_id =o.sales_id
      join items i on o.item_id =i.item_id
      where c.age between 18 and 35
      """
df=pd.read_sql_query(query,conn)
df=df.groupby(["customer_id","age","item_name"])["quantity"].sum().reset_index()
df=df[df["quantity"] > 0]
df.columns=["customer","age","item","quantity"]
df.to_csv("output_pandas.csv",sep=";",index=False)

conn.close()
