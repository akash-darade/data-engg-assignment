import sqlite3
import csv

conn=sqlite3.connect("Data Engineer_ETL Assignment.db")
cursor=conn.cursor()

query="""
        select c.customer_id as customer,
        c.age as age,
        i.item_name as item,
        sum(o.quantity) as quantity
        from customer c
        join sales s on customer_id =s.customer_id
        join orders o on s.sales_id=o.sales_id
        join items i on o.item_id=i.item_id
        where c.age between 18 and 35
        group by c.customer_id , c.age ,i.item_name
        having quantity > 0
        """
cursor.execute(query)
rows=cursor.fetchall()

with open ("output_sql.csv","w",newline="")as fp:
  writer=csv.writer(fp,delemiter=";")
  writer.writerow(["customer","age","item",quantity"])
  writer.writerows(rows)
                   
conn.close()
  
