import csv
import os
import psycopg2
from psycopg2 import sql

FILES = ["customers.csv", "products.csv", "orders.csv"]
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def column_type(name, rows):
    values = [row[name] for row in rows]

    if name.endswith("_timestamp"):
        return "TIMESTAMP"
    if all(value.isdigit() for value in values):
        return "INTEGER"
    try:
        for value in values:
            float(value)
        return "NUMERIC(14,2)"
    except ValueError:
        return "TEXT"


def main():
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="shop",
        password="shop123",
        dbname="shopdb",
    )

    cur = conn.cursor()

    for filename in FILES:
        table = filename.replace(".csv", "")
        
        # Reads three CSV files.
        with open(os.path.join(DATA_DIR, filename), newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames
            rows = list(reader)

        # Creates PostgreSQL tables dynamically.
        definitions = [
            sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(column_type(col, rows)))
            for col in columns
        ]
        create_table = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY ({}))").format(
            sql.Identifier(table),
            sql.SQL(", ").join(definitions),
            sql.Identifier(columns[0]),
        )
        cur.execute(create_table)

        # Inserts data into PostgreSQL.
        insert = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(col) for col in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )
        for row in rows:
            cur.execute(insert, [row[col] for col in columns])

        print(f"{table}: processed {len(rows)} records from {filename}")

    conn.commit()
    cur.close()
    conn.close()
    print("Task 1 finished.")


if __name__ == "__main__":
    main()
