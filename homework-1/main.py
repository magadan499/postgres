"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv


connections = {
    "host": "localhost",
    "database": "north",
    "user": "postgres",
    "password": "40219828"
}

with psycopg2.connect(**connections) as conn:
    with open('north_data/customers_data.csv', 'r', newline='') as file:
        file_reader = csv.reader(file)
        next(file_reader)
        with conn.cursor() as cur:
            for row in file_reader:
                cur.execute("INSERT INTO customers"
                            "(customer_id, company_name, contact_name) "
                            "VALUES (%s, %s, %s)", row)
            cur.execute("SELECT * FROM customers")
            rows = cur.fetchall()


with psycopg2.connect(**connections) as conn:
    with conn.cursor() as cur:
        with open('north_data/employees_data.csv', 'r', newline='') as file:
            file_reader = csv.reader(file)
            next(file_reader)
            with conn.cursor() as cur:
                for row in file_reader:
                    cur.execute("INSERT INTO employees"
                                "(employee_id, first_name, last_name, "
                                "title, birth_date, notes) "
                                "VALUES (%s, %s, %s, %s, %s, %s)", row)
                cur.execute("SELECT * FROM employees")
                rows = cur.fetchall()


with psycopg2.connect(**connections) as conn:
    with open('north_data/orders_data.csv', 'r', newline='') as file:
        file_reader = csv.reader(file)
        next(file_reader)
        with conn.cursor() as cur:
            for row in file_reader:
                cur.execute("INSERT INTO orders"
                            "(order_id, customer_id, employee_id,"
                            "order_date, ship_city) "
                            "VALUES (%s, %s, %s, %s, %s)", row)
            cur.execute("SELECT * FROM orders")
            rows = cur.fetchall()
