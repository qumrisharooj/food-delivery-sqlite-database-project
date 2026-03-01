import sqlite3
import random
from faker import Faker
import pandas as pd
#---------->  Initialize Faker
fake = Faker()

#---------->  Connect to SQLite database
conn = sqlite3.connect("food_delivery.db")
cursor = conn.cursor()

#---------->  Enable foreign key support
cursor.execute("PRAGMA foreign_keys = ON;")

#---------------------------------------
#     Create Customers Table
#---------------------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    full_name TEXT NOT NULL,
    email TEXT UNIQUE,
    gender TEXT CHECK (gender IN ('Male', 'Female', 'Other')),
    age INTEGER CHECK (age >= 18),
    city TEXT
);
""")

#---------------------------------------
#   Create Restaurants Table
#---------------------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS restaurants (
    restaurant_id INTEGER PRIMARY KEY AUTOINCREMENT,
    restaurant_name TEXT NOT NULL,
    cuisine_type TEXT,
    rating REAL CHECK (rating BETWEEN 1 AND 5),
    city TEXT
);
""")

#---------------------------------------
#  Create Orders Table (1000+ rows)
#---------------------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER,
    restaurant_id INTEGER,
    order_status INTEGER CHECK (order_status BETWEEN 1 AND 4),
    order_total_price REAL CHECK (order_total_price >= 0),
    delivery_time_minutes INTEGER,
    order_date TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(restaurant_id)
);
""")

#---------------------------------------
#  Create Order Items Table (Composite Key)
#---------------------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS order_items (
    order_id INTEGER,
    item_name TEXT,
    quantity INTEGER CHECK (quantity > 0),
    price REAL CHECK (price >= 0),
    PRIMARY KEY (order_id, item_name),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
""")

#---------------------------------------
#  Insert Customers
#---------------------------------------
for _ in range(300):
    cursor.execute("""
        INSERT INTO customers (full_name, email, gender, age, city)
        VALUES (?, ?, ?, ?, ?)
    """, (
        fake.name(),
        fake.email() if random.random() > 0.1 else None,  #---------->  deliberate missing data
        random.choice(['Male', 'Female', 'Other']),
        random.randint(18, 70),
        fake.city()
    ))

#---------------------------------------
#  Insert Restaurants
#---------------------------------------
cuisines = ['Italian', 'Indian', 'Chinese', 'Mexican', 'Thai', 'American']

for _ in range(50):
    cursor.execute("""
        INSERT INTO restaurants (restaurant_name, cuisine_type, rating, city)
        VALUES (?, ?, ?, ?)
    """, (
        fake.company(),
        random.choice(cuisines),
        round(random.uniform(1, 5), 1),
        fake.city()
    ))

#---------------------------------------
#  Insert Orders (1200 rows)
#---------------------------------------
for _ in range(1200):
    cursor.execute("""
        INSERT INTO orders (
            customer_id,
            restaurant_id,
            order_status,
            order_total_price,
            delivery_time_minutes,
            order_date
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        random.randint(1, 300),
        random.randint(1, 50),
        random.randint(1, 4),  #---------->  Ordinal data
        round(random.uniform(5, 100), 2),  #---------->  Ratio data
        random.randint(15, 90),  #---------->  Interval data
        fake.date_this_year()
    ))

#---------------------------------------
#  Insert Order Items
#---------------------------------------
menu_items = ['Burger', 'Pizza', 'Pasta', 'Noodles', 'Salad', 'Tacos']

for order_id in range(1, 1201):
    for _ in range(random.randint(1, 4)):
        cursor.execute("""
            INSERT OR IGNORE INTO order_items (
                order_id, item_name, quantity, price
            )
            VALUES (?, ?, ?, ?)
        """, (
            order_id,
            random.choice(menu_items),
            random.randint(1, 5),
            round(random.uniform(3, 20), 2)
        ))

#---------->  Commit changes and close connection
conn.commit()
conn.close()

print("SQLite database 'food_delivery.db' created successfully.")





#================================================================================
#              READING DATA FROM THE DATABASE INTO PANDAS DATAFRAMES
#================================================================================

#---------->  Connect to the database
conn = sqlite3.connect("food_delivery.db")

#---------->  Read full tables into DataFrames
customers_df = pd.read_sql("SELECT * FROM customers;", conn)
restaurants_df = pd.read_sql("SELECT * FROM restaurants;", conn)
orders_df = pd.read_sql("SELECT * FROM orders;", conn)
order_items_df = pd.read_sql("SELECT * FROM order_items;", conn)

#---------->  Preview the data
print(customers_df.head())
print(orders_df.head())

conn.close()