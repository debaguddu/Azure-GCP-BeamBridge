import csv
import random
from datetime import datetime, timedelta

def generate_datasets():
    # 1. SMALL: Categories (100 rows)
    print("Generating categories.csv (100 rows)...")
    with open('categories.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['category_id', 'category_name', 'department'])
        departments = ['Electronics', 'Clothing', 'Home', 'Toys', 'Sports']
        for i in range(1, 101):
            writer.writerow([i, f'Category_{i}', random.choice(departments)])

    # 2. MEDIUM: Customers (50,000 rows)
    print("Generating customers.csv (50,000 rows)...")
    with open('customers.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['customer_id', 'first_name', 'last_name', 'email', 'signup_date', 'city'])
        cities = ['New York', 'London', 'Bengaluru', 'Tokyo', 'Sydney', 'Paris']
        start_date = datetime(2020, 1, 1)
        for i in range(1, 50001):
            signup = start_date + timedelta(days=random.randint(0, 1500))
            writer.writerow([i, f'User_{i}', f'Last_{i}', f'user{i}@example.com', signup.strftime('%Y-%m-%d'), random.choice(cities)])

    # 3. LARGE: Orders (1,000,000 rows)
    print("Generating orders.csv (1,000,000 rows)...")
    with open('orders.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['order_id', 'customer_id', 'store_id', 'order_date', 'total_amount', 'status'])
        statuses = ['COMPLETED', 'PENDING', 'SHIPPED', 'CANCELLED']
        order_start = datetime(2022, 1, 1)
        for i in range(1, 1000001):
            cust_id = random.randint(1, 50000) # Enforces referential integrity
            order_date = order_start + timedelta(minutes=random.randint(0, 2000000))
            writer.writerow([i, cust_id, random.randint(1, 20), order_date.strftime('%Y-%m-%d %H:%M:%S'), round(random.uniform(10, 500), 2), random.choice(statuses)])

    # 4. EXTRA LARGE: Order Items (5,000,000 rows)
    print("Generating order_items.csv (5,000,000 rows)...")
    with open('order_items.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['order_item_id', 'order_id', 'product_id', 'quantity', 'unit_price', 'discount'])
        for i in range(1, 5000001):
            order_id = random.randint(1, 1000000) # Enforces referential integrity
            qty = random.randint(1, 5)
            price = round(random.uniform(5, 100), 2)
            discount = round(random.uniform(0, 10), 2)
            writer.writerow([i, order_id, random.randint(1, 1000), qty, price, discount])

    print("All 4 CSV files generated successfully!")

if __name__ == "__main__":
    generate_datasets()