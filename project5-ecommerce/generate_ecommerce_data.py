#!/usr/bin/env python3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Products + categories
categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
products = ['iPhone 15', 'Nike Air', 'Python Book', 'IKEA Lamp', 'Adidas Shoes']

np.random.seed(42)
start_date = datetime(2026, 3, 1)
n_orders = 1440000

print("Generating {:,} e-commerce orders...".format(n_orders))

data = []
order_id = 0
for i in range(n_orders):
    order_date = start_date + timedelta(days=i//48000, hours=(i%48000)//2000, minutes=(i%2000)//60)
    
    # Customer behavior
    customer_id = "CUST_{:06d}".format(random.randint(1, 50000))
    product = random.choice(products)
    category = random.choice(categories)
    qty = random.choices([1, 2, 3, 5, 10], weights=[0.5, 0.3, 0.15, 0.04, 0.01])[0]
    
    # Dynamic pricing
    base_price = random.uniform(10, 800)
    discount = random.uniform(0, 0.3)
    price = base_price * (1 - discount)
    
    # Anomalies (2%)
    if random.random() < 0.02:
        qty *= random.choice([5, 10])  # Bulk fraud
        discount = random.choice([0.8, 0.9])  # Coupon abuse
        price *= 0.1  # Refund fraud
    
    total = price * qty
    
    order_id += 1
    data.append({
        'order_id': order_id,
        'timestamp': order_date.strftime('%Y-%m-%d %H:%M:%S'),
        'customer_id': customer_id,
        'product': product,
        'category': category,
        'quantity': qty,
        'unit_price': round(price, 2),
        'discount_pct': round(discount * 100, 1),
        'total_amount': round(total, 2)
    })

df = pd.DataFrame(data)
df.to_csv('ecommerce_orders_raw.csv', index=False)
print("✅ Saved: ecommerce_orders_raw.csv ({:,} orders, {:.0f}MB)".format(
    len(df), df.memory_usage(deep=True).sum() / 1e6))

print("\n📊 Revenue Stats:")
print(df['total_amount'].describe())
print("\n🏆 Top Categories:")
print(df.groupby('category')['total_amount'].sum().sort_values(ascending=False))
