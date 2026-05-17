"""
Generates realistic e-commerce sample CSV data for the masterclass.
Run: python data/generate_data.py
"""

import csv
import random
from datetime import date, timedelta

random.seed(42)

BASE = "data/raw"

# ── helpers ──────────────────────────────────────────────────────────────────

def rand_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def write_csv(filename, rows, headers):
    with open(f"{BASE}/{filename}", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  {filename}: {len(rows)} rows")

# ── customers (200) ──────────────────────────────────────────────────────────

FIRST_NAMES = ["Alice","Bob","Carol","David","Eva","Frank","Grace","Hank",
               "Iris","Jake","Karen","Leo","Mia","Nate","Olivia","Paul",
               "Quinn","Rachel","Sam","Tina","Uma","Victor","Wendy","Xander",
               "Yara","Zoe"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller",
               "Davis","Wilson","Taylor","Anderson","Thomas","Jackson","White",
               "Harris","Martin","Thompson","Moore","Young","Allen"]
CHANNELS    = ["web","mobile","in-store","phone"]
CITIES_STATES = [
    ("New York","NY"),("Los Angeles","CA"),("Chicago","IL"),("Houston","TX"),
    ("Phoenix","AZ"),("Philadelphia","PA"),("San Antonio","TX"),("San Diego","CA"),
    ("Dallas","TX"),("San Jose","CA"),("Austin","TX"),("Jacksonville","FL"),
    ("Seattle","WA"),("Denver","CO"),("Boston","MA"),("Nashville","TN"),
    ("Portland","OR"),("Atlanta","GA"),("Miami","FL"),("Minneapolis","MN"),
]

customers = []
for i in range(1, 201):
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    city, state = random.choice(CITIES_STATES)
    customers.append({
        "customer_id":  i,
        "first_name":   fn,
        "last_name":    ln,
        "email":        f"{fn.lower()}.{ln.lower()}{i}@email.com",
        "phone":        f"{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        "address":      f"{random.randint(1,9999)} {random.choice(['Main','Oak','Pine','Elm','Maple'])} St",
        "city":         city,
        "state":        state,
        "country":      "US",
        "channel":      random.choice(CHANNELS),
    })

write_csv("customers.csv", customers,
    ["customer_id","first_name","last_name","email","phone","address","city","state","country","channel"])

# ── products (50) ─────────────────────────────────────────────────────────────

CATALOG = [
    ("Electronics","Laptops",     [("ProBook 14","PB14","TechBrand",899.99,500.00),
                                   ("UltraSlim 15","UL15","TechBrand",1199.99,680.00),
                                   ("WorkStation X","WSX","CompuMark",1599.99,900.00)]),
    ("Electronics","Smartphones", [("Pixel A","PXA","TechBrand",699.99,320.00),
                                   ("Nova S","NVS","GadgetCo",549.99,240.00),
                                   ("Budget B1","BB1","EcoPhone",299.99,120.00)]),
    ("Electronics","Accessories", [("USB-C Hub","USBH","GadgetCo",49.99,18.00),
                                   ("Wireless Mouse","WMSE","TechBrand",29.99,10.00),
                                   ("Bluetooth Earbuds","BTER","SoundMax",79.99,28.00),
                                   ("Laptop Stand","LPST","ErgoCo",39.99,14.00)]),
    ("Clothing","Mens",           [("Classic Tee","MCTEE","StyleCo",19.99,6.00),
                                   ("Slim Jeans","MSJN","DenimCo",59.99,22.00),
                                   ("Hoodie XL","MHXL","StyleCo",49.99,18.00),
                                   ("Polo Shirt","MPOLO","StyleCo",34.99,12.00)]),
    ("Clothing","Womens",         [("Summer Dress","WSDRS","FashionX",49.99,16.00),
                                   ("Yoga Pants","WYOGA","ActiveWear",39.99,14.00),
                                   ("Blouse Classic","WBLSE","FashionX",34.99,11.00),
                                   ("Winter Coat","WCOAT","WarmCo",129.99,55.00)]),
    ("Clothing","Kids",           [("Kids Tee Pack","KTEE","StyleCo",14.99,5.00),
                                   ("Kids Jeans","KJNS","DenimCo",29.99,10.00)]),
    ("Home & Kitchen","Cookware", [("Non-Stick Pan","NSPN","ChefLine",34.99,12.00),
                                   ("Knife Set 6pc","KS6P","ChefLine",59.99,22.00),
                                   ("Cast Iron Skillet","CISK","HeavyDuty",49.99,20.00)]),
    ("Home & Kitchen","Appliances",[("Air Fryer 5L","AF5L","HomeAppl",89.99,38.00),
                                    ("Coffee Maker","CFMK","HomeAppl",59.99,22.00),
                                    ("Blender Pro","BLNP","HomeAppl",79.99,30.00)]),
    ("Home & Kitchen","Decor",    [("Throw Pillow Set","TPLS","HomeDecor",24.99,8.00),
                                   ("Canvas Print","CVSP","ArtHouse",39.99,14.00),
                                   ("Table Lamp","TLMP","HomeDecor",44.99,16.00)]),
    ("Sports","Fitness",          [("Yoga Mat","YGMT","FitGear",29.99,10.00),
                                   ("Resistance Bands","RSBN","FitGear",19.99,6.00),
                                   ("Jump Rope","JMRP","FitGear",14.99,4.00),
                                   ("Dumbbell 10lb","DB10","IronMax",24.99,9.00)]),
    ("Sports","Outdoor",          [("Hiking Backpack","HKBP","TrailCo",89.99,35.00),
                                   ("Water Bottle 32oz","WB32","TrailCo",24.99,8.00),
                                   ("Camping Tent 2P","CT2P","WildCamp",149.99,65.00)]),
    ("Books","Non-Fiction",       [("The Data Mindset","TDMB","PressA",24.99,8.00),
                                   ("Cloud Native","CLNB","TechPress",34.99,12.00)]),
    ("Books","Fiction",           [("The Lost Signal","TLSB","PressA",14.99,5.00),
                                   ("Desert Storm","DSTB","PressA",12.99,4.00)]),
]

products = []
pid = 1
for category, subcategory, items in CATALOG:
    for name, sku, brand, unit_price, cost_price in items:
        products.append({
            "product_id":   pid,
            "sku":          sku,
            "product_name": name,
            "category":     category,
            "subcategory":  subcategory,
            "brand":        brand,
            "unit_price":   unit_price,
            "cost_price":   cost_price,
        })
        pid += 1

write_csv("products.csv", products,
    ["product_id","sku","product_name","category","subcategory","brand","unit_price","cost_price"])

# ── locations (30) ──────────────────────────────────────────────────────────

REGIONS = {
    "NY":"Northeast","PA":"Northeast","MA":"Northeast","NJ":"Northeast",
    "CA":"West","WA":"West","OR":"West","AZ":"West",
    "TX":"South","FL":"South","GA":"South","TN":"South","NC":"South",
    "IL":"Midwest","OH":"Midwest","MI":"Midwest","MN":"Midwest","CO":"Midwest",
}

STORE_CITIES = [
    ("New York","NY","10001"),("Brooklyn","NY","11201"),("Philadelphia","PA","19101"),
    ("Boston","MA","02101"),("Los Angeles","CA","90001"),("San Francisco","CA","94101"),
    ("San Diego","CA","92101"),("Seattle","WA","98101"),("Portland","OR","97201"),
    ("Phoenix","AZ","85001"),("Houston","TX","77001"),("Dallas","TX","75201"),
    ("Austin","TX","78701"),("San Antonio","TX","78201"),("Miami","FL","33101"),
    ("Orlando","FL","32801"),("Atlanta","GA","30301"),("Nashville","TN","37201"),
    ("Chicago","IL","60601"),("Columbus","OH","43201"),("Detroit","MI","48201"),
    ("Minneapolis","MN","55401"),("Denver","CO","80201"),("Charlotte","NC","28201"),
    ("Raleigh","NC","27601"),("Las Vegas","NV","89101"),("Salt Lake City","UT","84101"),
    ("Kansas City","MO","64101"),("St. Louis","MO","63101"),("New Orleans","LA","70112"),
]

locations = []
for i, (city, state, postal) in enumerate(STORE_CITIES, start=1):
    locations.append({
        "location_id":  i,
        "city":         city,
        "state":        state,
        "country":      "US",
        "region":       REGIONS.get(state, "Other"),
        "postal_code":  postal,
    })

write_csv("locations.csv", locations,
    ["location_id","city","state","country","region","postal_code"])

# ── orders (2000 order headers → line items) ─────────────────────────────────

START = date(2022, 1, 1)
END   = date(2024, 12, 31)

orders      = []
order_line_id = 1

for order_id in range(1, 2001):
    customer    = random.choice(customers)
    location    = random.choice(locations)
    order_date  = rand_date(START, END)
    ship_date   = order_date + timedelta(days=random.randint(1, 10))
    order_number = f"ORD-{order_id:06d}"
    num_lines   = random.randint(1, 4)

    for _ in range(num_lines):
        product  = random.choice(products)
        qty      = random.randint(1, 5)
        discount = round(random.choice([0, 0, 0, 5, 10, 15, 20]), 2)

        orders.append({
            "order_id":       order_id,
            "line_item_id":   order_line_id,
            "order_number":   order_number,
            "customer_id":    customer["customer_id"],
            "product_id":     product["product_id"],
            "location_id":    location["location_id"],
            "order_date":     order_date.isoformat(),
            "ship_date":      ship_date.isoformat(),
            "quantity":       qty,
            "unit_price":     product["unit_price"],
            "discount_amount": discount,
        })
        order_line_id += 1

write_csv("orders.csv", orders,
    ["order_id","line_item_id","order_number","customer_id","product_id",
     "location_id","order_date","ship_date","quantity","unit_price","discount_amount"])

# ── returns (300) ────────────────────────────────────────────────────────────

RETURN_REASONS = [
    "Defective product","Wrong item received","Changed mind",
    "Better price found","Damaged in shipping","Not as described",
]

sampled_orders = random.sample(orders, 300)
returns = []
for i, o in enumerate(sampled_orders, start=1):
    ret_date = date.fromisoformat(o["ship_date"]) + timedelta(days=random.randint(3, 30))
    returns.append({
        "return_id":          i,
        "order_id":           o["order_id"],
        "order_number":       o["order_number"],
        "customer_id":        o["customer_id"],
        "product_id":         o["product_id"],
        "return_date":        ret_date.isoformat(),
        "quantity_returned":  random.randint(1, o["quantity"]),
        "unit_price":         o["unit_price"],
        "return_reason":      random.choice(RETURN_REASONS),
    })

write_csv("returns.csv", returns,
    ["return_id","order_id","order_number","customer_id","product_id",
     "return_date","quantity_returned","unit_price","return_reason"])

print("\nAll CSV files generated in data/raw/")
