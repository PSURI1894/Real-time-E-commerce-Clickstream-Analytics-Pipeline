import time
import random
import requests
import threading
import uuid

COLLECTOR_URL = "http://localhost:8000/collect"
CATEGORIES = ["electronics", "apparel", "home", "books", "sports"]
PRODUCTS = {
    "electronics": ["prod_phone12", "prod_laptop_pro", "prod_headphones_noise"],
    "apparel": ["prod_tshirt_black", "prod_jeans_slim", "prod_jacket_winter"],
    "home": ["prod_coffee_maker", "prod_desk_lamp", "prod_sheets_cotton"],
    "books": ["prod_novel_thriller", "prod_tech_spark", "prod_cooking_easy"],
    "sports": ["prod_yoga_mat", "prod_dumbbell_set", "prod_water_bottle"]
}

def simulate_user_journey(user_id):
    session_id = f"sess_{uuid.uuid4().hex[:10]}"
    ip_address = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
    user_agent = random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/605.1.15"
    ])
    
    # 5% chance of being classified as a bot
    is_bot = random.random() < 0.05
    if is_bot:
        user_agent = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    
    steps = random.randint(3, 15) if not is_bot else 40
    current_category = random.choice(CATEGORIES)
    
    for step in range(steps):
        event_type = "page_view"
        page_url = f"https://www.e-shop.com/{current_category}"
        payload = {}
        
        # Behavior decision state machine
        roll = random.random()
        if step > 0:
            if roll < 0.3:
                event_type = "search"
                page_url = "https://www.e-shop.com/search"
                payload = {"search_query": random.choice(["deals", "best-seller", "new-arrivals"])}
            elif roll < 0.7:
                event_type = "page_view"
                prod = random.choice(PRODUCTS[current_category])
                page_url = f"https://www.e-shop.com/product/{prod}"
                payload = {"sku": prod, "category": current_category}
            elif roll < 0.85:
                event_type = "add_to_cart"
                prod = random.choice(PRODUCTS[current_category])
                page_url = "https://www.e-shop.com/cart"
                payload = {"sku": prod, "price": str(random.randint(10, 500)), "quantity": "1"}
            elif roll < 0.95:
                event_type = "checkout"
                page_url = "https://www.e-shop.com/checkout"
                payload = {"cart_value": str(random.randint(50, 1500))}
        
        event = {
            "user_id": user_id,
            "session_id": session_id,
            "event_type": event_type,
            "client_timestamp": int(time.time() * 1000),
            "ip_address": ip_address,
            "user_agent": user_agent,
            "page_url": page_url,
            "payload": payload
        }
        
        try:
            resp = requests.post(COLLECTOR_URL, json=event, timeout=2)
            if resp.status_code == 202:
                print(f"[{user_id}] Emitted {event_type} for {page_url}")
        except Exception as e:
            print(f"Error emitting event: {e}")
            
        # Simulating random delay between actions
        time.sleep(random.uniform(0.5, 4.0) if not is_bot else 0.05)

def run_simulation(users_count=10):
    threads = []
    for i in range(users_count):
        user_id = f"usr_{random.randint(10000, 99999)}"
        t = threading.Thread(target=simulate_user_journey, args=(user_id,))
        threads.append(t)
        t.start()
        time.sleep(random.uniform(0.1, 0.5))
        
    for t in threads:
        t.join()

if __name__ == "__main__":
    print("Starting e-commerce traffic simulator...")
    while True:
        run_simulation(random.randint(2, 5))
        time.sleep(2)
