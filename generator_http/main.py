# services/generator_http/generator_http.py
import os, time, uuid, random, requests

API_URL = os.getenv("API_URL", "http://order-api:8000/order")
INTERVAL = float(os.getenv("GENERATOR_INTERVAL", "2"))

def generate_order():
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": f"user-{random.randint(1, 100)}",
        "items": random.sample(["apple", "banana", "carrot", "pear"], 2),
        "total": round(random.uniform(10, 100), 2),
    }

if __name__ == "__main__":
    print(f"🚀 HTTP generator targeting {API_URL} every {INTERVAL}s")
    while True:
        order = generate_order()
        try:
            r = requests.post(API_URL, json=order)
            print(f"[{r.status_code}] {order}")
        except Exception as e:
            print(f"❌ Failed: {e}")
        time.sleep(INTERVAL)
