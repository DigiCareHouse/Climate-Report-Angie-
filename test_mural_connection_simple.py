import os
import requests
from dotenv import load_dotenv

load_dotenv()

def test_mural_connection():
    access_token = os.environ.get("MURAL_ACCESS_TOKEN")
    if not access_token:
        print("❌ MURAL_ACCESS_TOKEN not found in .env")
        return

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    url = "https://app.mural.co/api/public/v1/identity"
    
    print(f"Testing Mural connection with token: {access_token[:20]}...")
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("✅ Mural connection successful!")
            print(f"User Info: {response.json()}")
        else:
            print(f"❌ Mural connection failed: {response.status_code}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_mural_connection()
