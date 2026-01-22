import os
import requests
from dotenv import load_dotenv

load_dotenv()

def refresh_mural_token():
    client_id = os.environ.get("MURAL_CLIENT_ID")
    client_secret = os.environ.get("MURAL_CLIENT_SECRET")
    refresh_token = os.environ.get("MURAL_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        print("❌ Missing Mural credentials in .env")
        return

    url = "https://app.mural.co/api/public/v1/authorization/oauth2/token"
    
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    print("Refreshing Mural token...")
    try:
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            new_refresh_token = token_data.get("refresh_token")
            
            print("✅ Token refreshed successfully!")
            print(f"New Access Token: {access_token[:20]}...")
            
            # Update .env file
            with open(".env", "r") as f:
                lines = f.readlines()
            
            with open(".env", "w") as f:
                for line in lines:
                    if line.startswith("MURAL_ACCESS_TOKEN="):
                        f.write(f"MURAL_ACCESS_TOKEN={access_token}\n")
                    elif line.startswith("MURAL_REFRESH_TOKEN=") and new_refresh_token:
                        f.write(f"MURAL_REFRESH_TOKEN={new_refresh_token}\n")
                    else:
                        f.write(line)
            
            print("✅ .env file updated.")
        else:
            print(f"❌ Refresh failed: {response.status_code}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    refresh_mural_token()
