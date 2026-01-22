# get_full_tokens.py
import requests
import json
from flask import Flask, request
import threading
import webbrowser
import time
from urllib.parse import quote

# === YOUR CREDENTIALS ===
CLIENT_ID = "195c0a83-eac5-4fd4-b694-70a62a13da62"
CLIENT_SECRET = "3f256d04bc6c1dba96ecfea1cdd68287b0fc483d85070b7bcf5b2e0a8d9fe9fc9ceb8d12344da2cc392d2864c0b10cb82af813345da1c1a2ae39022307318f32"
REDIRECT_URI = "http://localhost:5000/callback"

app = Flask(__name__)
access_token = None
refresh_token = None
token_received = threading.Event()


@app.route('/callback')
def oauth_callback():
    global access_token, refresh_token

    auth_code = request.args.get('code')

    if auth_code:
        print(f"✅ Got auth code: {auth_code}")

        # Exchange code for tokens
        token_url = "https://app.mural.co/api/public/v1/authorization/oauth2/token"
        data = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI,
            "code": auth_code,
            "grant_type": "authorization_code"
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        response = requests.post(token_url, headers=headers, data=data)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data["access_token"]
            refresh_token = token_data.get("refresh_token")

            print("\n" + "=" * 80)
            print("🎉 COMPLETE ACCESS TOKEN:")
            print("=" * 80)
            print(access_token)
            print("=" * 80)

            print(f"\n📏 Token length: {len(access_token)} characters")

            if refresh_token:
                print("\n" + "=" * 80)
                print("🔄 COMPLETE REFRESH TOKEN:")
                print("=" * 80)
                print(refresh_token)
                print("=" * 80)
                print(f"📏 Refresh token length: {len(refresh_token)} characters")

            # Save to file
            with open('FULL_TOKENS.txt', 'w') as f:
                f.write("=" * 80 + "\n")
                f.write("ACCESS TOKEN:\n")
                f.write("=" * 80 + "\n")
                f.write(access_token + "\n\n")
                if refresh_token:
                    f.write("=" * 80 + "\n")
                    f.write("REFRESH TOKEN:\n")
                    f.write("=" * 80 + "\n")
                    f.write(refresh_token + "\n")

            print(f"\n💾 Full tokens saved to: FULL_TOKENS.txt")

            token_received.set()

            return """
            <h1>✅ Success!</h1>
            <p>Tokens obtained. Check your console for complete tokens.</p>
            <p>You can close this window.</p>
            """

    token_received.set()
    return "<h1>Done</h1>"


@app.route('/')
def home():
    return "OAuth server running"


def get_tokens():
    print("🚀 Getting Mural tokens...")

    # Start Flask
    flask_thread = threading.Thread(
        target=lambda: app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False),
        daemon=True
    )
    flask_thread.start()
    time.sleep(2)

    # Open auth URL
    scope = "rooms:read workspaces:read murals:read identity:read"
    auth_url = (
        f"https://app.mural.co/api/public/v1/authorization/oauth2/?"
        f"client_id={CLIENT_ID}&"
        f"redirect_uri={quote(REDIRECT_URI, safe='')}&"
        f"scope={quote(scope, safe='')}&"
        f"response_type=code"
    )

    print(f"\n🌐 Opening: {auth_url}")
    webbrowser.open(auth_url)

    print("\n⏳ Waiting for you to authorize in browser...")
    print("1. Login to Mural if needed")
    print("2. Click 'Authorize'")
    print("3. Wait for redirect...")

    token_received.wait(timeout=120)

    if access_token:
        print("\n✅ Token retrieval complete!")
        print(f"\n📋 Access token saved to FULL_TOKENS.txt")
    else:
        print("\n❌ Failed to get token")


if __name__ == "__main__":
    get_tokens()