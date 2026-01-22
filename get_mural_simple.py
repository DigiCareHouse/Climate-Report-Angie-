# get_mural_simple.py
import requests
import base64
import json
from flask import Flask, request
import threading
import webbrowser
import time
import sys
import os
from urllib.parse import quote

# === YOUR CREDENTIALS ===
CLIENT_ID = "195c0a83-eac5-4fd4-b694-70a62a13da62"
CLIENT_SECRET = "3f256d04bc6c1dba96ecfea1cdd68287b0fc483d85070b7bcf5b2e0a8d9fe9fc9ceb8d12344da2cc392d2864c0b10cb82af813345da1c1a2ae39022307318f32"
REDIRECT_URI = "http://localhost:5000/callback"

app = Flask(__name__)
access_token = None
token_received = threading.Event()
flask_thread = None
server_started = False

# Clear any existing access token at startup
access_token = None


def get_mural_token_with_auth_code():
    """Get token using authorization code flow"""
    global access_token

    print("🔑 Starting OAuth flow...")
    print(f"📋 Using Redirect URI: {REDIRECT_URI}")

    # Use the EXACT scope names from Mural documentation
    scope = "rooms:read workspaces:read murals:read identity:read"
    print(f"🔐 Using Mural scopes: {scope}")

    # Properly encode all parameters
    auth_url = (
        f"https://app.mural.co/api/public/v1/authorization/oauth2/?"
        f"client_id={CLIENT_ID}&"
        f"redirect_uri={quote(REDIRECT_URI, safe='')}&"
        f"scope={quote(scope, safe='')}&"
        f"response_type=code"
    )

    print(f"🌐 Opening browser for Mural authorization...")

    # Try to open browser automatically
    try:
        webbrowser.open(auth_url)
    except Exception as e:
        print(f"⚠️  Could not open browser automatically: {e}")
        print(f"🔗 Please open this URL manually: {auth_url}")

    # Wait for the callback
    print("⏳ Waiting for authorization... (This will timeout after 120 seconds)")

    # Wait for token with timeout
    if token_received.wait(timeout=120):
        print("✅ Authorization successful!")
        return access_token
    else:
        print("❌ Authorization timeout - no response received within 120 seconds")
        return None


@app.route('/callback')
def oauth_callback():
    """Handle OAuth callback from Mural"""
    global access_token

    print("=" * 50)
    print("📥 Received callback from Mural!")
    print(f"🔍 Query parameters: {dict(request.args)}")
    print("=" * 50)

    # Get authorization code from query parameters
    auth_code = request.args.get('code')
    error = request.args.get('error')
    error_description = request.args.get('error_description', 'Unknown error')

    if error:
        print(f"❌ OAuth Error: {error}")
        print(f"📝 Error description: {error_description}")
        return f"<h1>Authorization Failed</h1><p>Error: {error}</p><p>Description: {error_description}</p>"

    if auth_code:
        print(f"✅ Received authorization code: {auth_code[:20]}...")

        # Exchange authorization code for access token
        token_url = "https://app.mural.co/api/public/v1/authorization/oauth2/token"

        # Prepare the data as form-urlencoded
        data = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI,
            "code": auth_code,
            "grant_type": "authorization_code"
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        try:
            print("🔄 Exchanging authorization code for access token...")
            response = requests.post(token_url, headers=headers, data=data)

            print(f"📡 Token response status: {response.status_code}")

            if response.status_code == 200:
                token_data = response.json()
                access_token = token_data["access_token"]
                refresh_token = token_data.get("refresh_token")
                expires_in = token_data.get("expires_in", "Unknown")

                print("🎉 Successfully obtained access token!")
                print(f"   Access Token: {access_token[:50]}...")
                if refresh_token:
                    print(f"   Refresh Token: {refresh_token[:50]}...")
                print(f"   Expires in: {expires_in} seconds")

                # Signal that we have the token
                token_received.set()

                return """
                <!DOCTYPE html>
                <html>
                <head>
                    <title>✅ Authorization Successful</title>
                    <style>
                        body { font-family: Arial, sans-serif; margin: 40px; background: #f0f8f0; }
                        .success { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                    </style>
                </head>
                <body>
                    <div class="success">
                        <h1>✅ Authorization Successful!</h1>
                        <p>You can close this window and return to the application.</p>
                        <p>The script will continue automatically...</p>
                    </div>
                    <script>
                        setTimeout(function() { 
                            window.close(); 
                        }, 1000);
                    </script>
                </body>
                </html>
                """
            else:
                print(f"❌ Token exchange failed: {response.status_code}")
                print(f"   Response: {response.text}")
                token_received.set()  # Still signal to continue
                return f"<h1>Token Exchange Failed</h1><p>Status: {response.status_code}</p><p>Response: {response.text}</p>"

        except Exception as e:
            print(f"❌ Error during token exchange: {e}")
            token_received.set()  # Still signal to continue
            return f"<h1>Error</h1><p>{str(e)}</p>"
    else:
        print("❌ No authorization code received in callback")
        token_received.set()  # Still signal to continue
        return "<h1>Error</h1><p>No authorization code received</p>"


@app.route('/')
def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Mural OAuth Server</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .info { background: #f0f0f0; padding: 20px; border-radius: 10px; }
        </style>
    </head>
    <body>
        <div class="info">
            <h1>Mural OAuth Callback Server</h1>
            <p>Server is running and ready for OAuth callbacks.</p>
            <p>Endpoint: <code>/callback</code></p>
            <p>This window can be closed after authorization is complete.</p>
        </div>
    </body>
    </html>
    """


def test_mural_api(token):
    """Test the Mural API with the obtained token"""
    print("\n" + "=" * 60)
    print("🚀 Testing Mural API with obtained token...")
    print("=" * 60)

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    all_data = {}

    # Test workspaces endpoint
    print("\n🔍 Testing endpoint: https://app.mural.co/api/public/v1/workspaces")
    try:
        response = requests.get("https://app.mural.co/api/public/v1/workspaces", headers=headers)
        print(f"   Status: {response.status_code}")

        if response.status_code == 200:
            workspaces = response.json()
            print(f"✅ Success! Found {len(workspaces)} workspace(s)")

            # Display workspaces
            if workspaces and len(workspaces) > 0:
                for i, workspace in enumerate(workspaces[:5]):  # Show first 5 workspaces
                    workspace_name = workspace.get('name', 'Unknown')
                    workspace_id = workspace.get('id', 'Unknown')
                    print(f"   📁 {i + 1}. {workspace_name} (ID: {workspace_id[:8]}...)")

            all_data['workspaces'] = workspaces

            # If we have workspaces, try to get rooms and murals
            if workspaces and len(workspaces) > 0:
                for i, workspace in enumerate(workspaces[:2]):  # Try first 2 workspaces
                    workspace_id = workspace.get('id')
                    workspace_name = workspace.get('name', f'Workspace {i + 1}')

                    print(f"\n🔍 Getting rooms for workspace: {workspace_name}")
                    rooms_url = f"https://app.mural.co/api/public/v1/workspaces/{workspace_id}/rooms"
                    rooms_response = requests.get(rooms_url, headers=headers)

                    if rooms_response.status_code == 200:
                        rooms = rooms_response.json()
                        print(f"✅ Found {len(rooms)} room(s) in {workspace_name}")

                        if 'rooms' not in all_data:
                            all_data['rooms'] = []
                        all_data['rooms'].extend(rooms)

                        # Get murals from each room
                        for room in rooms[:3]:  # Limit to first 3 rooms
                            room_id = room.get('id')
                            room_name = room.get('name', 'Unnamed Room')

                            print(f"\n🔍 Getting murals for room: {room_name}")
                            murals_url = f"https://app.mural.co/api/public/v1/rooms/{room_id}/murals"
                            murals_response = requests.get(murals_url, headers=headers)

                            if murals_response.status_code == 200:
                                murals = murals_response.json()
                                print(f"✅ Found {len(murals)} mural(s) in {room_name}")

                                if 'murals' not in all_data:
                                    all_data['murals'] = []
                                all_data['murals'].extend(murals)

                                # Display first few murals
                                for j, mural in enumerate(murals[:3]):
                                    mural_title = mural.get('title', 'Untitled')
                                    mural_id = mural.get('id', 'Unknown')
                                    print(f"   🎨 {j + 1}. {mural_title} (ID: {mural_id[:8]}...)")
                            else:
                                print(f"❌ Failed to get murals: {murals_response.status_code}")
                    else:
                        print(f"❌ Failed to get rooms for workspace {workspace_name}: {rooms_response.status_code}")
        else:
            print(f"❌ Failed to get workspaces: {response.text}")

    except Exception as e:
        print(f"❌ Error getting workspaces: {e}")

    # Try to get user information
    print(f"\n🔍 Testing user information endpoint...")
    try:
        user_response = requests.get("https://app.mural.co/api/public/v1/identity", headers=headers)
        print(f"   Identity endpoint status: {user_response.status_code}")

        if user_response.status_code == 200:
            user_data = user_response.json()
            print(f"✅ Got user identity information!")
            print(f"   👤 Name: {user_data.get('name', 'Unknown')}")
            print(f"   📧 Email: {user_data.get('email', 'Unknown')}")
            all_data['user'] = user_data
        else:
            print(f"   Identity endpoint failed with status: {user_response.status_code}")

    except Exception as e:
        print(f"   Error with identity endpoint: {e}")

    # Save all collected data
    if all_data:
        with open('mural_data.json', 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)
        print("\n💾 All data saved to mural_data.json")
        return all_data

    return None


def run_flask():
    """Run Flask app in a separate thread"""
    global server_started
    print("🚀 Starting Flask server for OAuth callbacks...")
    try:
        # Disable Flask's reloader to avoid issues
        app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False)
        server_started = True
    except Exception as e:
        print(f"❌ Failed to start Flask server: {e}")
        print("💡 Try closing any other applications using port 5000")
        sys.exit(1)


def cleanup():
    """Cleanup function to ensure proper shutdown"""
    print("\n🧹 Cleaning up...")
    if flask_thread and flask_thread.is_alive():
        print("   Shutting down Flask server...")
        # Flask doesn't have a clean shutdown in thread, but we'll signal it
        pass


if __name__ == "__main__":
    print("🚀 Starting Mural OAuth Integration...")
    print("=" * 60)
    print(f"🔑 Client ID: {CLIENT_ID[:20]}...")
    print(f"📍 Redirect URI: {REDIRECT_URI}")
    print(f"🔐 Scopes: rooms:read workspaces:read murals:read identity:read")
    print("=" * 60)

    # Reset the event
    token_received.clear()

    # Clear previous access token
    access_token = None

    try:
        # Start Flask server in background thread for OAuth callbacks
        flask_thread = threading.Thread(target=run_flask)
        flask_thread.daemon = True
        flask_thread.start()

        # Give Flask a moment to start
        print("⏳ Waiting for server to start...")
        for i in range(10):  # Wait up to 10 seconds
            try:
                # Try to connect to the server
                response = requests.get("http://127.0.0.1:5000/", timeout=1)
                if response.status_code == 200:
                    print("✅ Flask server started successfully!")
                    break
            except:
                if i < 9:
                    time.sleep(1)
                else:
                    print("⚠️  Server might be slow to start, continuing anyway...")
                    time.sleep(2)

        # Get the token
        token = get_mural_token_with_auth_code()

        if token:
            # Test the API with the token
            data = test_mural_api(token)

            print("\n" + "=" * 60)
            print("🎉 Mural API Integration Summary")
            print("=" * 60)
            if data:
                print("✅ Successfully collected data:")
                if 'workspaces' in data:
                    if isinstance(data['workspaces'], list):
                        print(f"   📁 {len(data['workspaces'])} workspace(s)")
                    else:
                        print(f"   📁 Workspace data available")
                if 'rooms' in data:
                    if isinstance(data['rooms'], list):
                        print(f"   🏢 {len(data['rooms'])} room(s)")
                    else:
                        print(f"   🏢 Room data available")
                if 'murals' in data:
                    if isinstance(data['murals'], list):
                        print(f"   🎨 {len(data['murals'])} mural(s)")
                    else:
                        print(f"   🎨 Mural data available")
                if 'user' in data:
                    print(f"   👤 User identity information")
                print(f"\n💾 Data saved to: mural_data.json")
            else:
                print("⚠️  Some API endpoints failed")
        else:
            print("💀 Mural OAuth flow failed")
            print("\n💡 Troubleshooting tips:")
            print("1. Make sure port 5000 is not being used by another application")
            print("2. Check if Mural app is properly configured in developer portal")
            print("3. Try manually opening the authorization URL")
            print("4. Clear browser cookies for app.mural.co and try again")

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    finally:
        cleanup()
        print("\n👋 Script finished")