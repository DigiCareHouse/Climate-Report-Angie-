#!/usr/bin/env python3
"""
Test your Dropbox connection with the tokens you have
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

print("\n🔗 TESTING DROPBOX CONNECTION")
print("=" * 60)

# Get your tokens
REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
ACCESS_TOKEN = os.getenv("DROPBOX_TOKEN")
APP_KEY = os.getenv("DROPBOX_APP_KEY")
APP_SECRET = os.getenv("DROPBOX_APP_SECRET")

print(f"\n📏 Token lengths:")
print(f"   Refresh token: {len(REFRESH_TOKEN) if REFRESH_TOKEN else 0} chars")
print(f"   Access token: {len(ACCESS_TOKEN) if ACCESS_TOKEN else 0} chars")
print(f"   App Key: {APP_KEY}")
print(f"   App Secret: {len(APP_SECRET) if APP_SECRET else 0} chars")

# Test 1: Test refresh token mechanism
print("\n" + "=" * 60)
print("🔄 TEST 1: REFRESH TOKEN")
print("=" * 60)

try:
    response = requests.post(
        "https://api.dropbox.com/oauth2/token",
        data={
            'grant_type': 'refresh_token',
            'refresh_token': REFRESH_TOKEN,
            'client_id': APP_KEY,
            'client_secret': APP_SECRET
        },
        timeout=30
    )

    print(f"📊 Status: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        new_access_token = data['access_token']
        print("✅ SUCCESS! Refresh token works!")
        print(f"   New access token: {new_access_token[:30]}...")
        print(f"   Length: {len(new_access_token)} chars")
        print(f"   Expires in: {data.get('expires_in', 'unknown')} seconds")

        # Update the access token
        os.environ["DROPBOX_TOKEN"] = new_access_token
        print("   ✅ Updated environment variable")
    else:
        print(f"❌ Failed: {response.status_code}")
        print(f"   Error: {response.text}")

except Exception as e:
    print(f"❌ Exception: {e}")

# Test 2: Test current access token
print("\n" + "=" * 60)
print("🔑 TEST 2: CURRENT ACCESS TOKEN")
print("=" * 60)

try:
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
    response = requests.post(
        'https://api.dropboxapi.com/2/users/get_current_account',
        headers=headers,
        timeout=30
    )

    print(f"📊 Status: {response.status_code}")

    if response.status_code == 200:
        account_data = response.json()
        print("✅ SUCCESS! Access token works!")
        print(f"   👤 Name: {account_data.get('name', {}).get('display_name')}")
        print(f"   📧 Email: {account_data.get('email')}")
        print(f"   🏢 Account ID: {account_data.get('account_id')}")
    else:
        print(f"⚠️ Current token might be expired: {response.status_code}")
        print(f"   Error: {response.text}")

except Exception as e:
    print(f"❌ Exception: {e}")

# Test 3: Upload a test file
print("\n" + "=" * 60)
print("📤 TEST 3: FILE UPLOAD")
print("=" * 60)

try:
    # Use the latest access token
    current_token = os.getenv("DROPBOX_TOKEN") or ACCESS_TOKEN
    test_content = f"Dropbox connection test\nTime: {os.times()}\nApp: Flask Gemini Report\nStatus: Working!"
    test_filename = f"test_connection_{os.times()[4]}.txt"

    headers = {
        'Authorization': f'Bearer {current_token}',
        'Dropbox-API-Arg': f'{{"path": "/{test_filename}", "mode": "overwrite"}}',
        'Content-Type': 'application/octet-stream'
    }

    response = requests.post(
        'https://content.dropboxapi.com/2/files/upload',
        headers=headers,
        data=test_content.encode('utf-8'),
        timeout=30
    )

    print(f"📊 Status: {response.status_code}")

    if response.status_code == 200:
        upload_data = response.json()
        print("✅ SUCCESS! File uploaded!")
        print(f"   📁 Path: {upload_data.get('path_display')}")
        print(f"   📏 Size: {upload_data.get('size')} bytes")
        print(f"   📅 Modified: {upload_data.get('server_modified')}")
    else:
        print(f"⚠️ Upload failed: {response.status_code}")
        print(f"   Error: {response.text}")

except Exception as e:
    print(f"❌ Exception: {e}")

print("\n" + "=" * 60)
print("🎉 TEST COMPLETE!")
print("=" * 60)