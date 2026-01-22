#!/usr/bin/env python3
"""
Dropbox Refresh Token Generator
Run this script to get your permanent refresh token
"""

import requests
import webbrowser
import sys
import json


def main():
    print("\n" + "=" * 60)
    print("DROPBOX REFRESH TOKEN GENERATOR")
    print("=" * 60)
    print("\n📋 Follow these steps:\n")

    # Step 0: Instructions
    print("STEP 0: GET YOUR APP CREDENTIALS")
    print("-" * 40)
    print("1. Go to: https://www.dropbox.com/developers/apps")
    print("2. Click 'Create app'")
    print("3. Choose: 'Scoped access' → 'Full Dropbox'")
    print("4. Name it (e.g., 'ClimateReportApp')")
    print("5. Copy the 'App key' and 'App secret'\n")

    # Get app credentials
    APP_KEY = input("Enter your App Key: ").strip()
    APP_SECRET = input("Enter your App Secret: ").strip()

    if not APP_KEY or not APP_SECRET:
        print("\n❌ Error: App Key and App Secret are required!")
        sys.exit(1)

    print("\n✅ Step 1: Credentials saved")

    # Step 2: Generate auth URL
    print("\n" + "=" * 60)
    print("STEP 2: AUTHORIZE THE APP")
    print("=" * 60)

    auth_url = f"https://www.dropbox.com/oauth2/authorize?client_id={APP_KEY}&response_type=code&token_access_type=offline"

    print(f"\n🔗 Authorization URL:\n{auth_url}\n")

    # Try to open browser
    try:
        webbrowser.open(auth_url)
        print("✅ Browser opened! If not, copy the URL above.")
    except:
        print("⚠️ Please copy the URL above and open it in your browser")

    print("\n📋 In the browser:")
    print("1. Login to your Dropbox account")
    print("2. Click 'Allow' to authorize the app")
    print("3. You'll be redirected (may see an error page)")
    print("4. Look at the URL in your address bar")
    print("5. Find the part that says 'code=XXXXXXXX'")
    print("   Example: https://localhost/?code=abcdef123456")
    print()

    # Step 3: Get authorization code
    auth_code = input("Enter the authorization code: ").strip()

    if not auth_code:
        print("\n❌ Error: Authorization code is required!")
        sys.exit(1)

    print("\n🔄 Step 3: Exchanging code for tokens...")

    # Step 4: Exchange code for tokens
    token_url = "https://api.dropbox.com/oauth2/token"

    data = {
        'code': auth_code,
        'grant_type': 'authorization_code',
        'client_id': APP_KEY,
        'client_secret': APP_SECRET
    }

    try:
        response = requests.post(token_url, data=data, timeout=30)

        if response.status_code == 200:
            tokens = response.json()

            print("\n" + "=" * 60)
            print("✅ SUCCESS! TOKENS OBTAINED")
            print("=" * 60)

            refresh_token = tokens.get('refresh_token', '')
            access_token = tokens.get('access_token', '')

            if not refresh_token:
                print("\n❌ ERROR: No refresh token received!")
                print("Make sure 'token_access_type=offline' is in the auth URL")
                sys.exit(1)

            # Display tokens
            print(f"\n🔑 ACCESS TOKEN: {access_token[:30]}...")
            print(f"🔄 REFRESH TOKEN: {refresh_token[:30]}...")
            print(f"⏱️  EXPIRES IN: {tokens.get('expires_in')} seconds")

            # Save to .env format
            print("\n" + "=" * 60)
            print("STEP 4: SAVE TO .ENV FILE")
            print("=" * 60)

            env_content = f"""# Dropbox Configuration
DROPBOX_REFRESH_TOKEN={refresh_token}
DROPBOX_TOKEN={access_token}
DROPBOX_APP_KEY={APP_KEY}
DROPBOX_APP_SECRET={APP_SECRET}
"""

            print("\n📁 Copy this to your .env file:")
            print("-" * 40)
            print(env_content)
            print("-" * 40)

            # Save to file
            with open('.env.dropbox', 'w') as f:
                f.write(env_content)

            print(f"\n💾 Also saved to 'env.dropbox' file")

            # Test the tokens
            print("\n" + "=" * 60)
            print("STEP 5: TESTING TOKENS")
            print("=" * 60)

            # Test access token
            try:
                import dropbox
                dbx = dropbox.Dropbox(access_token)
                account = dbx.users_get_current_account()
                print(f"\n✅ Access Token Test: PASSED")
                print(f"   Connected as: {account.name.display_name}")
                print(f"   Email: {account.email}")
            except ImportError:
                print("\n⚠️ Install dropbox module: pip install dropbox")
                print("   Skipping access token test...")
            except Exception as e:
                print(f"\n⚠️ Access Token Test: FAILED - {e}")

            # Test refresh token
            print("\n🔄 Testing refresh token...")
            refresh_data = {
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'client_id': APP_KEY,
                'client_secret': APP_SECRET
            }

            refresh_response = requests.post(token_url, data=refresh_data)
            if refresh_response.status_code == 200:
                print("✅ Refresh Token Test: PASSED")
                print("   Can get new access tokens successfully")
            else:
                print(f"❌ Refresh Token Test: FAILED - {refresh_response.status_code}")
                print(f"   Response: {refresh_response.text[:100]}")

            print("\n" + "=" * 60)
            print("🎉 SETUP COMPLETE!")
            print("=" * 60)
            print("\nNext steps:")
            print("1. Add the tokens to your Flask app's .env file")
            print("2. Restart your Flask app")
            print("3. Your app now has permanent Dropbox access!")

        else:
            print(f"\n❌ ERROR {response.status_code}:")
            print(response.text)
            print("\nTroubleshooting:")
            print("1. Check App Key/Secret are correct")
            print("2. Authorization codes expire quickly - get a new one")
            print("3. Try the process again")

    except requests.exceptions.RequestException as e:
        print(f"\n❌ Network error: {e}")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()