import upstox_client
from upstox_client.rest import ApiException
import os
import glob
from colorama import Fore, Style, init
import sys
import io

# Set the stdout encoding to UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Initialize colorama
init(autoreset=True)

def resource_path(relative_path):
    """Get the absolute path to the resource, works for dev and for PyInstaller."""
    try:
        # PyInstaller creates a temp folder and stores the path in _MEIPASS
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)

# Token pattern to match files (wildcard matching for .txt files)
token_pattern = resource_path('api/token/*.txt')

def load_access_tokens(token_pattern):
    """Load all access token files matching the pattern."""
    token_files = glob.glob(token_pattern)
    if not token_files:
        raise FileNotFoundError("Error: No access token files found in the directory.")
    return token_files

def read_access_token(file_dir):
    """Load a single access token from a file."""
    try:
        with open(file_dir, 'r') as token_file:
            return token_file.read().strip()
    except FileNotFoundError:
        raise FileNotFoundError(f"Error: Access token file '{file_dir}' not found.")
    except Exception as e:
        raise Exception(f"An error occurred while loading the access token from '{file_dir}': {e}")

def config_upstox_instance(access_token):
    """Configure the Upstox API instance."""
    configuration = upstox_client.Configuration()
    configuration.access_token = access_token
    return upstox_client.LoginApi(upstox_client.ApiClient(configuration))

def logout_from_upstox(api_instance):
    """Log out from Upstox."""
    api_version = '2.0'
    try:
        api_response = api_instance.logout(api_version)

        if api_response is None:
            return "⚠️ No response from logout API. Check the token validity."

        if api_response.status == 'error':
            error_code = api_response.errors[0].errorCode if api_response.errors else None
            if error_code == 'UDAPI100050':
                return "⚠️ Invalid token used to access API. Please check the token."
            else:
                error_message = api_response.errors[0].message if api_response.errors else 'Unknown error'
                return f"Error during logout: {error_message}"

        if api_response.status == 'success' and api_response.data:
            return "🔓 LOGOUT SUCCESSFULLY"
        else:
            return "⚠️ Logout failed with unexpected status."
    except ApiException as e:
        return f"Exception when calling LoginApi->logout: {e}"
    except Exception as e:
        return f"An error occurred during logout: {e}"

def delete_access_token_file(file_dir):
    """Delete the access token file."""
    try:
        os.remove(file_dir)
        return f"Access token file '{file_dir}' has been deleted."
    except FileNotFoundError:
        return f"Error: Access token file '{file_dir}' not found for deletion."
    except Exception as e:
        return f"An error occurred while deleting the access token file '{file_dir}': {e}"

def main():
    """Main function to handle the logout process for multiple tokens."""
    try:
        token_files = load_access_tokens(token_pattern)
        if not token_files:
            print("No access token files found.")
            return

        print(f"Found {len(token_files)} token files to process...")
        print("-" * 50)

        for i, token_file in enumerate(token_files, 1):
            print(f"\nProcessing token {i}/{len(token_files)}:")
            
            # Read token
            access_token = read_access_token(token_file)
            print(f"Token loaded from: {token_file}")

            # Logout
            api_instance = config_upstox_instance(access_token)
            logout_message = logout_from_upstox(api_instance)
            print(logout_message)

            # Delete token file
            delete_message = delete_access_token_file(token_file)
            print(delete_message)
            print("-" * 50)

        print("\nAll tokens processed successfully!")
        input("\nPress Enter to exit...")

    except FileNotFoundError as e:
        print(f"\nError: {e}")
    except ApiException as e:
        print(f"\nAPI Error: {e}")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")
    finally:
        print("\nScript execution completed.")

if __name__ == "__main__":
    main()