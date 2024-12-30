import upstox_client
from upstox_client.rest import ApiException
import os
import glob
from colorama import Fore, Style, init
import PySimpleGUI as sg
import sys
import io

# Set the stdout encoding to UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Initialize colorama
init(autoreset=True)

# token_pattern = 'api/token/*.txt'

# # Load Access Tokens DIR
# def load_access_tokens(token_pattern):
#     """Load all access token files matching the pattern."""
#     token_files = glob.glob(token_pattern)
#     if not token_files:
#         raise FileNotFoundError("Error: No access token files found in the directory.")
#     return token_files

# # Access Token DIR
# def read_access_token(file_dir):
#     """Load a single access token from a file."""
#     try:
#         with open(file_dir, 'r') as token_file:
#             return token_file.read().strip()
#     except FileNotFoundError:
#         raise FileNotFoundError(f"Error: Access token file '{file_dir}' not found.")
#     except Exception as e:
#         raise Exception(f"An error occurred while loading the access token from '{file_dir}': {e}")

# Function to ensure paths are correctly located in executable
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

# Load Access Tokens DIR
def load_access_tokens(token_pattern):
    """Load all access token files matching the pattern."""
    # Use glob to find matching files
    token_files = glob.glob(token_pattern)
    if not token_files:
        raise FileNotFoundError("Error: No access token files found in the directory.")
    return token_files

# Access Token DIR
def read_access_token(file_dir):
    """Load a single access token from a file."""
    try:
        with open(file_dir, 'r') as token_file:
            return token_file.read().strip()
    except FileNotFoundError:
        raise FileNotFoundError(f"Error: Access token file '{file_dir}' not found.")
    except Exception as e:
        raise Exception(f"An error occurred while loading the access token from '{file_dir}': {e}")

# Example usage
try:
    token_files = load_access_tokens(token_pattern)
    for token_file in token_files:
        token = read_access_token(token_file)
        print(f"Loaded token: {token}")
except Exception as e:
    print(e)

# Configure Upstox API instance
def config_upstox_instance(access_token):
    """Configure the Upstox API instance."""
    configuration = upstox_client.Configuration()
    configuration.access_token = access_token
    return upstox_client.LoginApi(upstox_client.ApiClient(configuration))

# LogOut User
def logout_from_upstox(api_instance):
    """Log out from Upstox."""
    api_version = '2.0'
    try:
        # Call the logout method
        api_response = api_instance.logout(api_version)

        # Check if the response is successful
        if api_response is None:
            return "⚠️ No response from logout API. Check the token validity."

        # Check the status and data of the response
        if api_response.status == 'error':
            error_code = api_response.errors[0].errorCode if api_response.errors else None
            if error_code == 'UDAPI100050':
                return "⚠️ Invalid token used to access API. Please check the token."
            else:
                error_message = api_response.errors[0].message if api_response.errors else 'Unknown error'
                return f"Error during logout: {error_message}"

        # Check if logout was successful
        if api_response.status == 'success' and api_response.data:
            return "🔓 LOGOUT SUCCESSFULLY"
        else:
            return "⚠️ Logout failed with unexpected status."
    except ApiException as e:
        return f"Exception when calling LoginApi->logout: {e}"
    except Exception as e:
        return f"An error occurred during logout: {e}"

# Delete Access Token Files
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
    
    # Create a PySimpleGUI progress bar window
    sg.theme('DarkAmber')
    layout = [
        [sg.Text('Processing Logout')],
        [sg.ProgressBar(100, orientation='h', size=(40, 20), key='progressbar')],
        [sg.Button('Close', disabled=True, key='close_button')]  # Close button is initially disabled
    ]
    window = sg.Window('Progress', layout, finalize=True)
    progress_bar = window['progressbar']

    try:
        token_files = load_access_tokens(token_pattern)
        if not token_files:
            sg.PopupError("No access token files found.")
            return

        total_tasks = len(token_files) * 5  # Each token has 5 main steps
        task_count = 0

        # Automatically log out all tokens
        for token_file in token_files:
            access_token = read_access_token(token_file)
            task_count += 1
            progress_bar.UpdateBar((task_count / total_tasks) * 100)

            api_instance = config_upstox_instance(access_token)
            logout_message = logout_from_upstox(api_instance)
            print(logout_message)  # Print logout message to console
            task_count += 1
            progress_bar.UpdateBar((task_count / total_tasks) * 100)

            delete_message = delete_access_token_file(token_file)
            print(delete_message)  # Print delete message to console
            task_count += 1
            progress_bar.UpdateBar((task_count / total_tasks) * 100)

            # Check for window events
            event, _ = window.read(timeout=1)
            if event == sg.WIN_CLOSED:  # Disable window closing while processing
                print("Please wait until the process completes.")
                break

        # Progress bar completed
        progress_bar.UpdateBar(100)

        # Start 10-second countdown for the close button
        for i in range(1, 0, -1):
            window['close_button'].update(f'Close ({i})', disabled=True)
            window.read(timeout=1000)  # Wait for 1 second between updates

        # Enable the close button after countdown
        window['close_button'].update('Close', disabled=False)

        # Wait for the user to close the window
        while True:
            event, _ = window.read(timeout=100)
            if event == 'close_button' or event == sg.WIN_CLOSED:
                break

    except FileNotFoundError as e:
        sg.PopupError(str(e))
    except ApiException as e:
        sg.PopupError(str(e))
    except Exception as e:
        sg.PopupError(str(e))
    finally:
        window.close()

if __name__ == "__main__":
    main()
