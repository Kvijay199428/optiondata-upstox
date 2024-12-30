import pandas as pd
import os
import json

def list_json_files(directory):
    """List all JSON files in the given directory."""
    files = [f for f in os.listdir(directory) if f.endswith('.json')]
    return files

def read_json_to_dataframe(file_path):
    """Read a JSON file and convert it into a DataFrame."""
    with open(file_path, 'r') as file:
        data = json.load(file)
    # Convert JSON data to DataFrame
    df = pd.json_normalize(data)
    return df

def save_dataframe_to_csv(df, output_path):
    """Save the DataFrame to a CSV file."""
    df.to_csv(output_path, index=False)
    print(f"DataFrame has been saved to {output_path}")

def open_csv_in_excel(csv_file_path):
    """Open the CSV file in Microsoft Excel."""
    if os.name == 'nt':  # Check if the operating system is Windows
        os.startfile(csv_file_path)
    else:
        print("This script currently only supports opening files in Windows.")

def main():
    # Define the directories
    json_directory = 'api/instrument/'
    csv_directory = 'api/csv/'

    # Create the CSV directory if it does not exist
    os.makedirs(csv_directory, exist_ok=True)

    # List JSON files in the directory
    json_files = list_json_files(json_directory)

    # Print JSON files and ask user to select one
    print("Available JSON files:")
    for idx, file_name in enumerate(json_files):
        print(f"{idx}: {file_name}")
    
    # Prompt user to select a file
    selected_index = int(input("Enter the index of the file to convert: "))
    if selected_index < 0 or selected_index >= len(json_files):
        print("Invalid index selected.")
        return
    
    selected_file = json_files[selected_index]
    file_path = os.path.join(json_directory, selected_file)
    
    # Convert the selected JSON file to DataFrame
    df = read_json_to_dataframe(file_path)
    
    # Define the CSV output path
    csv_file_name = os.path.splitext(selected_file)[0] + '.csv'
    csv_file_path = os.path.join(csv_directory, csv_file_name)
    
    # Save the DataFrame to a CSV file
    save_dataframe_to_csv(df, csv_file_path)
    
    # Open the CSV file in Excel
    open_csv_in_excel(csv_file_path)

if __name__ == '__main__':
    main()
