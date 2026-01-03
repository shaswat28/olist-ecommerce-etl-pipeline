import pandas as pd
import os

def extract_data(data_folder="data"):

    print('scanning directory {data_folder}')
    files = [f for f in os.listdir(data_folder) if f.endswith('.csv')]   

    if len(files) < 9:
        print('wrong num')
        
    return {f.replace('.csv', ''): os.path.join(data_folder, f) for f in files}

if __name__ == "__main__":
    # Test the extraction logic
    file_map = extract_data()
    for name, path in file_map.items():
        print(f"Ready to extract: {name}")
