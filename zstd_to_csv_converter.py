import json
import csv
import zstandard as zstd
import os

def decompress_zstd_file(input_file, output_file):
    with open(input_file, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        with open(output_file, 'wb') as decompressed_file:
            dctx.copy_stream(compressed_file, decompressed_file)
    print(f"Decompression complete. File saved as {output_file}")

def convert_json_to_csv(input_file, output_file_base, rows_per_file):
    headers = ['timestamp', 'geo_city', 'response_status', 'org', 'apiKey', 'shield', 'cache', 'host', 'pop', 'resTime', 'response_body_size', 'request_user_agent', 'response_body_size', 'url'] 
    file_count = 1
    row_count = 0

    with open(input_file, 'r') as json_file:
        csv_file = open(f"{output_file_base}_{file_count}.csv", 'w', newline='')
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        writer.writeheader()

        for line in json_file:
            if line.strip():
                data = json.loads(line)
                row = {key: data.get(key, '') for key in headers}
                writer.writerow(row)
                row_count += 1

                if row_count >= rows_per_file:
                    csv_file.close()
                    file_count += 1
                    row_count = 0
                    csv_file = open(f"{output_file_base}_{file_count}.csv", 'w', newline='')
                    writer = csv.DictWriter(csv_file, fieldnames=headers)
                    writer.writeheader()

        csv_file.close()

    print(f"Conversion complete. CSV files saved as {output_file_base}_1.csv, {output_file_base}_2.csv, etc.")

def process_file(input_zstd, output_json, output_csv_base, rows_per_file=1000000): 
    # Step 1: Decompress the zstd file
    decompress_zstd_file(input_zstd, output_json)
    
    # Step 2: Check if the decompressed file exists
    if not os.path.exists(output_json):
        print(f"Error: Decompressed file {output_json} not found.")
        return
    
    # Step 3: Convert the decompressed JSON to CSV with specific fields
    convert_json_to_csv(output_json, output_csv_base, rows_per_file)

# Usage
# Update the input path
input_zstd = '/input-files/Test_Log.zst'  # Your zstd compressed input file
# input_zstd = 'Test_Log.zst'  # Your zstd compressed input file
output_json = 'Test.json'  # Decompressed JSON file
output_csv_base = 'Test'  # Base name for output CSV files 
rows_per_file = 1000000     # Example: Split into files of 500,000 rows each

process_file(input_zstd, output_json, output_csv_base, rows_per_file)

#Hello
