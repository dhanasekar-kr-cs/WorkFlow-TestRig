import os
import json
import csv
import zstandard as zstd
from datetime import datetime

# Temp directory for processing
TEMP_DIR = 'Bronto Export Docs/'
os.makedirs(TEMP_DIR, exist_ok=True)

# CSV file size management
ROWS_PER_CSV = 1048575  # Save 1 million rows per CSV

def process_zstd_to_multiple_csv(local_file):
    try:
        decompressed_file = f'{TEMP_DIR}decompressed_file.json'
        with open(local_file, 'rb') as compressed_file, open(decompressed_file, 'wb') as decomp_file:
            decompressor = zstd.ZstdDecompressor()
            decompressor.copy_stream(compressed_file, decomp_file)

        file_count = 1
        row_count = 0
        csv_files = []  # Store the names of created CSV files

        current_timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        csv_file_path = f'{TEMP_DIR}Exported_data_part_{file_count}_{current_timestamp}.csv'
        csv_files.append(csv_file_path)
        csv_file = open(csv_file_path, 'w', newline='')
        writer = csv.writer(csv_file)
        is_header_written = False

        total_rows = 0  # Track total rows processed

        with open(decompressed_file, 'r') as json_file:
            for line in json_file:
                line = line.strip()  # Ensure the line is stripped of whitespace
                if not line:  # Skip empty lines
                    continue

                data = json.loads(line)
                total_rows += 1

                if not is_header_written:
                    writer.writerow(data.keys())  # Write header once
                    is_header_written = True

                writer.writerow(data.values())
                row_count += 1

                if row_count >= ROWS_PER_CSV:
                    csv_file.close()
                    print(f"CSV part {file_count} created with {row_count} rows.")
                    file_count += 1
                    row_count = 0
                    csv_file_path = f'{TEMP_DIR}Exported_data_part_{file_count}_{current_timestamp}.csv'
                    csv_files.append(csv_file_path)
                    csv_file = open(csv_file_path, 'w', newline='')
                    writer = csv.writer(csv_file)
                    writer.writerow(data.keys())  # Write header for new file

        # Close the last open CSV file
        csv_file.close()

        print(f"\nProcessing completed. Total rows processed: {total_rows}.\n")

        # Delete the .zstd and .json files
        if os.path.exists(local_file):
            os.remove(local_file)
            print(f"Deleted zstd file: {local_file}")
        if os.path.exists(decompressed_file):
            os.remove(decompressed_file)
            print(f"Deleted json file: {decompressed_file}")

        # Print CSV file names and their locations
        print("\nGenerated CSV files:")
        for file in csv_files:
            print(file)
        print("\n")

    except Exception as e:
        print(f"Error processing zstd file: {e}")

if __name__ == '__main__':
    # Provide the path to your local zstd file here
    local_zstd_file = "Test_Log.zst"
    process_zstd_to_multiple_csv(local_zstd_file)