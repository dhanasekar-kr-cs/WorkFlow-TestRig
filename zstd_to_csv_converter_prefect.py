#!/usr/bin/env python3

import os
import json
import csv
import zstandard as zstd
from prefect import flow, task
from datetime import datetime

@task
def decompress_zstd_file(input_file, output_file):
    """Decompress zstd file to JSON"""
    print(f"📦 Starting decompression of {input_file}...")
    print(f"📏 Input file size: {os.path.getsize(input_file):,} bytes")
    
    with open(input_file, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        with open(output_file, 'wb') as decompressed_file:
            dctx.copy_stream(compressed_file, decompressed_file)
    
    print(f"✅ Decompression complete. File saved as {output_file}")
    print(f"📏 Output file size: {os.path.getsize(output_file):,} bytes")
    return output_file

@task
def convert_json_to_csv(input_file, output_file_base, rows_per_file):
    """Convert JSON to CSV files"""
    print(f"🔄 Starting conversion of {input_file} to CSV...")
    print(f"📊 Target: {rows_per_file:,} rows per CSV file")
    
    headers = ['timestamp', 'geo_city', 'response_status', 'org', 'apiKey', 'shield', 'cache', 'host', 'pop', 'resTime', 'response_body_size', 'request_user_agent', 'response_body_size', 'url'] 
    file_count = 1
    row_count = 0
    total_lines_processed = 0

    with open(input_file, 'r') as json_file:
        csv_file = open(f"{output_file_base}_{file_count}.csv", 'w', newline='')
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        writer.writeheader()

        for line_num, line in enumerate(json_file, 1):
            if line.strip():
                try:
                    data = json.loads(line)
                    row = {key: data.get(key, '') for key in headers}
                    writer.writerow(row)
                    row_count += 1
                    total_lines_processed += 1

                    # Progress logging every 10,000 lines
                    if line_num % 10000 == 0:
                        print(f"📈 Processed {line_num:,} lines, {row_count:,} rows in current file")

                    if row_count >= rows_per_file:
                        csv_file.close()
                        print(f"✅ Completed CSV file {file_count} with {row_count:,} rows")
                        file_count += 1
                        row_count = 0
                        csv_file = open(f"{output_file_base}_{file_count}.csv", 'w', newline='')
                        writer = csv.DictWriter(csv_file, fieldnames=headers)
                        writer.writeheader()
                        
                except json.JSONDecodeError as e:
                    print(f"⚠️  Error parsing line {line_num}: {e}")
                    continue

        csv_file.close()

    print(f"🎉 Conversion complete! Created {file_count} CSV files")
    print(f"📊 Total lines processed: {total_lines_processed:,}")
    print(f"📁 Files saved as: {output_file_base}_1.csv, {output_file_base}_2.csv, etc.")
    return file_count

@task
def cleanup_temp_file(file_path):
    """Clean up temporary JSON file"""
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        os.remove(file_path)
        print(f"🗑️  Cleaned up temporary file: {file_path}")
        print(f"📏 Freed up {file_size:,} bytes of disk space")
    return True

@flow(name="ZSTD to CSV Converter Flow")
def zstd_to_csv_converter_flow():
    """Main flow to convert zstd file to CSV"""
    print("🚀 Starting ZSTD to CSV conversion process...")
    
    # File paths
    input_zstd = '/home/ubuntu/Files/Input.zstd'
    output_json = '/home/ubuntu/Output files/temp_conversion.json'
    output_csv_base = '/home/ubuntu/Output files/converted_data'
    rows_per_file = 1000000
    
    # Create output directory if it doesn't exist
    os.makedirs('/home/ubuntu/Output files', exist_ok=True)
    
    print(f"📂 Input file: {input_zstd}")
    print(f"📄 Output JSON: {output_json}")
    print(f"📊 Output CSV base: {output_csv_base}")
    print(f"📈 Rows per CSV file: {rows_per_file:,}")
    
    # Check if input file exists
    if not os.path.exists(input_zstd):
        print(f"❌ Error: Input file {input_zstd} not found!")
        return
    
    print(f"✅ Input file found! Size: {os.path.getsize(input_zstd):,} bytes")
    
    # Step 1: Decompress the zstd file
    print("\n🔄 STEP 1: Decompressing ZSTD file...")
    decompressed_file = decompress_zstd_file(input_zstd, output_json)
    
    # Step 2: Check if the decompressed file exists
    if not os.path.exists(output_json):
        print(f"❌ Error: Decompressed file {output_json} not found.")
        return
    
    print(f"✅ Decompression successful!")
    
    # Step 3: Convert the decompressed JSON to CSV
    print("\n🔄 STEP 2: Converting JSON to CSV files...")
    csv_file_count = convert_json_to_csv(output_json, output_csv_base, rows_per_file)
    
    # Step 4: Clean up temporary JSON file
    print("\n🔄 STEP 3: Cleaning up temporary files...")
    cleanup_temp_file(output_json)
    
    print(f"✅ Conversion completed successfully!")
    print(f"📁 Created {csv_file_count} CSV files in /home/ubuntu/Output files/")
    print(f"📊 Each file contains up to {rows_per_file:,} rows")
    print(f"🎯 Output directory: /home/ubuntu/Output files/")
    
    return {
        "status": "success",
        "csv_files_created": csv_file_count,
        "rows_per_file": rows_per_file,
        "output_directory": "/home/ubuntu/Output files/"
    }

if __name__ == "__main__":
    result = zstd_to_csv_converter_flow()
    print(f"Final result: {result}")
