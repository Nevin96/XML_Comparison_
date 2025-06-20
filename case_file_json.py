import os
import json
import csv
from . import common_utils
from . import config

def run_case_file_json(output_csv_filename: str):
    """
    Compare JSON files from filesystem pairs listed in a CSV.
    CSV columns must include 'wcs_json' and 'micro_json' with file names including extensions.
    """
    all_diffs_for_csv = []

    try:
        # Read file pairs from config.FILE_PAIRS_JSON_CSV
        with open(config.FILE_PAIRS_JSON_CSV, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            if rdr.fieldnames is None: # Check if CSV is empty or not valid
                print(f"🛑 Error: CSV file '{config.FILE_PAIRS_JSON_CSV}' is empty or not a valid CSV.")
                return

            required_fields = {"wcs_json", "micro_json"}
            if not required_fields.issubset(rdr.fieldnames):
                raise ValueError(f"CSV '{config.FILE_PAIRS_JSON_CSV}' must have columns: {required_fields}")

            for row_number, row in enumerate(rdr, start=2): # start=2 because DictReader consumes header
                wcs_json_file = row.get("wcs_json")
                micro_json_file = row.get("micro_json")

                if not wcs_json_file or not micro_json_file:
                    print(f"⚠️ Warning: Skipping row {row_number} due to missing 'wcs_json' or 'micro_json' in '{config.FILE_PAIRS_JSON_CSV}'")
                    continue

                # Use config.XML_FOLDER for paths (as per original logic)
                wcs_path = os.path.join(config.XML_FOLDER, wcs_json_file)
                mic_path = os.path.join(config.XML_FOLDER, micro_json_file)

                print(f"\n🔍 Comparing {wcs_json_file} ↔ {micro_json_file}")

                try:
                    with open(wcs_path, "r", encoding="utf-8") as f1, open(mic_path, "r", encoding="utf-8") as f2:
                        json1 = json.load(f1)
                        json2 = json.load(f2)
                except FileNotFoundError as e:
                    print(f"🛑 File not found error: {e}")
                    continue
                except json.JSONDecodeError as e:
                    print(f"🛑 JSON decode error for '{wcs_json_file}' or '{micro_json_file}': {e}")
                    continue
                except Exception as e: # Catch other potential errors during file reading/JSON parsing
                    print(f"🛑 An unexpected error occurred while processing files '{wcs_json_file}' or '{micro_json_file}': {e}")
                    continue

                # Use common_utils.compare_json_files
                diffs_for_pair = common_utils.compare_json_files(json1, json2)

                for attribute_path, diff_type, wcs_val, mic_val in diffs_for_pair:
                    # Prepend filenames to the attribute/path for context
                    contextual_path = f"{wcs_json_file} vs {micro_json_file}::{attribute_path}"
                    all_diffs_for_csv.append((contextual_path, diff_type, wcs_val, mic_val))

    except FileNotFoundError:
        print(f"🛑 Error: Input CSV file '{config.FILE_PAIRS_JSON_CSV}' not found.")
        return
    except ValueError as e: # Catch ValueError from missing columns
        print(f"🛑 Error: {e}")
        return
    except Exception as e: # Catch other potential errors during CSV processing
        print(f"🛑 An unexpected error occurred while reading '{config.FILE_PAIRS_JSON_CSV}': {e}")
        return

    # Use common_utils.write_csv for output.
    # Header is fixed in common_utils.write_csv:
    # ["attribute", "difference type", "wcs value", "microservice value"]
    if not all_diffs_for_csv:
        print("\nNo JSON differences found or no pairs processed.")
    common_utils.write_csv(all_diffs_for_csv, output_csv_filename)


if __name__ == '__main__':
    # Example usage:
    # This part is for direct testing of the module.
    print("Running JSON File Comparison (Case 3)")

    # Ensure XML_FOLDER (used for JSON files here) from config exists
    if not os.path.exists(config.XML_FOLDER):
        os.makedirs(config.XML_FOLDER)
        print(f"Created dummy folder for JSON files (using config.XML_FOLDER): {config.XML_FOLDER}")

    # Create dummy FILE_PAIRS_JSON_CSV if it doesn't exist
    if not os.path.exists(config.FILE_PAIRS_JSON_CSV):
        with open(config.FILE_PAIRS_JSON_CSV, 'w', newline='') as f_csv:
            writer = csv.writer(f_csv)
            writer.writerow(['wcs_json', 'micro_json'])
            writer.writerow(['test_wcs1.json', 'test_mic1.json'])
            writer.writerow(['test_wcs2.json', 'test_mic2.json'])
        print(f"Created dummy input CSV: {config.FILE_PAIRS_JSON_CSV}")

        # Create dummy JSON files for the CSV
        dummy_wcs1 = {"id": 1, "name": "WCS Item 1", "value": "Original", "details": {"attr": "A"}}
        dummy_mic1 = {"id": 1, "name": "Micro Item 1", "value": "Modified", "details": {"attr": "B"}}
        dummy_wcs2 = {"id": 2, "name": "WCS Item 2", "status": "Active"}
        dummy_mic2 = {"id": 2, "name": "WCS Item 2", "status": "Inactive", "extra_field": "Present"}

        with open(os.path.join(config.XML_FOLDER, "test_wcs1.json"), 'w') as f_json:
            json.dump(dummy_wcs1, f_json, indent=2)
        with open(os.path.join(config.XML_FOLDER, "test_mic1.json"), 'w') as f_json:
            json.dump(dummy_mic1, f_json, indent=2)
        with open(os.path.join(config.XML_FOLDER, "test_wcs2.json"), 'w') as f_json:
            json.dump(dummy_wcs2, f_json, indent=2)
        with open(os.path.join(config.XML_FOLDER, "test_mic2.json"), 'w') as f_json:
            json.dump(dummy_mic2, f_json, indent=2)
        print(f"Created dummy JSON files in {config.XML_FOLDER}")

    run_case_file_json("all_differences_case_file_json.csv")
    print("\nJSON File Comparison (Case 3) finished. Check 'all_differences_case_file_json.csv'")
