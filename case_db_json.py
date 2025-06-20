import json
import mysql.connector
import csv
import os # For __main__ block example file checks
from . import common_utils
from . import config

def run_case_db_json(output_csv_filename: str):
    """
    Compare JSON content stored in a database for order pairs specified in a CSV.
    CSV columns must include 'wcs_order_id' and 'micro_order_id'.
    """
    all_diffs_for_csv = []
    conn = None  # Initialize conn to None for finally block

    try:
        # Connect to the database using config.DB_CONFIG
        conn = mysql.connector.connect(**config.DB_CONFIG)
        print("Successfully connected to the database for JSON comparison.")

        # Read order ID pairs from config.DB_ORDERS_JSON_CSV
        try:
            with open(config.DB_ORDERS_JSON_CSV, newline="", encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                if rdr.fieldnames is None: # Check if CSV is empty or not valid
                    print(f"🛑 Error: CSV file '{config.DB_ORDERS_JSON_CSV}' is empty or not a valid CSV.")
                    if conn: conn.close()
                    return

                required_fields = {"wcs_order_id", "micro_order_id"}
                if not required_fields.issubset(rdr.fieldnames):
                    raise ValueError(f"CSV '{config.DB_ORDERS_JSON_CSV}' must have columns: {required_fields}")

                pairs = []
                for row in rdr:
                    wcs_id = row.get("wcs_order_id")
                    mic_id = row.get("micro_order_id")
                    if not wcs_id or not mic_id:
                        print(f"⚠️ Warning: Skipping row due to missing 'wcs_order_id' or 'micro_order_id' in '{config.DB_ORDERS_JSON_CSV}'")
                        continue
                    pairs.append((wcs_id, mic_id))

        except FileNotFoundError:
            print(f"🛑 Error: Input CSV file '{config.DB_ORDERS_JSON_CSV}' not found.")
            if conn: conn.close()
            return
        except ValueError as e: # Catch ValueError from missing columns
            print(f"🛑 Error: {e}")
            if conn: conn.close()
            return
        except Exception as e: # Catch other potential errors during CSV processing
            print(f"🛑 An unexpected error occurred while reading '{config.DB_ORDERS_JSON_CSV}': {e}")
            if conn: conn.close()
            return

        for wcs_id, mic_id in pairs:
            print(f"\n🔍 Comparing DB JSON for order {wcs_id} ↔ {mic_id}")

            # Use common_utils.fetch_json
            json_data1_raw = common_utils.fetch_json(conn, wcs_id)
            json_data2_raw = common_utils.fetch_json(conn, mic_id)

            if not json_data1_raw or not json_data2_raw:
                print(f"📢 Info: Missing JSON for WCS ID '{wcs_id}' or Microservice ID '{mic_id}'. Skipping this pair.")
                if not json_data1_raw: print(f"  - WCS JSON for ID '{wcs_id}' not found.")
                if not json_data2_raw: print(f"  - Microservice JSON for ID '{mic_id}' not found.")
                continue

            try:
                # Handle cases where fetched JSON is a string or already a dict
                json1 = json.loads(json_data1_raw) if isinstance(json_data1_raw, str) else json_data1_raw
                json2 = json.loads(json_data2_raw) if isinstance(json_data2_raw, str) else json_data2_raw
            except json.JSONDecodeError as e:
                print(f"🛑 JSON decode error for WCS ID '{wcs_id}' or Microservice ID '{mic_id}': {e}")
                continue
            except Exception as e: # Catch other potential errors during JSON processing
                print(f"🛑 An unexpected error occurred while processing JSON for IDs '{wcs_id}', '{mic_id}': {e}")
                continue

            # Use common_utils.compare_json_files
            diffs_for_pair = common_utils.compare_json_files(json1, json2)

            for attribute_path, diff_type, val1, val2 in diffs_for_pair:
                # Prepend order IDs to the attribute/path for context
                contextual_path = f"Order {wcs_id} vs {mic_id}::{attribute_path}"
                all_diffs_for_csv.append((contextual_path, diff_type, val1, val2))

    except mysql.connector.Error as err:
        print(f"🛑 Database connection error: {err}")
    except Exception as e: # Catch any other unexpected errors
        print(f"🛑 An unexpected error occurred: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("Database connection closed.")

    # Use common_utils.write_csv for output.
    # Header is fixed in common_utils.write_csv:
    # ["attribute", "difference type", "wcs value", "microservice value"]
    if not all_diffs_for_csv and not pairs: # If pairs list was empty or no diffs were generated
         print("\nNo data to write to CSV. Either no pairs were processed or no differences found.")
    else:
        common_utils.write_csv(all_diffs_for_csv, output_csv_filename)


if __name__ == '__main__':
    # Example usage:
    # This part is for direct testing of the module.
    print("Running JSON Database Comparison (Case 4)")

    # Create dummy DB_ORDERS_JSON_CSV if it doesn't exist
    if not os.path.exists(config.DB_ORDERS_JSON_CSV):
        with open(config.DB_ORDERS_JSON_CSV, 'w', newline='') as f_csv:
            writer = csv.writer(f_csv)
            writer.writerow(['wcs_order_id', 'micro_order_id'])
            # Add some dummy IDs. These won't work unless your DB has them
            # and fetch_json is set up to retrieve them.
            writer.writerow(['sample_wcs_json_id_1', 'sample_mic_json_id_1'])
            writer.writerow(['sample_wcs_json_id_2', 'sample_mic_json_id_2'])
        print(f"Created dummy input CSV: {config.DB_ORDERS_JSON_CSV}")
        print("Note: The dummy CSV contains placeholder order IDs. "
              "For a real test, ensure these IDs exist in your database "
              "and are associated with JSON data, or update the CSV with valid IDs.")

    print("Attempting to run JSON DB comparison. Ensure DB is configured and accessible.")
    print(f"Using DB config: Host={config.DB_CONFIG.get('host', 'N/A')}, "
          f"Database={config.DB_CONFIG.get('database', 'N/A')}, "
          f"User={config.DB_CONFIG.get('user', 'N/A')}")
    print(f"Reading order pairs from: {config.DB_ORDERS_JSON_CSV}")

    run_case_db_json("all_differences_case_db_json.csv")
    print("\nJSON Database Comparison (Case 4) finished. Check 'all_differences_case_db_json.csv'")
    print("If the output CSV is empty, it might be due to DB connection issues, "
          "missing order IDs/JSON data in the DB, or no differences found.")
