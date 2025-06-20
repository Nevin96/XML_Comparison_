import xml.etree.ElementTree as ET
import mysql.connector
import csv
from . import common_utils
from . import config

def run_case_db_xml(output_csv_filename: str):
    """
    Compare XML content stored in a database for order pairs specified in a CSV.
    CSV columns must include 'wcs_order_id' and 'micro_order_id'.
    """
    excluded = common_utils.load_excluded_attributes(common_utils.EXCLUDED_ATTRIBUTES_FILE)
    all_diffs_for_csv = []
    conn = None  # Initialize conn to None for finally block

    try:
        # Connect to the database using config.DB_CONFIG
        conn = mysql.connector.connect(**config.DB_CONFIG)
        print("Successfully connected to the database.")

        # Read order ID pairs from config.DB_ORDERS_XML_CSV
        try:
            with open(config.DB_ORDERS_XML_CSV, newline="", encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                if rdr.fieldnames is None: # Check if CSV is empty or not valid
                    print(f"🛑 Error: CSV file '{config.DB_ORDERS_XML_CSV}' is empty or not a valid CSV.")
                    if conn: conn.close()
                    return

                required_fields = {"wcs_order_id", "micro_order_id"}
                if not required_fields.issubset(rdr.fieldnames):
                    raise ValueError(f"CSV '{config.DB_ORDERS_XML_CSV}' must have columns: {required_fields}")

                pairs = []
                for row in rdr:
                    wcs_id = row.get("wcs_order_id")
                    mic_id = row.get("micro_order_id")
                    if not wcs_id or not mic_id:
                        print(f"⚠️ Warning: Skipping row due to missing 'wcs_order_id' or 'micro_order_id' in '{config.DB_ORDERS_XML_CSV}'")
                        continue
                    pairs.append((wcs_id, mic_id))

        except FileNotFoundError:
            print(f"🛑 Error: Input CSV file '{config.DB_ORDERS_XML_CSV}' not found.")
            if conn: conn.close()
            return
        except ValueError as e: # Catch ValueError from missing columns
            print(f"🛑 Error: {e}")
            if conn: conn.close()
            return
        except Exception as e: # Catch other potential errors during CSV processing
            print(f"🛑 An unexpected error occurred while reading '{config.DB_ORDERS_XML_CSV}': {e}")
            if conn: conn.close()
            return

        for wcs_id, mic_id in pairs:
            print(f"\n🔍 Comparing DB order {wcs_id} ↔ {mic_id}")

            # Use common_utils.fetch_xml_by_id
            wcs_xml = common_utils.fetch_xml_by_id(conn, wcs_id)
            mic_xml = common_utils.fetch_xml_by_id(conn, mic_id)

            if not wcs_xml or not mic_xml:
                print(f"📢 Info: Missing XML for WCS ID '{wcs_id}' or Microservice ID '{mic_id}'. Skipping this pair.")
                if not wcs_xml: print(f"  - WCS XML for ID '{wcs_id}' not found.")
                if not mic_xml: print(f"  - Microservice XML for ID '{mic_id}' not found.")
                continue

            try:
                wcs_root = ET.fromstring(wcs_xml)
                mic_root = ET.fromstring(mic_xml)
            except ET.ParseError as e:
                print(f"🛑 XML parse error for WCS ID '{wcs_id}' or Microservice ID '{mic_id}': {e}")
                continue
            except Exception as e: # Catch other potential errors during parsing
                print(f"🛑 An unexpected error occurred while parsing XML for IDs '{wcs_id}', '{mic_id}': {e}")
                continue

            # Use common_utils functions for flattening and comparing
            wcs_dict = common_utils.flatten_elements(wcs_root)
            mic_dict = common_utils.flatten_elements(mic_root)
            diffs_for_pair = common_utils.compare_dicts(wcs_dict, mic_dict, excluded)

            for attribute_path, diff_type, wcs_val, mic_val in diffs_for_pair:
                # Prepend order IDs to the attribute/path for context
                contextual_path = f"Order {wcs_id} vs {mic_id}::{attribute_path}"
                all_diffs_for_csv.append((contextual_path, diff_type, wcs_val, mic_val))

    except mysql.connector.Error as err:
        print(f"🛑 Database connection error: {err}")
        # No need to return here, finally block will handle conn.close()
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
    # Ensure common_utils.py and config.py are accessible.
    # For direct execution, relative imports might need adjustment (e.g., using sys.path.append)

    print("Running XML Database Comparison (Case 2)")

    # It's complex to set up a live DB for a simple test script.
    # This __main__ block will assume the DB is configured and accessible
    # as per config.DB_CONFIG and that common_utils.EXCLUDED_ATTRIBUTES_FILE
    # and config.DB_ORDERS_XML_CSV are correctly set up.

    # Create dummy EXCLUDED_ATTRIBUTES_FILE if it doesn't exist
    if not hasattr(common_utils, 'EXCLUDED_ATTRIBUTES_FILE'):
        print("Warning: common_utils.EXCLUDED_ATTRIBUTES_FILE not defined. Creating a dummy one.")
        # This assumes EXCLUDED_ATTRIBUTES_FILE is a simple filename.
        # If it's a path, this logic might need adjustment.
        dummy_exc_file = "dummy_ignore_attributes.csv"
        common_utils.EXCLUDED_ATTRIBUTES_FILE = dummy_exc_file # Temporarily assign for test
        if not os.path.exists(dummy_exc_file):
             with open(dummy_exc_file, 'w', newline='') as f_exc:
                writer = csv.writer(f_exc)
                writer.writerow(['Attribute'])
                writer.writerow(['LastUpdated'])
             print(f"Created dummy exclusion file: {dummy_exc_file}")
    elif not os.path.exists(common_utils.EXCLUDED_ATTRIBUTES_FILE):
        print(f"Warning: {common_utils.EXCLUDED_ATTRIBUTES_FILE} not found. Creating a dummy one.")
        with open(common_utils.EXCLUDED_ATTRIBUTES_FILE, 'w', newline='') as f_exc:
            writer = csv.writer(f_exc)
            writer.writerow(['Attribute'])
            writer.writerow(['LastUpdated'])
        print(f"Created dummy exclusion file: {common_utils.EXCLUDED_ATTRIBUTES_FILE}")


    # Create dummy DB_ORDERS_XML_CSV if it doesn't exist
    if not os.path.exists(config.DB_ORDERS_XML_CSV):
        with open(config.DB_ORDERS_XML_CSV, 'w', newline='') as f_csv:
            writer = csv.writer(f_csv)
            writer.writerow(['wcs_order_id', 'micro_order_id'])
            # Add some dummy IDs. These won't work unless your DB has them.
            writer.writerow(['sample_wcs_id_1', 'sample_mic_id_1'])
            writer.writerow(['sample_wcs_id_2', 'sample_mic_id_2'])
        print(f"Created dummy input CSV: {config.DB_ORDERS_XML_CSV}")
        print("Note: The dummy CSV contains placeholder order IDs. "
              "For a real test, ensure these IDs exist in your database "
              "or update the CSV with valid IDs.")

    print("Attempting to run comparison. Ensure DB is configured and accessible.")
    print(f"Using DB config: {config.DB_CONFIG.get('host', 'N/A')}, "
          f"DB: {config.DB_CONFIG.get('database', 'N/A')}, "
          f"User: {config.DB_CONFIG.get('user', 'N/A')}")
    print(f"Reading order pairs from: {config.DB_ORDERS_XML_CSV}")
    print(f"Exclusion file: {common_utils.EXCLUDED_ATTRIBUTES_FILE}")

    run_case_db_xml("all_differences_case_db_xml.csv")
    print("\nXML Database Comparison (Case 2) finished. Check 'all_differences_case_db_xml.csv'")
    print("If the output CSV is empty, it might be due to DB connection issues, "
          "missing order IDs in the DB, or no differences found.")
