import os
import xml.etree.ElementTree as ET
import csv
from . import common_utils  # Assuming common_utils.py is in the same directory
from . import config      # Assuming config.py is in the same directory

def run_case_file_xml(output_csv_filename: str):
    """
    Compare XML files from filesystem pairs listed in a CSV.
    CSV columns must include 'wcs_file' and 'micro_file' without extensions.
    """
    # Use EXCLUDED_ATTRIBUTES_FILE from common_utils
    excluded = common_utils.load_excluded_attributes(common_utils.EXCLUDED_ATTRIBUTES_FILE)
    all_diffs = []

    # Read file pairs from config.FILE_PAIRS_XML_CSV
    try:
        with open(config.FILE_PAIRS_XML_CSV, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            if rdr.fieldnames is None:
                print(f"🛑 Error: CSV file {config.FILE_PAIRS_XML_CSV} might be empty or not a valid CSV.")
                return # Exit if fieldnames can't be read

            required_fields = {"wcs_file", "micro_file"}
            if not required_fields.issubset(set(rdr.fieldnames)):
                # Try to read fieldnames again, as sometimes the first check might consume them
                # This is a defensive check.
                f.seek(0) # Reset reader to the beginning
                header = next(rdr, None) # Read header row
                if header is None or not required_fields.issubset(set(header)):
                     raise ValueError(f"CSV {config.FILE_PAIRS_XML_CSV} must have columns: {required_fields}")
                # Re-initialize DictReader if header was consumed
                f.seek(0)
                rdr = csv.DictReader(f)


            for row_number, row in enumerate(rdr, start=1): # start=1 for header if not using next() before
                # Skip header row if DictReader didn't consume it (it usually does)
                if row_number == 1 and row['wcs_file'] == 'wcs_file' and row['micro_file'] == 'micro_file': # Heuristic for header
                    continue

                wcs_file_name = row.get("wcs_file")
                micro_file_name = row.get("micro_file")

                if not wcs_file_name or not micro_file_name:
                    print(f"⚠️ Warning: Skipping row {row_number} due to missing 'wcs_file' or 'micro_file' in {config.FILE_PAIRS_XML_CSV}")
                    continue

                # Use config.XML_FOLDER for paths
                wcs_path = os.path.join(config.XML_FOLDER, wcs_file_name + ".xml")
                mic_path = os.path.join(config.XML_FOLDER, micro_file_name + ".xml")

                print(f"\n🔍 Comparing {wcs_file_name} ↔ {micro_file_name}")

                try:
                    wcs_root = ET.parse(wcs_path).getroot()
                    mic_root = ET.parse(mic_path).getroot()
                except ET.ParseError as e:
                    print(f"🛑 XML parse error in {wcs_path} or {mic_path}: {e}")
                    continue
                except FileNotFoundError as e:
                    print(f"🛑 File not found error: {e}")
                    continue
                except Exception as e: # Catch other potential errors during parsing
                    print(f"🛑 An unexpected error occurred while processing files {wcs_path} or {mic_path}: {e}")
                    continue

                # Use common_utils functions
                wcs_dict = common_utils.flatten_elements(wcs_root)
                mic_dict = common_utils.flatten_elements(mic_root)
                diffs_for_pair = common_utils.compare_dicts(wcs_dict, mic_dict, excluded)

                # Add context (filenames) to each difference
                contextual_diffs = []
                for diff_item in diffs_for_pair:
                    # Prepend filenames to the existing diff tuple
                    contextual_diffs.append(
                        (wcs_file_name, micro_file_name) + diff_item
                    )
                all_diffs.extend(contextual_diffs)

    except FileNotFoundError:
        print(f"🛑 Error: Input CSV file {config.FILE_PAIRS_XML_CSV} not found.")
        return
    except ValueError as e: # Catch ValueError from missing columns
        print(f"🛑 Error: {e}")
        return
    except Exception as e: # Catch other potential errors during CSV processing
        print(f"🛑 An unexpected error occurred while reading {config.FILE_PAIRS_XML_CSV}: {e}")
        return

    # Use common_utils.write_csv for output
    # Modify header for contextual diffs
    header_row = ["WCS File", "Microservice File", "Attribute/Path", "Difference Type", "WCS Value", "Microservice Value"]
    common_utils.write_csv(all_diffs, output_csv_filename, header=header_row)

if __name__ == '__main__':
    # Example usage:
    # This part is for direct testing of the module and might be removed or modified
    # Ensure common_utils.py and config.py are accessible in the PYTHONPATH
    # or modify imports if running as a script in a different way.

    # For direct execution, Python might not handle relative imports like ". common_utils"
    # correctly if this file is the top-level script.
    # One way to handle this for testing is to temporarily add the parent directory to sys.path:
    # import sys
    # sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    # from src import common_utils
    # from src import config

    print("Running XML File Comparison (Case 1)")
    # Create dummy files and CSV for testing if they don't exist
    # Ensure XML_FOLDER from config exists
    if not os.path.exists(config.XML_FOLDER):
        os.makedirs(config.XML_FOLDER)
        print(f"Created dummy XML_FOLDER: {config.XML_FOLDER}")

    # Create dummy EXCLUDED_ATTRIBUTES_FILE if it doesn't exist in common_utils path
    # This depends on how common_utils.EXCLUDED_ATTRIBUTES_FILE is defined (e.g., relative to common_utils.py or an absolute path)
    # For this example, assuming common_utils.EXCLUDED_ATTRIBUTES_FILE is just a filename.
    if not os.path.exists(common_utils.EXCLUDED_ATTRIBUTES_FILE):
        with open(common_utils.EXCLUDED_ATTRIBUTES_FILE, 'w', newline='') as f_exc:
            writer = csv.writer(f_exc)
            writer.writerow(['Attribute'])
            writer.writerow(['Timestamp']) # Example excluded attribute
        print(f"Created dummy exclusion file: {common_utils.EXCLUDED_ATTRIBUTES_FILE}")

    # Create dummy FILE_PAIRS_XML_CSV
    if not os.path.exists(config.FILE_PAIRS_XML_CSV):
        with open(config.FILE_PAIRS_XML_CSV, 'w', newline='') as f_csv:
            writer = csv.writer(f_csv)
            writer.writerow(['wcs_file', 'micro_file'])
            writer.writerow(['test_wcs1', 'test_mic1'])
            writer.writerow(['test_wcs2', 'test_mic2'])
        print(f"Created dummy input CSV: {config.FILE_PAIRS_XML_CSV}")
        # Create dummy XML files for the CSV
        with open(os.path.join(config.XML_FOLDER, "test_wcs1.xml"), 'w') as f_xml:
            f_xml.write("<root><item name='A'>Value1</item><Timestamp>2023</Timestamp></root>")
        with open(os.path.join(config.XML_FOLDER, "test_mic1.xml"), 'w') as f_xml:
            f_xml.write("<root><item name='A'>Value2</item><Timestamp>2024</Timestamp></root>")
        with open(os.path.join(config.XML_FOLDER, "test_wcs2.xml"), 'w') as f_xml:
            f_xml.write("<root><item name='B'>OnlyWCS</item></root>")
        with open(os.path.join(config.XML_FOLDER, "test_mic2.xml"), 'w') as f_xml:
            f_xml.write("<root><item name='C'>OnlyMicro</item></root>")
        print(f"Created dummy XML files in {config.XML_FOLDER}")

    run_case_file_xml("all_differences_case_file_xml.csv")
    print("\nXML File Comparison (Case 1) finished. Check 'all_differences_case_file_xml.csv'")

# Note: The original write_csv in common_utils might need adjustment
# if it doesn't accept a custom header.
# The provided common_utils.write_csv takes (rows, out_path).
# It needs to be:
# def write_csv(rows, out_path, header=None):
#     """Write differences to CSV file"""
#     with open(out_path, "w", newline="", encoding="utf-8") as f:
#         w = csv.writer(f)
#         if header:
#             w.writerow(header)
#         else: # Default header if none provided
#             w.writerow(["attribute", "difference type", "wcs value", "microservice value"])
#         w.writerows(rows)
#     print(f"\n✅ {len(rows)} difference rows written ➜ {out_path}")
# This change should be made in common_utils.py in a separate step.
# For this subtask, I'm assuming common_utils.write_csv can handle a custom header or will be updated.
# For now, I've added a default header to write_csv, which means the custom header here is not strictly needed
# unless common_utils.write_csv is changed to require it.
# The current implementation of common_utils.write_csv has a hardcoded header.
# I will adjust the call to write_csv to pass the header as a named argument if possible,
# or assume it will be fixed.
# For now, I will call common_utils.write_csv(all_diffs, output_csv_filename)
# and the header will be the one hardcoded in common_utils.
# The task states "It should use common_utils.write_csv to write the results."
# The provided common_utils.write_csv has a hardcoded header:
# w.writerow(["attribute", "difference type", "wcs value", "microservice value"])
# My contextual_diffs adds two leading columns. This means the header in common_utils.write_csv
# will not match the data written by this new function.
# This needs to be reconciled.
# EITHER: common_utils.write_csv is made more flexible (accepting custom headers)
# OR: run_case_file_xml prepares data that matches the fixed header in common_utils.write_csv
#
# For this subtask, I will modify the diff format to match the existing common_utils.write_csv header.
# This means the context (filenames) will be part of the "attribute/path" string.

# Re-defining the function with corrected diff format to match common_utils.write_csv header.
def run_case_file_xml_corrected(output_csv_filename: str):
    """
    Compare XML files from filesystem pairs listed in a CSV.
    CSV columns must include 'wcs_file' and 'micro_file' without extensions.
    This version formats diffs to match the header in common_utils.write_csv.
    """
    excluded = common_utils.load_excluded_attributes(common_utils.EXCLUDED_ATTRIBUTES_FILE)
    all_diffs_for_csv = []

    try:
        with open(config.FILE_PAIRS_XML_CSV, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            if rdr.fieldnames is None: # Check if CSV is empty or not valid
                print(f"🛑 Error: CSV file '{config.FILE_PAIRS_XML_CSV}' is empty or not a valid CSV.")
                return

            required_fields = {"wcs_file", "micro_file"}
            if not required_fields.issubset(rdr.fieldnames):
                 raise ValueError(f"CSV '{config.FILE_PAIRS_XML_CSV}' must have columns: {required_fields}")

            for row_number, row in enumerate(rdr, start=2): # start=2 because DictReader consumes header
                wcs_file_name = row.get("wcs_file")
                micro_file_name = row.get("micro_file")

                if not wcs_file_name or not micro_file_name:
                    print(f"⚠️ Warning: Skipping row {row_number} due to missing 'wcs_file' or 'micro_file' in '{config.FILE_PAIRS_XML_CSV}'")
                    continue

                wcs_path = os.path.join(config.XML_FOLDER, wcs_file_name + ".xml")
                mic_path = os.path.join(config.XML_FOLDER, micro_file_name + ".xml")

                print(f"\n🔍 Comparing {wcs_file_name} ↔ {micro_file_name}")

                try:
                    wcs_root = ET.parse(wcs_path).getroot()
                    mic_root = ET.parse(mic_path).getroot()
                except ET.ParseError as e:
                    print(f"🛑 XML parse error in '{wcs_path}' or '{mic_path}': {e}")
                    continue
                except FileNotFoundError as e:
                    print(f"🛑 File not found error: {e}")
                    continue
                except Exception as e:
                    print(f"🛑 An unexpected error occurred while processing files '{wcs_path}' or '{mic_path}': {e}")
                    continue

                wcs_dict = common_utils.flatten_elements(wcs_root)
                mic_dict = common_utils.flatten_elements(mic_root)
                diffs_for_pair = common_utils.compare_dicts(wcs_dict, mic_dict, excluded)

                for attribute_path, diff_type, wcs_val, mic_val in diffs_for_pair:
                    # Prepend filenames to the attribute/path for context
                    contextual_path = f"{wcs_file_name} vs {micro_file_name}::{attribute_path}"
                    all_diffs_for_csv.append((contextual_path, diff_type, wcs_val, mic_val))

    except FileNotFoundError:
        print(f"🛑 Error: Input CSV file '{config.FILE_PAIRS_XML_CSV}' not found.")
        return
    except ValueError as e:
        print(f"🛑 Error: {e}")
        return
    except Exception as e:
        print(f"🛑 An unexpected error occurred while reading '{config.FILE_PAIRS_XML_CSV}': {e}")
        return

    # common_utils.write_csv uses a fixed header:
    # ["attribute", "difference type", "wcs value", "microservice value"]
    common_utils.write_csv(all_diffs_for_csv, output_csv_filename)

# Replace the original function definition with the corrected one
run_case_file_xml = run_case_file_xml_corrected

# The __main__ block needs to be updated to use the corrected function name if changed,
# or ensure the new function is assigned to the old name if that's preferred.
# For now, I've reassigned run_case_file_xml = run_case_file_xml_corrected
# so the __main__ block should still work as intended.
# The dummy file creation in __main__ is helpful for testing but might be extensive for a typical module.
# It's included here as it was part of my thought process for ensuring the function works.
# In a real-world scenario, this would be part of a separate test suite.
