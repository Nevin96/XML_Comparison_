# Main script to run XML/JSON comparison based on user input.

# No specific module imports like csv, json, os, ET, mysql.connector are needed here
# as that functionality is now within the imported case modules or common_utils.
# input() and print() are built-in.

from . import config # To ensure config is loaded, though main doesn't directly use it.
                     # Case modules will use it.
from . import case_file_xml
from . import case_db_xml
from . import case_file_json
from . import case_db_json

def main():
    print("Select source:\n 1 – File System\n 2 – Database")
    source_choice = input("Enter 1 or 2: ").strip()

    if source_choice not in ("1", "2"):
        print("Invalid source selection. Exiting.")
        return

    print("\nSelect data format:\n 1 – XML\n 2 – JSON")
    format_choice = input("Enter 1 or 2: ").strip()

    if format_choice not in ("1", "2"):
        print("Invalid format selection. Exiting.")
        return

    output_filename = ""
    operation_performed = False

    if source_choice == "1" and format_choice == "1":
        output_filename = "all_differences_file_xml.csv"
        print(f"\nRunning File System XML comparison. Output will be in '{output_filename}'")
        case_file_xml.run_case_file_xml(output_csv_filename=output_filename)
        operation_performed = True
    elif source_choice == "2" and format_choice == "1":
        output_filename = "all_differences_db_xml.csv"
        print(f"\nRunning Database XML comparison. Output will be in '{output_filename}'")
        case_db_xml.run_case_db_xml(output_csv_filename=output_filename)
        operation_performed = True
    elif source_choice == "1" and format_choice == "2":
        output_filename = "all_differences_file_json.csv"
        print(f"\nRunning File System JSON comparison. Output will be in '{output_filename}'")
        case_file_json.run_case_file_json(output_csv_filename=output_filename)
        operation_performed = True
    elif source_choice == "2" and format_choice == "2":
        output_filename = "all_differences_db_json.csv"
        print(f"\nRunning Database JSON comparison. Output will be in '{output_filename}'")
        case_db_json.run_case_db_json(output_csv_filename=output_filename)
        operation_performed = True

    if operation_performed:
        print(f"\nOperation finished. Please check '{output_filename}' for results.")
    else:
        # This case should not be reached if input validation is correct
        print("No operation performed due to invalid choices.")

if __name__ == "__main__":
    # This makes the script executable and also allows the modules to be imported
    # without running main() automatically if they are part of a larger package.
    # For example, if another script imports `comparison` for some reason,
    # `main()` won't run unless that script explicitly calls `comparison.main()`.
    main()
