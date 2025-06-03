import xml.etree.ElementTree as ET
import csv
import os
import mysql.connector
from collections import defaultdict

# --- Common Utility Functions ---
def simplify_tag_path(path):
    return path.split("/")[-1] if "/" in path else path

# --- FILE-BASED COMPARISON ---
def parse_xml_with_lines(file_path):
    lines_by_path = {}
    parser = ET.iterparse(file_path, events=("start",))
    path_stack = []

    with open(file_path, "r", encoding="utf-8") as f:
        all_lines = f.readlines()

    for event, elem in parser:
        path_stack.append(elem.tag)
        path = "/" + "/".join(path_stack)
        lines_by_path[path] = {
            "attrib": dict(elem.attrib),
            "text": (elem.text or "").strip(),
            "line": next((line.strip() for line in all_lines if f"<{elem.tag}" in line or f"</{elem.tag}>" in line), "")
        }
        path_stack.pop()

    return lines_by_path

def compare_xml_with_lines(good, bad, order_pair=None):
    differences = []
    for key in good:
        simple_tag = simplify_tag_path(key)

        if key not in bad:
            differences.append({
                "Difference Type": "Tag missing",

                "Attribute": "-",
                "Good Value": good[key]['text'],
                "Bad Value": ""
            })
        else:
            for attr in good[key]['attrib']:
                if attr not in bad[key]['attrib']:
                    differences.append({
                        "Difference Type": "Attribute missing",
                        "Attribute": attr,
                        "Good Value": good[key]['attrib'][attr],
                        "Bad Value": ""
                    })
                elif good[key]['attrib'][attr] != bad[key]['attrib'][attr]:
                    differences.append({
                        "Difference Type": "Attribute mismatch",
                        "Attribute": attr,
                        "Good Value": good[key]['attrib'][attr],
                        "Bad Value": bad[key]['attrib'][attr]
                    })

            if good[key]['text'] != bad[key]['text']:
                differences.append({
                    "Difference Type": "Text mismatch",
                    "Attribute": "(text)",
                    "Good Value": good[key]['text'],
                    "Bad Value": bad[key]['text']
                })

    for key in bad:
        if key not in good:
            simple_tag = simplify_tag_path(key)
            differences.append({
                "Difference Type": "Extra tag",
                "Attribute": "-",
                "Good Value": "",
                "Bad Value": bad[key]['text']
            })

    return differences




# --- DATABASE-BASED COMPARISON ---
def get_xml_by_order_id(connection, order_id):
    cursor = connection.cursor()
    cursor.execute("SELECT xml_content FROM orders WHERE order_id = %s", (order_id,))
    result = cursor.fetchone()
    return result[0] if result else None

def parse_xml_from_string(xml_string):
    try:
        return ET.fromstring(xml_string), None
    except ET.ParseError as e:
        return None, f"XML ParseError: {e}"

def flatten_elements(root):
    elements = {}
    def recurse(element):
        tag = element.tag
        elements.setdefault(tag, []).append({
            "attrib": element.attrib,
            "text": (element.text or "").strip()
        })
        for child in element:
            recurse(child)
    recurse(root)
    return elements

def compare_xml_database(good_elements, bad_elements):
    differences = []
    for tag in good_elements:
        good_items = good_elements[tag]
        bad_items = bad_elements.get(tag, [])
        for idx, good_item in enumerate(good_items):
            if idx >= len(bad_items):
                differences.append({
                    "Difference Type": "Tag missing",
                    "Attribute": "-",
                    "Good Value": good_item['text'],
                    "Bad Value": ""
                })
                continue

            bad_item = bad_items[idx]
            for attr in good_item['attrib']:
                if attr not in bad_item['attrib']:
                    differences.append({
                        "Difference Type": "Attribute missing",
                        "Attribute": attr,
                        "Good Value": good_item['attrib'][attr],
                        "Bad Value": ""
                    })
                elif good_item['attrib'][attr] != bad_item['attrib'][attr]:
                    differences.append({
                        "Difference Type": "Attribute mismatch",
                        "Attribute": attr,
                        "Good Value": good_item['attrib'][attr],
                        "Bad Value": bad_item['attrib'][attr]
                    })

            if good_item['text'] != bad_item['text']:
                differences.append({
                    "Difference Type": "Text mismatch",
                    "Attribute": "(text)",
                    "Good Value": good_item['text'],
                    "Bad Value": bad_item['text']
                })

    for tag in bad_elements:
        if tag not in good_elements:
            for item in bad_elements[tag]:
                differences.append({
                    "Difference Type": "Extra tag",
                    "Attribute": "-",
                    "Good Value": "",
                    "Bad Value": item['text']
                })

    return differences



def process_file_pairs(input_csv, xml_folder):
    all_differences = []
    with open(input_csv, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            base_good = row["wcs_file"]
            base_bad = row["micro_file"]
            good_file = os.path.join(xml_folder, base_good + ".xml")
            bad_file = os.path.join(xml_folder, base_bad + ".xml")

            if not os.path.exists(good_file) or not os.path.exists(bad_file):
                print(f"❌ File missing: {good_file} or {bad_file}")
                continue

            order_pair = f"{base_good}-{base_bad}"
            good_elements = parse_xml_with_lines(good_file)
            bad_elements = parse_xml_with_lines(bad_file)

            diffs = compare_xml_with_lines(good_elements, bad_elements)
            all_differences.extend(diffs)

    return all_differences


def process_database_pairs(csv_filename):
    connection = mysql.connector.connect(
        host="localhost", user="root", password="Nevin@134", database="xml5"
    )
    all_differences = []
    with open(csv_filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            wcs_id, micro_id = row['wcs_order_id'], row['micro_order_id']
            wcs_xml = get_xml_by_order_id(connection, wcs_id)
            micro_xml = get_xml_by_order_id(connection, micro_id)
            pair_label = f"{wcs_id}-{micro_id}"

            if not wcs_xml or not micro_xml:
                all_differences.append((pair_label, [{"Difference Type": "Missing XML", "Tag Path": "N/A", "Attribute": f"Missing for {'WCS' if not wcs_xml else 'Micro'}"}]))
                continue

            good_root, good_err = parse_xml_from_string(wcs_xml)
            bad_root, bad_err = parse_xml_from_string(micro_xml)

            pair_diffs = []
            if good_err:
                pair_diffs.append({"Difference Type": "Parse error", "Tag": "WCS", "Attribute": good_err})
            if bad_err:
                pair_diffs.append({"Difference Type": "Parse error", "Tag": "Micro", "Attribute": bad_err})

            if not good_err and not bad_err:
                good_elements = flatten_elements(good_root)
                bad_elements = flatten_elements(bad_root)
                pair_diffs.extend(compare_xml_database(good_elements, bad_elements))

            all_differences.append((pair_label, pair_diffs))
    connection.close()
    # Flatten differences
    flat = []
    for pair_id, diffs in all_differences:
        for diff in diffs:
            flat.append({**diff, "Order Pair": pair_id})
    return flat

# --- CSV WRITER ---
def write_csv(differences, csv_file):
    fieldnames = [ "Attribute", "Difference Type", "Good Value", "Bad Value"]
    with open(csv_file, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for diff in differences:
            writer.writerow({
                "Attribute": diff.get("Attribute", ""),
                "Difference Type": diff.get("Difference Type", ""),
                "Good Value": diff.get("Good Value", ""),
                "Bad Value": diff.get("Bad Value", "")
            })



# --- MAIN ENTRY POINT ---
if __name__ == "__main__":
    print("Choose comparison mode:")
    print("1. Compare files")
    print("2. Compare from database")
    choice = input("Enter 1 or 2: ").strip()

    if choice == "1":
        xml_folder = r"D:\litmus7\pro\xml_files"  # Hardcoded XML folder path
        file_pairs_csv = "file_pairs.csv"          # Hardcoded CSV filename for files
        diffs = process_file_pairs(file_pairs_csv, xml_folder)
        write_csv(diffs, "differences_summary.csv")
        print("✅ Differences saved to differences_summary.csv")

    elif choice == "2":
        db_pairs_csv = "orders_to_compare.csv"    # Hardcoded CSV filename for DB order pairs
        diffs = process_database_pairs(db_pairs_csv)
        write_csv(diffs, "order_compare_xml_differences.csv")
        print("✅ Differences saved to order_compare_xml_differences.csv")
    else:
        print("❌ Invalid choice.")

