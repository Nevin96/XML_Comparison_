import xml.etree.ElementTree as ET
import csv
import os
import mysql.connector
import json

XML_FOLDER = r"D:\litmus7\pro\xml_files"
EXCLUDED_ATTRIBUTES_FILE = "ignore_attributes.csv"
INPUT_CSV_CASE1 = "file_pairs.csv"
ORDER_PAIR_CSV = "orders_to_compare.csv"
DB_CONFIG_xml = dict(host="localhost", user="root", password="004", database="xml5")
DB_CONFIG_json = dict(host="localhost", user="root", password="004", database="json_db")
INPUT_CSV_JSON = "input_json.csv"
ORDER_PAIR_JSON       = "orders_to_compare_json.csv" 
DEBUG = False

def strip_ns(tag):
    return tag.split('}', 1)[-1] if '}' in tag else tag

TAG_MAPPING = {}
ATTR_MAPPING = {}
IGNORE_TAGS = {
    "ApplicationArea", "Process", "ActionCriteria", "ActionExpression",
}

def canonical_tag(local): return TAG_MAPPING.get(local, local)
def canonical_attr(local): return ATTR_MAPPING.get(local, local)

def flatten_elements(root: ET.Element) -> dict:
    elements = {}

    def recurse(elem: ET.Element, path="", sib_counter=None):
        if sib_counter is None:
            sib_counter = {}

        local = strip_ns(elem.tag)
        canon = canonical_tag(local)
        if canon in IGNORE_TAGS:
            return

        attribs = {canonical_attr(strip_ns(k)): v for k, v in elem.attrib.items()}
        name_attr = attribs.get("name")

        if canon in {"ProtocolData", "UserDataField"} and name_attr:
            new_path = f"{path}/{canon}[@name='{name_attr}']"
        else:
            idx = sib_counter.get(canon, 0) + 1
            sib_counter[canon] = idx
            new_path = f"{path}/{canon}[{idx}]" if path else f"/{canon}[{idx}]"

        elements[new_path] = {
            "attrib": attribs,
            "text": (elem.text or "").strip()
        }

        child_counts = {}
        for child in elem:
            recurse(child, new_path, child_counts)

    recurse(root)
    if DEBUG:
        print("Flattened elements:", list(elements.items())[:5])
    return elements

def flatten_json(obj, path=""):
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_path = f"{path}.{k}" if path else k
            items.update(flatten_json(v, new_path))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            new_path = f"{path}[{i}]"
            items.update(flatten_json(v, new_path))
    else:
        items[path] = str(obj)
    return items

def compare_json_files(json1, json2):
    flat1 = flatten_json(json1)
    flat2 = flatten_json(json2)
    diffs = []

    keys = set(flat1.keys()).union(set(flat2.keys()))
    for key in keys:
        val1 = flat1.get(key, "-")
        val2 = flat2.get(key, "-")
        if val1 != val2:
            diffs.append((key, "Value mismatch" if key in flat1 and key in flat2 else "Missing key", val1, val2))
    return diffs

def load_excluded_attributes(csv_path=EXCLUDED_ATTRIBUTES_FILE):
    excluded = set()
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                if 'attribute' in row and row['attribute'].strip():
                    excluded.add(canonical_attr(row['attribute'].strip()))
    except FileNotFoundError:
        print(f"  Attribute-exclusion file not found: {csv_path}")
    return excluded

def compare_dicts(wcs_dict: dict, micro_dict: dict, excluded_attrs: set):
    diffs = []

    for path, g in wcs_dict.items():
        if path not in micro_dict:
            name_hint = g["attrib"].get("name", path.split("/")[-1])
            diffs.append((name_hint, "Tag missing", g["text"], "-"))
            continue

        m = micro_dict[path]
        for attr, wcs_val in g["attrib"].items():
            if attr in excluded_attrs:
                continue
            mic_val = m["attrib"].get(attr)
            if mic_val is None:
                diffs.append((attr, "Attribute missing", wcs_val, "-"))
            elif mic_val != wcs_val:
                diffs.append((attr, "Attribute mismatch", wcs_val, mic_val))
        if g["text"] != m["text"]:
            diffs.append(("(text)", "Text mismatch", g["text"], m["text"]))

    for path in micro_dict:
        if path not in wcs_dict:
            g = micro_dict[path]
            name_hint = g["attrib"].get("name", path.split("/")[-1])
            diffs.append((name_hint, "Extra tag", "-", g["text"]))

    return diffs

def process_case1(input_csv, output_csv):
    excluded = load_excluded_attributes()
    all_diffs = []

    with open(input_csv, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        if {"wcs_file", "micro_file"} - set(rdr.fieldnames):
            raise ValueError("CSV must have 'wcs_file' and 'micro_file' columns.")
        for row in rdr:
            wcs_path = os.path.join(XML_FOLDER, row["wcs_file"] + ".xml")
            mic_path = os.path.join(XML_FOLDER, row["micro_file"] + ".xml")

            print(f"\n🔍 Comparing {row['wcs_file']} ↔ {row['micro_file']}")

            try:
                wcs_root = ET.parse(wcs_path).getroot()
                mic_root = ET.parse(mic_path).getroot()
            except ET.ParseError as e:
                print("🛑 XML parse error:", e)
                continue

            wcs_dict = flatten_elements(wcs_root)
            mic_dict = flatten_elements(mic_root)
            all_diffs.extend(compare_dicts(wcs_dict, mic_dict, excluded))

    write_csv(all_diffs, output_csv)

def fetch_xml_by_id(conn, order_id):
    cur = conn.cursor()
    cur.execute("SELECT xml_content FROM orders WHERE order_id=%s", (order_id,))
    row = cur.fetchone()
    return row[0] if row else None

def fetch_json(conn, order_id):
    cur = conn.cursor()
    cur.execute("SELECT json_content FROM orders WHERE order_id=%s", (order_id,))
    row = cur.fetchone()
    return row[0] if row else None

def process_case2(output_csv):
    excluded = load_excluded_attributes()
    conn = mysql.connector.connect(**DB_CONFIG_xml)

    with open(ORDER_PAIR_CSV, newline="") as f:
        rdr = csv.DictReader(f)
        pairs = [(r["wcs_order_id"], r["micro_order_id"]) for r in rdr]

    all_diffs = []
    for wcs_id, mic_id in pairs:
        print(f"\n Comparing DB order {wcs_id} ↔ {mic_id}")
        wcs_xml = fetch_xml_by_id(conn, wcs_id)
        mic_xml = fetch_xml_by_id(conn, mic_id)
        if not wcs_xml or not mic_xml:
            print(" Missing XML for pair; skipping")
            continue
        try:
            wcs_root = ET.fromstring(wcs_xml)
            mic_root = ET.fromstring(mic_xml)
        except ET.ParseError as e:
            print("Parse error:", e)
            continue

        wcs_dict = flatten_elements(wcs_root)
        mic_dict = flatten_elements(mic_root)
        all_diffs.extend(compare_dicts(wcs_dict, mic_dict, excluded))

    conn.close()
    write_csv(all_diffs, output_csv)

def process_case3(input_csv, output_csv):
    all_diffs = []

    with open(input_csv, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        if {"wcs_json", "micro_json"} - set(rdr.fieldnames):
            raise ValueError("CSV must have 'wcs_json' and 'micro_json' columns.")
        for row in rdr:
            wcs_path = os.path.join(XML_FOLDER, row["wcs_json"])
            mic_path = os.path.join(XML_FOLDER, row["micro_json"])
            print(f"\n Comparing {row['wcs_json']} ↔ {row['micro_json']}")

            try:
                with open(wcs_path, "r", encoding="utf-8") as f1, open(mic_path, "r", encoding="utf-8") as f2:
                    json1 = json.load(f1)
                    json2 = json.load(f2)
            except Exception as e:
                print(" JSON parse error:", e)
                continue

            all_diffs.extend(compare_json_files(json1, json2))

    write_csv(all_diffs, output_csv)
def process_case4(out="all_differences_case4.csv", pair_csv=ORDER_PAIR_JSON):
    conn=mysql.connector.connect(**DB_CONFIG_json); diffs=[]
    for wcs_id,mic_id in [(r["wcs_order_id"],r["micro_order_id"])
                          for r in csv.DictReader(open(pair_csv,encoding="utf-8"))]:
        print(f"\n• DB-JSON {wcs_id} ↔ {mic_id}")
        j1, j2 = fetch_json(conn,wcs_id), fetch_json(conn,mic_id)
        if not j1 or not j2: print("  missing JSON – skipped"); continue
        try:
            # MySQL may return dict or str depending on driver; handle both
            if isinstance(j1,str): j1=json.loads(j1)
            if isinstance(j2,str): j2=json.loads(j2)
        except json.JSONDecodeError as e:
            print("  decode error:",e); continue
        diffs += compare_json_files(j1,j2)
    conn.close(); write_csv(diffs,out)

def write_csv(rows, out_path):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["attribute", "difference type", "wcs value", "microservice value"])
        w.writerows(rows)
    print(f"\n {len(rows)} difference rows written ➜ {out_path}")

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

    if source_choice == "1" and format_choice == "1":
        process_case1(INPUT_CSV_CASE1, "all_differences_case1.csv")  
    elif source_choice == "2" and format_choice == "1":
        process_case2("all_differences_case2.csv")                   
    elif source_choice == "1" and format_choice == "2":
        process_case3(INPUT_CSV_JSON, "all_differences_case3.csv")   
    elif source_choice == "2" and format_choice == "2":
        process_case4("all_differences_case4.csv")                  


if __name__ == "__main__":
    main()

