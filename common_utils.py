import xml.etree.ElementTree as ET
import csv
import json
import mysql.connector # Not strictly necessary here but good for type hinting if used
import os # For EXCLUDED_ATTRIBUTES_FILE path construction if needed later

# Constants
EXCLUDED_ATTRIBUTES_FILE = "ignore_attributes.csv" # Will be defined here for now
TAG_MAPPING = {}
ATTR_MAPPING = {}
IGNORE_TAGS = {
    "ApplicationArea", "Process", "ActionCriteria", "ActionExpression",
}
DEBUG = False

# Functions
def strip_ns(tag):
    """Remove namespace from XML tag"""
    return tag.split('}', 1)[-1] if '}' in tag else tag

def canonical_tag(local):
    """Return canonical form of tag, fallback to original"""
    return TAG_MAPPING.get(local, local)

def canonical_attr(local):
    """Return canonical form of attribute, fallback to original"""
    result = ATTR_MAPPING.get(local, local)
    if DEBUG:
        print(f"Canonical attribute for '{local}': '{result}'")
    return result

def flatten_elements(root: ET.Element) -> dict:
    """
    Flatten an XML element tree into dictionary of paths mapping to attribute/text data
    """
    elements = {}

    def recurse(elem: ET.Element, path="", sib_counter=None):
        if sib_counter is None:
            sib_counter = {}

        local = strip_ns(elem.tag)
        canon = canonical_tag(local)
        if canon in IGNORE_TAGS:
            if DEBUG:
                print(f"Ignoring tag: {canon}")
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
            "text": (elem.text or "").strip(),
            "tag": canon  # Store the canonical tag name for reference
        }

        child_counts = {}
        for child in elem:
            recurse(child, new_path, child_counts)

    recurse(root)
    if DEBUG:
        print("Flattened elements (sample):")
        for i, (k,v) in enumerate(elements.items()):
            print(f"{k}: {v}")
            if i>=5:
                break
    return elements

def flatten_json(obj, path=""):
    """Flatten nested JSON into dictionary of paths to values"""
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
    """Compare two JSON objects returning list of differences"""
    flat1 = flatten_json(json1)
    flat2 = flatten_json(json2)
    diffs = []

    keys = set(flat1.keys()).union(set(flat2.keys()))
    for key in keys:
        val1 = flat1.get(key, "-")
        val2 = flat2.get(key, "-")
        if val1 != val2:
            if key in flat1 and key in flat2:
                diff_type = "Value mismatch"
            else:
                diff_type = "Missing key"
            diffs.append((key, diff_type, val1, val2))
    return diffs

def load_excluded_attributes(csv_path=EXCLUDED_ATTRIBUTES_FILE):
    """Load set of excluded attributes from CSV file"""
    excluded = set()
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                # Handle both 'attribute' and 'Attribute' column names
                attr_key = 'attribute' if 'attribute' in row else 'Attribute'
                if attr_key in row and row[attr_key].strip():
                    attr = canonical_attr(row[attr_key].strip())
                    excluded.add(attr)
                    if DEBUG:
                        print(f"Excluded attribute loaded: '{attr}'")
    except FileNotFoundError:
        print(f"  Attribute-exclusion file not found: {csv_path}")
    except Exception as e:
        print(f"  Error loading excluded attributes: {e}")

    print(f"Loaded {len(excluded)} excluded attributes: {excluded}")
    return excluded

def should_exclude_element(elem_data, excluded_attrs):
    """Check if an element should be excluded based on its tag name or attributes"""
    # Check if the tag itself is in excluded attributes
    tag_name = elem_data.get("tag", "")
    if tag_name in excluded_attrs:
        return True

    # Check if element has a 'name' attribute that's in excluded list
    name_attr = elem_data.get("attrib", {}).get("name", "")
    if name_attr in excluded_attrs:
        return True

    return False

def compare_dicts(wcs_dict: dict, micro_dict: dict, excluded_attrs: set):
    """Compare two flattened XML dicts, ignoring excluded attributes"""
    diffs = []

    # Filter out excluded elements from both dictionaries
    filtered_wcs = {k: v for k, v in wcs_dict.items() if not should_exclude_element(v, excluded_attrs)}
    filtered_micro = {k: v for k, v in micro_dict.items() if not should_exclude_element(v, excluded_attrs)}

    if DEBUG:
        print(f"Original WCS elements: {len(wcs_dict)}, Filtered: {len(filtered_wcs)}")
        print(f"Original Micro elements: {len(micro_dict)}, Filtered: {len(filtered_micro)}")

    for path, wcs_elem in filtered_wcs.items():
        if path not in filtered_micro:
            name_hint = wcs_elem["attrib"].get("name", path.split("/")[-1])
            # Double-check exclusion at difference level
            if name_hint not in excluded_attrs and wcs_elem.get("tag", "") not in excluded_attrs:
                diffs.append((name_hint, "Tag missing", wcs_elem["text"], "-"))
            continue

        mic_elem = filtered_micro[path]

        # Compare attributes
        for attr, wcs_val in wcs_elem["attrib"].items():
            if attr in excluded_attrs:
                if DEBUG:
                    print(f"Excluding attribute '{attr}' from comparison")
                continue
            mic_val = mic_elem["attrib"].get(attr)
            if mic_val is None:
                diffs.append((attr, "Attribute missing", wcs_val, "-"))
            elif mic_val != wcs_val:
                diffs.append((attr, "Attribute mismatch", wcs_val, mic_val))

        # Compare text content
        if wcs_elem["text"] != mic_elem["text"]:
            diffs.append(("(text)", "Text mismatch", wcs_elem["text"], mic_elem["text"]))

    # Check for extra elements in micro
    for path, mic_elem in filtered_micro.items():
        if path not in filtered_wcs:
            name_hint = mic_elem["attrib"].get("name", path.split("/")[-1])
            # Double-check exclusion at difference level
            if name_hint not in excluded_attrs and mic_elem.get("tag", "") not in excluded_attrs:
                diffs.append((name_hint, "Extra tag", "-", mic_elem["text"]))

    return diffs

def fetch_xml_by_id(conn, order_id):
    """Fetch XML content string by order_id from DB"""
    cur = conn.cursor()
    cur.execute("SELECT xml_content FROM orders WHERE order_id=%s", (order_id,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None

def fetch_json(conn, order_id):
    """Fetch JSON content string or dict by order_id from DB"""
    cur = conn.cursor()
    cur.execute("SELECT json_content FROM orders_json WHERE order_id=%s", (order_id,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None

def write_csv(rows, out_path):
    """Write differences to CSV file"""
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["attribute", "difference type", "wcs value", "microservice value"])
        w.writerows(rows)
    print(f"\n✅ {len(rows)} difference rows written ➜ {out_path}")
