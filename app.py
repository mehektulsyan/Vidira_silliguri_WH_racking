# app.py
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

# ----------------------------
# Config
# ----------------------------
PART_CODES_CSV = Path("part_codes.csv")  # must contain column: part_code
LOCATIONS_SHEET_TAB = "locations"        # Google Sheet tab name
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

REQUIRED_FIELDS_NOTE = "Row, Rack, and Shelf are required. Bin is optional."

# ----------------------------
# Google Sheets helpers
# ----------------------------
@st.cache_resource
def get_gspread_client():
    sa = json.loads(st.secrets["GCP_SERVICE_ACCOUNT"])
    creds = Credentials.from_service_account_info(sa, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_resource
def get_locations_ws():
    gc = get_gspread_client()
    sh = gc.open_by_key(st.secrets["SHEET_ID"])
    return sh.worksheet(LOCATIONS_SHEET_TAB)


def ensure_locations_header(ws):
    desired = ["part_code", "row", "rack", "shelf", "bin", "updated_at", "updated_by"]
    header = ws.row_values(1)
    if header != desired:
        ws.clear()
        ws.append_row(desired)


@st.cache_data(ttl=60)
def load_locations_index():
    """
    Build a dict {part_code: row_number} by reading only the first column.
    Cached for 60 seconds to reduce API calls.
    """
    ws = get_locations_ws()
    ensure_locations_header(ws)

    col = ws.col_values(1)  # includes header
    idx = {}
    for i, code in enumerate(col[1:], start=2):  # rows start at 1; row 1 is header
        code = (code or "").strip()
        if code:
            idx[code] = i
    return idx


def fetch_location_from_sheet(part_code: str):
    ws = get_locations_ws()
    ensure_locations_header(ws)

    idx = load_locations_index()
    rownum = idx.get(part_code)
    if not rownum:
        return None

    row = ws.row_values(rownum)
    row += [""] * (7 - len(row))  # pad to A:G
    return {
        "part_code": row[0],
        "row": row[1],
        "rack": row[2],
        "shelf": row[3],
        "bin": row[4],
        "updated_at": row[5],
        "updated_by": row[6],
    }


def upsert_location_to_sheet(
    part_code: str,
    row_loc: str,
    rack: str,
    shelf: str,
    bin_val: str | None,
    updated_by: str = "",
):
    ws = get_locations_ws()
    ensure_locations_header(ws)

    idx = load_locations_index()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    values = [
        part_code,
        row_loc,
        rack,
        shelf,
        bin_val or "",
        now,
        updated_by or "",
    ]

    rownum = idx.get(part_code)
    if rownum:
        ws.update(f"A{rownum}:G{rownum}", [values])
    else:
        ws.append_row(values)

    # Clear cache so next read sees latest
    load_locations_index.clear()


# ----------------------------
# Part codes load (CSV)
# ----------------------------
@st.cache_data(show_spinner=False)
def load_part_codes(csv_path: Path) -> pd.Series:
    df = pd.read_csv(csv_path, dtype={"part_code": "string"})
    if "part_code" not in df.columns:
        raise ValueError("part_codes.csv must have a column named 'part_code'")
    codes = df["part_code"].dropna().astype(str).str.strip()
    codes = codes[codes != ""].drop_duplicates()
    return codes


def exact_match_in_series(codes: pd.Series, q: str) -> bool:
    # Series membership test that works fine at 80k scale
    return (codes == q).any()


def prefix_suggestions(codes: pd.Series, prefix: str, limit: int = 50) -> list[str]:
    if not prefix:
        return []
    # Prefix search is fast and usually enough for barcodes
    matches = codes[codes.str.startswith(prefix, na=False)].head(limit)
    return matches.tolist()


# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="Inventory Location Mapper", layout="centered")
st.title("Inventory Location Mapper (Google Sheets backend)")
st.caption("Search/scan a part code → set Row/Rack/Shelf/Bin → saved to Google Sheets.")

# Basic preflight checks
if "GCP_SERVICE_ACCOUNT" not in st.secrets or "SHEET_ID" not in st.secrets:
    st.error(
        "Missing Streamlit Secrets.\n\n"
        "You must set:\n"
        "- GCP_SERVICE_ACCOUNT (multiline JSON string)\n"
        "- SHEET_ID\n"
    )
    st.stop()

# Ensure sheet header exists early (and fail fast if permissions are wrong)
try:
    ws = get_locations_ws()
    ensure_locations_header(ws)
except Exception as e:
    st.error(
        "Could not access Google Sheet tab 'locations'.\n\n"
        "Check:\n"
        "1) SHEET_ID is correct\n"
        "2) The sheet has a tab named 'locations'\n"
        "3) You shared the sheet with the service account email as Editor\n\n"
        f"Error: {e}"
    )
    st.stop()

if not PART_CODES_CSV.exists():
    st.error("part_codes.csv not found in the repo. Add it alongside app.py.")
    st.stop()

codes = load_part_codes(PART_CODES_CSV)
st.caption(f"Loaded {len(codes):,} part codes from CSV.")

# Optional "updated_by" field (lightweight)
with st.expander("Optional: set your name (saved in updated_by)"):
    updated_by = st.text_input("Your name / initials", value=st.session_state.get("updated_by", ""))
    st.session_state["updated_by"] = updated_by.strip()
updated_by = st.session_state.get("updated_by", "")

st.divider()

query = st.text_input(
    "Scan or type part code (search)",
    placeholder="e.g., ABC123",
    help="Barcode scanners typically type into this box like a keyboard.",
)

selected_part_code = None

if query:
    q = query.strip()

    # If part code exists in CSV: allow selection immediately
    if q and exact_match_in_series(codes, q):
        selected_part_code = q
        st.success("Exact match found in master list.")
    else:
        sugg = prefix_suggestions(codes, q, limit=50)
        if sugg:
            selected_part_code = st.selectbox("Select from suggestions", options=sugg)
        else:
            st.warning("No matching part code found in master list (CSV).")
            st.info("You can add it anyway (it will be stored in the Google Sheet when you save location).")
            if st.button("Use this as a NEW part code"):
                if q:
                    selected_part_code = q
                else:
                    st.error("Please type a part code first.")

st.divider()

if selected_part_code:
    # Fetch existing mapping from Google Sheet (if any)
    existing = None
    try:
        existing = fetch_location_from_sheet(selected_part_code)
    except Exception as e:
        st.error(f"Could not read from Google Sheet. Error: {e}")
        st.stop()

    if existing:
        st.info(f"Existing location found (updated_at: {existing.get('updated_at', '')}). You can edit and save.")
        row0 = existing.get("row", "")
        rack0 = existing.get("rack", "")
        shelf0 = existing.get("shelf", "")
        bin0 = existing.get("bin", "")
    else:
        row0 = rack0 = shelf0 = bin0 = ""

    st.caption(REQUIRED_FIELDS_NOTE)

    with st.form("location_form", clear_on_submit=False):
        st.subheader(f"Set location for: {selected_part_code}")

        row_loc = st.text_input("Row *", value=row0, placeholder="Required")
        rack = st.text_input("Rack *", value=rack0, placeholder="Required")
        shelf = st.text_input("Shelf *", value=shelf0, placeholder="Required")
        bin_val = st.text_input("Bin (optional)", value=bin0, placeholder="Optional")

        submitted = st.form_submit_button("Save")

        if submitted:
            errors = []
            if not row_loc.strip():
                errors.append("Row is required.")
            if not rack.strip():
                errors.append("Rack is required.")
            if not shelf.strip():
                errors.append("Shelf is required.")

            if errors:
                for e in errors:
                    st.error(e)
            else:
                try:
                    upsert_location_to_sheet(
                        part_code=selected_part_code.strip(),
                        row_loc=row_loc.strip(),
                        rack=rack.strip(),
                        shelf=shelf.strip(),
                        bin_val=bin_val.strip() if bin_val.strip() else None,
                        updated_by=updated_by,
                    )
                    st.success("Saved successfully ✅")
                    st.toast("Saved", icon="✅")
                except Exception as e:
                    st.error(f"Could not write to Google Sheet. Error: {e}")

    with st.expander("View current saved record (from Google Sheet)"):
        current = fetch_location_from_sheet(selected_part_code)
        if current:
            st.json(current)
        else:
            st.write("No record saved yet.")

st.divider()

# Download all mappings (reads entire sheet tab; okay for moderate size)
if st.button("Download all saved mappings as CSV"):
    try:
        ws = get_locations_ws()
        ensure_locations_header(ws)
        values = ws.get_all_values()
        if len(values) <= 1:
            st.info("No mappings saved yet.")
        else:
            df = pd.DataFrame(values[1:], columns=values[0])
            st.download_button(
                "Click to download",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="inventory_locations.csv",
                mime="text/csv",
            )
    except Exception as e:
        st.error(f"Download failed. Error: {e}")
