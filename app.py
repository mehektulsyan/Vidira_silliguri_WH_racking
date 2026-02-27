import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

DB_PATH = Path("inventory_locations.db")
PART_CODES_CSV = Path("part_codes.csv")


# ----------------------------
# DB helpers
# ----------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS part_codes (
            part_code TEXT PRIMARY KEY,
            source    TEXT NOT NULL DEFAULT 'manual',  -- 'csv' or 'manual'
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS locations (
            part_code TEXT PRIMARY KEY,
            row_loc   TEXT NOT NULL,
            rack      TEXT NOT NULL,
            shelf     TEXT NOT NULL,
            bin       TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(part_code) REFERENCES part_codes(part_code)
        );
        """
    )
    conn.commit()


def seed_part_codes_from_csv(conn, codes: pd.Series):
    # Insert CSV codes once; ignore conflicts if already present
    conn.executemany(
        "INSERT OR IGNORE INTO part_codes (part_code, source) VALUES (?, 'csv')",
        [(c,) for c in codes.tolist()],
    )
    conn.commit()


def part_code_exists(conn, part_code: str) -> bool:
    cur = conn.execute("SELECT 1 FROM part_codes WHERE part_code = ? LIMIT 1", (part_code,))
    return cur.fetchone() is not None


def add_part_code(conn, part_code: str, source: str = "manual"):
    conn.execute(
        "INSERT OR IGNORE INTO part_codes (part_code, source) VALUES (?, ?)",
        (part_code, source),
    )
    conn.commit()


def upsert_location(conn, part_code, row_loc, rack, shelf, bin_val):
    conn.execute(
        """
        INSERT INTO locations (part_code, row_loc, rack, shelf, bin, updated_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(part_code) DO UPDATE SET
            row_loc = excluded.row_loc,
            rack = excluded.rack,
            shelf = excluded.shelf,
            bin = excluded.bin,
            updated_at = datetime('now');
        """,
        (part_code, row_loc, rack, shelf, bin_val),
    )
    conn.commit()


def fetch_location(conn, part_code):
    cur = conn.execute(
        "SELECT part_code, row_loc, rack, shelf, bin, updated_at FROM locations WHERE part_code = ?",
        (part_code,),
    )
    return cur.fetchone()


def search_suggestions(conn, prefix: str, limit: int = 50):
    # Prefix search in DB for fast suggestions
    cur = conn.execute(
        """
        SELECT part_code
        FROM part_codes
        WHERE part_code LIKE ? ESCAPE '\\'
        ORDER BY part_code
        LIMIT ?
        """,
        (prefix.replace("%", "\\%").replace("_", "\\_") + "%", limit),
    )
    return [r[0] for r in cur.fetchall()]


def count_part_codes(conn) -> int:
    cur = conn.execute("SELECT COUNT(*) FROM part_codes")
    return int(cur.fetchone()[0])


# ----------------------------
# Data load
# ----------------------------
@st.cache_data(show_spinner=False)
def load_part_codes_csv(csv_path: Path) -> pd.Series:
    df = pd.read_csv(csv_path, dtype={"part_code": "string"})
    if "part_code" not in df.columns:
        raise ValueError("CSV must have a column named 'part_code'")
    codes = df["part_code"].dropna().astype(str).str.strip()
    codes = codes[codes != ""].drop_duplicates()
    return codes


# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="Inventory Location Mapper", layout="centered")
st.title("Inventory Location Mapper")

conn = get_conn()
init_db(conn)

# Seed from CSV if present
if PART_CODES_CSV.exists():
    csv_codes = load_part_codes_csv(PART_CODES_CSV)
    seed_part_codes_from_csv(conn, csv_codes)
else:
    st.warning("part_codes.csv not found. You can still add codes manually.")

st.caption(f"Total part codes in system: {count_part_codes(conn):,}")

query = st.text_input(
    "Scan or type part code (search)",
    placeholder="e.g., ABC123",
    help="Barcode scanners usually type into this box like a keyboard.",
)

selected_part_code = None
exact_match = False

if query:
    q = query.strip()
    if q and part_code_exists(conn, q):
        selected_part_code = q
        exact_match = True
        st.success("Exact match found.")
    else:
        suggestions = search_suggestions(conn, q, limit=50) if q else []
        if suggestions:
            selected_part_code = st.selectbox("Select from suggestions", options=suggestions)
        else:
            st.warning("No matching part codes found.")
            # Offer to add new part code
            st.subheader("Add new part code")
            with st.form("add_part_code_form"):
                new_code = st.text_input("New part code", value=q, placeholder="Enter new unique part code")
                confirm_add = st.form_submit_button("Add part code")
                if confirm_add:
                    new_code = new_code.strip()
                    if not new_code:
                        st.error("Part code cannot be empty.")
                    else:
                        add_part_code(conn, new_code, source="manual")
                        st.success("Part code added ✅ Now assign its location below.")
                        selected_part_code = new_code  # proceed to location form

st.divider()

if selected_part_code:
    existing = fetch_location(conn, selected_part_code)

    if existing:
        _, row_loc0, rack0, shelf0, bin0, updated_at = existing
        st.info(f"Existing location found (last updated: {updated_at}). You can edit and save.")
    else:
        row_loc0 = rack0 = shelf0 = ""
        bin0 = ""

    with st.form("location_form", clear_on_submit=False):
        st.subheader(f"Set location for: {selected_part_code}")

        row_loc = st.text_input("Row *", value=row_loc0, placeholder="Required")
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
                # ensure part code exists in master table (in case it came from manual flow)
                add_part_code(conn, selected_part_code.strip(), source="manual")
                upsert_location(
                    conn,
                    selected_part_code.strip(),
                    row_loc.strip(),
                    rack.strip(),
                    shelf.strip(),
                    bin_val.strip() if bin_val.strip() else None,
                )
                st.success("Saved successfully ✅")
                st.toast("Saved", icon="✅")

    st.divider()

    with st.expander("View current saved record"):
        current = fetch_location(conn, selected_part_code)
        if current:
            st.json(
                {
                    "part_code": current[0],
                    "row": current[1],
                    "rack": current[2],
                    "shelf": current[3],
                    "bin": current[4],
                    "updated_at": current[5],
                }
            )

st.divider()
if st.button("Download all saved mappings as CSV"):
    df = pd.read_sql_query(
        """
        SELECT pc.part_code, l.row_loc, l.rack, l.shelf, l.bin, l.updated_at
        FROM part_codes pc
        LEFT JOIN locations l ON l.part_code = pc.part_code
        ORDER BY l.updated_at DESC
        """,
        conn,
    )
    st.download_button(
        "Click to download",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="inventory_locations.csv",
        mime="text/csv",
    )
