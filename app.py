import os
import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

PART_CODES_CSV = Path("part_codes.csv")

def get_conn():
    db_url = st.secrets.get("DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        st.error("DATABASE_URL not set. Add it in Streamlit Secrets.")
        st.stop()
    return psycopg2.connect(db_url)

def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS part_codes (
            part_code TEXT PRIMARY KEY,
            source TEXT NOT NULL DEFAULT 'manual',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            part_code TEXT PRIMARY KEY REFERENCES part_codes(part_code) ON DELETE CASCADE,
            row_loc TEXT NOT NULL,
            rack TEXT NOT NULL,
            shelf TEXT NOT NULL,
            bin TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
    conn.commit()

@st.cache_data(show_spinner=False)
def load_part_codes_csv(csv_path: Path) -> pd.Series:
    df = pd.read_csv(csv_path, dtype={"part_code": "string"})
    if "part_code" not in df.columns:
        raise ValueError("CSV must have a column named 'part_code'")
    codes = df["part_code"].dropna().astype(str).str.strip()
    codes = codes[codes != ""].drop_duplicates()
    return codes

def seed_part_codes(conn, codes: pd.Series):
    rows = [(c, "csv") for c in codes.tolist()]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO part_codes (part_code, source)
            VALUES %s
            ON CONFLICT (part_code) DO NOTHING
            """,
            rows,
            page_size=5000,
        )
    conn.commit()

def part_code_exists(conn, part_code: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM part_codes WHERE part_code=%s LIMIT 1", (part_code,))
        return cur.fetchone() is not None

def add_part_code(conn, part_code: str, source="manual"):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO part_codes (part_code, source) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (part_code, source),
        )
    conn.commit()

def fetch_location(conn, part_code: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT part_code, row_loc, rack, shelf, bin, updated_at FROM locations WHERE part_code=%s",
            (part_code,),
        )
        return cur.fetchone()

def upsert_location(conn, part_code, row_loc, rack, shelf, bin_val):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO locations (part_code, row_loc, rack, shelf, bin, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (part_code) DO UPDATE SET
              row_loc=EXCLUDED.row_loc,
              rack=EXCLUDED.rack,
              shelf=EXCLUDED.shelf,
              bin=EXCLUDED.bin,
              updated_at=NOW()
            """,
            (part_code, row_loc, rack, shelf, bin_val),
        )
    conn.commit()

def search_suggestions(conn, prefix: str, limit: int = 50):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT part_code
            FROM part_codes
            WHERE part_code ILIKE %s
            ORDER BY part_code
            LIMIT %s
            """,
            (prefix + "%", limit),
        )
        return [r[0] for r in cur.fetchall()]

def count_part_codes(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM part_codes")
        return cur.fetchone()[0]

# ---------------- UI ----------------
st.set_page_config(page_title="Inventory Location Mapper", layout="centered")
st.title("Inventory Location Mapper")

conn = get_conn()
init_db(conn)

if PART_CODES_CSV.exists():
    csv_codes = load_part_codes_csv(PART_CODES_CSV)
    seed_part_codes(conn, csv_codes)
else:
    st.warning("part_codes.csv not found. You can still add codes manually.")

st.caption(f"Total part codes in system: {count_part_codes(conn):,}")

query = st.text_input("Scan or type part code (search)", placeholder="e.g., ABC123")

selected_part_code = None
if query:
    q = query.strip()
    if q and part_code_exists(conn, q):
        selected_part_code = q
        st.success("Exact match found.")
    else:
        suggestions = search_suggestions(conn, q, limit=50) if q else []
        if suggestions:
            selected_part_code = st.selectbox("Select from suggestions", options=suggestions)
        else:
            st.warning("No matching part codes found.")
            st.subheader("Add new part code")
            with st.form("add_part_code_form"):
                new_code = st.text_input("New part code", value=q)
                confirm_add = st.form_submit_button("Add part code")
                if confirm_add:
                    new_code = new_code.strip()
                    if not new_code:
                        st.error("Part code cannot be empty.")
                    else:
                        add_part_code(conn, new_code, source="manual")
                        st.success("Part code added ✅ Now assign its location below.")
                        selected_part_code = new_code

st.divider()

if selected_part_code:
    existing = fetch_location(conn, selected_part_code)
    if existing:
        _, row0, rack0, shelf0, bin0, updated_at = existing
        st.info(f"Existing location found (last updated: {updated_at}).")
    else:
        row0 = rack0 = shelf0 = ""
        bin0 = ""

    with st.form("location_form"):
        st.subheader(f"Set location for: {selected_part_code}")
        row_loc = st.text_input("Row *", value=row0)
        rack = st.text_input("Rack *", value=rack0)
        shelf = st.text_input("Shelf *", value=shelf0)
        bin_val = st.text_input("Bin (optional)", value=bin0)
        submitted = st.form_submit_button("Save")

        if submitted:
            if not row_loc.strip() or not rack.strip() or not shelf.strip():
                st.error("Row, Rack, and Shelf are required.")
            else:
                add_part_code(conn, selected_part_code, source="manual")
                upsert_location(conn, selected_part_code, row_loc.strip(), rack.strip(), shelf.strip(),
                               bin_val.strip() if bin_val.strip() else None)
                st.success("Saved ✅")
