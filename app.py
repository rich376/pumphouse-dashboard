
import os
import io
import re
import duckdb
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from urllib.request import urlopen, Request

# ----------------------
# THEME & PAGE CONFIG
# ----------------------
st.set_page_config(
    page_title="Pump House Sales Dashboard",
    page_icon="🍺",
    layout="wide"
)

PRIMARY_COLOR = st.get_option("theme.primaryColor") or "#D94F2A"  # fallback

# ----------------------
# UTILITIES
# ----------------------
@st.cache_data(show_spinner=False)
def fetch_xls_from_url(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req) as resp:
        return resp.read()

def convert_container_ml(val):
    try:
        # Values are like .3750 meaning 375 ml
        num = float(val)
        ml = int(round(num * 1000))
        return f"{ml} ml"
    except Exception:
        return str(val)

def parse_supplier_report(file_bytes: bytes) -> pd.DataFrame:
    # Read metadata rows (0..3) and header row at 4
    xls = pd.ExcelFile(io.BytesIO(file_bytes))
    df_head = pd.read_excel(xls, sheet_name="SUPPLIER REPORT", header=None, nrows=5)
    meta = {
        "fiscal_year": str(df_head.iat[0,1]).strip(),
        "fiscal_week": str(df_head.iat[1,1]).strip(),
        "inventory_pull_date": pd.to_datetime(df_head.iat[2,1], errors="coerce"),
        "sold_date_range": str(df_head.iat[3,1]).strip(),
    }
    df = pd.read_excel(xls, sheet_name="SUPPLIER REPORT", header=4)
    # Standardize column names
    df.columns = [str(c).strip() for c in df.columns]

    # Keep relevant columns
    base_cols = ["Item UPC","Item Description","Vendor Name","Class","Container Size","Retail Price","Total","Agent","Grocery","Licensee","Other","Public"]
    base_cols_present = [c for c in base_cols if c in df.columns]
    core = df[base_cols_present].copy()

    # Convert container to ml display
    if "Container Size" in core.columns:
        core["Container Size"] = core["Container Size"].apply(convert_container_ml).rename("Container Size")

    # Identify store columns: pattern like '002 Qty Sold' and '002 Qty On Hand'
    sold_cols = [c for c in df.columns if re.match(r"^\d{3}\s+Qty Sold$", c)]
    onhand_cols = [c for c in df.columns if re.match(r"^\d{3}\s+Qty On Hand$", c)]
    # Ensure aligned store codes
    store_codes = sorted({c.split()[0] for c in sold_cols})
    # Melt to long format
    long_frames = []
    for sc in store_codes:
        sold_col = f"{sc} Qty Sold"
        if sold_col not in df.columns:
            continue
        tmp = core.copy()
        tmp["Store Code"] = sc
        tmp["Qty Sold"] = df[sold_col].fillna(0).astype(float)
        long_frames.append(tmp)
    if not long_frames:
        return pd.DataFrame()
    sales_long = pd.concat(long_frames, ignore_index=True)

    # Rename columns per business terms
    sales_long = sales_long.rename(columns={
        "Vendor Name": "Brand",
        "Item Description": "Product",
        "Container Size": "Container",
        "Retail Price": "RetailPrice",
    })
    # Dollars
    if "RetailPrice" in sales_long.columns:
        sales_long["Dollars"] = (sales_long["Qty Sold"].astype(float) * sales_long["RetailPrice"].astype(float)).round(2)
    else:
        sales_long["Dollars"] = np.nan

    # Attach meta
    sales_long["FiscalYear"] = meta["fiscal_year"]
    sales_long["FiscalWeek"] = meta["fiscal_week"]
    sales_long["InventoryPullDate"] = meta["inventory_pull_date"]
    sales_long["SoldDateRange"] = meta["sold_date_range"]
    return sales_long

# ----------------------
# DATABASE
# ----------------------
DB_PATH = os.environ.get("DB_PATH", "pumphouse.duckdb")
con = duckdb.connect(DB_PATH)
con.execute("""
CREATE TABLE IF NOT EXISTS sales (
    FiscalYear TEXT,
    FiscalWeek TEXT,
    InventoryPullDate TIMESTAMP,
    SoldDateRange TEXT,
    Brand TEXT,
    Product TEXT,
    Class TEXT,
    Container TEXT,
    RetailPrice DOUBLE,
    Total DOUBLE,
    Agent DOUBLE,
    Grocery DOUBLE,
    Licensee DOUBLE,
    Other DOUBLE,
    Public DOUBLE,
    StoreCode TEXT,
    QtySold DOUBLE,
    Dollars DOUBLE
);
""")
con.execute("""
CREATE TABLE IF NOT EXISTS stores (
    StoreCode TEXT PRIMARY KEY,
    StoreName TEXT,
    Address TEXT,
    City TEXT,
    Province TEXT,
    Lat DOUBLE,
    Lon DOUBLE
);
""")

def upsert_sales(df: pd.DataFrame):
    if df.empty:
        return 0
    # Deduplicate on key: FiscalYear, FiscalWeek, Product, StoreCode, Dollars/QtySold taken from latest
    # We'll delete then insert for that slice
    keys = df[["FiscalYear","FiscalWeek","Product","Store Code"]].drop_duplicates()
    for _, row in keys.iterrows():
        con.execute("""DELETE FROM sales WHERE FiscalYear=? AND FiscalWeek=? AND Product=? AND StoreCode=?""",
                    [row["FiscalYear"], row["FiscalWeek"], row["Product"], row["Store Code"]])
    con.execute("INSERT INTO sales SELECT * FROM df").df()
    return len(df)

# ----------------------
# SIDEBAR FILTERS
# ----------------------
def load_data():
    df = con.execute("""
        SELECT s.*, st.StoreName, st.City, st.Province, st.Lat, st.Lon
        FROM sales s
        LEFT JOIN stores st ON st.StoreCode = s.StoreCode
    """).df()
    return df

def compute_top_comp_brands(df: pd.DataFrame, pump_house_name="Pump House"):
    # total by brand
    t = df.groupby("Brand", as_index=False)["Dollars"].sum().sort_values("Dollars", ascending=False)
    # Pump House first, then 5 closest competitors
    brands = t["Brand"].tolist()
    pump = [b for b in brands if pump_house_name.lower() in str(b).lower()]
    pump = pump[:1] if pump else brands[:1]
    others = [b for b in brands if b not in pump][:5]
    ordered = pump + others
    return ordered

# ----------------------
# UI
# ----------------------
st.title("Pump House Sales Dashboard")
st.caption("Dark-mode, Pump House first. Upload monthly XLS or paste a direct link in Admin to merge data.")

with st.sidebar:
    st.subheader("Filters")
    df_all = load_data()
    if df_all.empty:
        st.info("No data yet. Go to Admin to upload your first XLS.")
    fiscal_weeks = sorted(df_all["FiscalWeek"].dropna().unique().tolist())
    fiscal_week_sel = st.multiselect("Fiscal Week", fiscal_weeks, default=fiscal_weeks[-4:] if fiscal_weeks else [])
    brands = sorted(df_all["Brand"].dropna().unique().tolist())
    brand_sel = st.multiselect("Brand", brands, default=[])
    stores = sorted(df_all["StoreCode"].dropna().unique().tolist())
    store_sel = st.multiselect("Store Code", stores, default=[])
    cities = sorted(df_all["City"].dropna().unique().tolist())
    city_sel = st.multiselect("City", cities, default=[])
    skus = sorted(df_all["Product"].dropna().unique().tolist())
    sku_sel = st.multiselect("Product", [], default=[])

    # Apply filters
    df = df_all.copy()
    if fiscal_week_sel:
        df = df[df["FiscalWeek"].isin(fiscal_week_sel)]
    if brand_sel:
        df = df[df["Brand"].isin(brand_sel)]
    if store_sel:
        df = df[df["StoreCode"].isin(store_sel)]
    if city_sel:
        df = df[df["City"].isin(city_sel)]
    if sku_sel:
        df = df[df["Product"].isin(sku_sel)]

# KPI Section
if not df.empty:
    total_dollars = df["Dollars"].sum()
    total_units = df["QtySold"].sum()
    stores_active = df["StoreCode"].nunique()
    top_store = df.groupby("StoreCode")["Dollars"].sum().sort_values(ascending=False).head(1)
    top_sku = df.groupby("Product")["Dollars"].sum().sort_values(ascending=False).head(1)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Dollars", f"${total_dollars:,.0f}")
    c2.metric("Total Units", f"{int(total_units):,}")
    c3.metric("Active Stores", f"{stores_active}")
    c4.metric("Top Store", top_store.index[0] if not top_store.empty else "—")
    c5.metric("Top Product", top_sku.index[0] if not top_sku.empty else "—")

# BRAND SHARE
if not df.empty:
    ordered_brands = compute_top_comp_brands(df, pump_house_name="Pump House")
    df_comp = df[df["Brand"].isin(ordered_brands)]
    brand_totals = df_comp.groupby(["FiscalWeek","Brand"], as_index=False)["Dollars"].sum()
    fig = px.area(brand_totals, x="FiscalWeek", y="Dollars", color="Brand", category_orders={"Brand": ordered_brands})
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")

# MAP + TOP STORES
if not df.empty:
    cc1, cc2 = st.columns([2,1])
    with cc1:
        map_df = df.groupby(["StoreCode","StoreName","City","Lat","Lon"], as_index=False)["Dollars"].sum()
        map_df = map_df.dropna(subset=["Lat","Lon"])
        if not map_df.empty:
            figm = px.scatter_mapbox(map_df, lat="Lat", lon="Lon", size="Dollars", hover_name="StoreName",
                                     hover_data={"City":True,"Dollars":":,.0f"}, zoom=5, height=500)
            figm.update_layout(mapbox_style="carto-darkmatter", margin=dict(l=0,r=0,t=0,b=0))
            st.plotly_chart(figm, use_container_width=True, theme="streamlit")
        else:
            st.info("Add lat/lon to the store directory to enable the map.")
    with cc2:
        top10 = df.groupby(["StoreCode","StoreName"], as_index=False)["Dollars"].sum().sort_values("Dollars", ascending=False).head(10)
        st.dataframe(top10, use_container_width=True)

# LEADERBOARDS
if not df.empty:
    st.subheader("Leaderboards")
    cc3, cc4 = st.columns(2)
    with cc3:
        lb_stores = df.groupby(["StoreCode","StoreName"], as_index=False)["Dollars"].sum().sort_values("Dollars", ascending=False).head(10)
        st.dataframe(lb_stores, use_container_width=True)
    with cc4:
        lb_brands = df.groupby("Brand", as_index=False)["Dollars"].sum().sort_values("Dollars", ascending=False).head(10)
        st.dataframe(lb_brands, use_container_width=True)

# DATA TABLE + CSV EXPORT
st.subheader("Data")
if not df.empty:
    st.download_button("Download current view as CSV", df.to_csv(index=False).encode("utf-8"), file_name="pumphouse_filtered.csv", mime="text/csv")
    st.dataframe(df, use_container_width=True, height=500)

# ----------------------
# ADMIN
# ----------------------
st.markdown("---")
st.header("Admin")
if st.session_state.get("is_admin") or st.text_input("Enter Admin Password", type="password") == os.environ.get("ADMIN_PASSWORD", "changeme"):
    st.session_state["is_admin"] = True
    st.success("Admin mode")
    st.write("Upload monthly XLS or paste a direct XLS URL. Files with the same FiscalWeek/Product/StoreCode will replace prior rows.")

    up_col, url_col = st.columns(2)
    with up_col:
        upf = st.file_uploader("Upload SUPPLIER REPORT (XLSX)", type=["xlsx"])
        if upf:
            data = upf.read()
            parsed = parse_supplier_report(data)
            if parsed.empty:
                st.error("Could not parse any sales rows.")
            else:
                st.dataframe(parsed.head(20))
                if st.button("Ingest uploaded file"):
                    count = upsert_sales(parsed)
                    st.success(f"Ingested {count:,} rows.")
    with url_col:
        url = st.text_input("Paste direct XLS URL")
        if st.button("Fetch and Ingest URL") and url:
            try:
                b = fetch_xls_from_url(url)
                parsed = parse_supplier_report(b)
                if parsed.empty:
                    st.error("Parsed zero rows.")
                else:
                    count = upsert_sales(parsed)
                    st.success(f"Ingested {count:,} rows from URL.")
            except Exception as e:
                st.error(f"Fetch failed: {e}")

    st.subheader("Store Directory")
    st.write("Upload a CSV with columns: StoreCode,StoreName,Address,City,Province,Lat,Lon")
    stores_csv = st.file_uploader("Upload Stores CSV", type=["csv"], key="stores")
    if stores_csv:
        sdf = pd.read_csv(stores_csv, dtype={"StoreCode":str})
        sdf["StoreCode"] = sdf["StoreCode"].str.zfill(3)
        con.execute("DELETE FROM stores")
        con.execute("INSERT INTO stores SELECT * FROM sdf")
        st.success(f"Loaded {len(sdf)} stores.")

else:
    st.info("Enter the Admin password to upload data.")
