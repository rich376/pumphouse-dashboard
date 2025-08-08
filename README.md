# Pump House Sales Dashboard

A dark-mode Streamlit dashboard that ingests monthly ANBL XLS reports, merges them into a DuckDB data store, and visualizes Pump House vs the top 5 closest competitors.

## One-click deploy (Streamlit Cloud)
1. Push this folder to a GitHub repo.
2. Go to share.streamlit.io, create a new app, choose your repo and `app.py`.
3. Set environment variables:
   - `ADMIN_PASSWORD`: a strong password for the Admin page
   - Optional `DB_PATH`: leave default
4. Deploy. Copy the app URL.

## Embed in Wix
On your Wix site, add an Embed > Custom Embed > iFrame/HTML and paste:
```html
<div style="width:100%;height:1200px">
  <iframe src="YOUR_APP_URL" width="100%" height="1200" frameborder="0" allow="clipboard-read; clipboard-write"></iframe>
</div>
```
Ensure "Let site visitors interact" is enabled.

## Admin
- Upload an XLSX or paste a direct XLS URL. The app parses:
  - Brand from `Vendor Name` (Pump House brand)
  - Product from `Item Description`
  - Store codes from `### Qty Sold` columns
- Container size is converted to ml (e.g., `.3750` → `375 ml`).
- Dollars = Qty Sold × Retail Price.
- Dedup key = (FiscalYear, FiscalWeek, Product, StoreCode).

## Store Directory
Upload a CSV with columns:
`StoreCode,StoreName,Address,City,Province,Lat,Lon` (StoreCode should be three digits).