"""
WattWise Rate Scraper
Fetches electricity plans from PowerToChoose.org and writes to Supabase.

Usage:
  python scrape_rates.py

Environment variables required:
  SUPABASE_URL      - Your Supabase project URL
  SUPABASE_KEY      - Your Supabase service_role key (NOT the anon key)

Can be run locally or via GitHub Actions on a weekly schedule.
"""

import os
import sys
import csv
import io
import json
import re
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urljoin

# ============================================================
# CONFIG
# ============================================================

POWERTOCHOOSE_CSV_URL = "http://www.powertochoose.org/en-us/Plan/ExportToCsv"

# TDU name mapping from PowerToChoose to our codes
TDU_MAP = {
    "ONCOR ELECTRIC DELIVERY": "ONCOR",
    "ONCOR ELECTRIC DELIVERY COMPANY": "ONCOR",
    "ONCOR": "ONCOR",
    "CENTERPOINT ENERGY": "CENTPT",
    "CENTERPOINT": "CENTPT",
    "CENTERPOINT ENERGY HOUSTON ELECTRIC": "CENTPT",
    "TEXAS-NEW MEXICO POWER": "TNMP",
    "TNMP": "TNMP",
    "AEP TEXAS CENTRAL": "AEP_TCC",
    "AEP TEXAS CENTRAL COMPANY": "AEP_TCC",
    "AEP TEXAS NORTH": "AEP_TNC",
    "AEP TEXAS NORTH COMPANY": "AEP_TNC",
    "AEP TEXAS": "AEP_TCC",  # Default AEP to Central
}

# Provider signup URLs
SIGNUP_URLS = {
    "TXU Energy": "https://www.txu.com/enrollment",
    "Reliant Energy": "https://www.reliant.com/en/public/residential/electricity-plans.jsp",
    "Gexa Energy": "https://www.gexaenergy.com/electricity-plans",
    "Green Mountain Energy": "https://www.greenmountainenergy.com/plans",
    "Green Mountain": "https://www.greenmountainenergy.com/plans",
    "Constellation": "https://www.constellation.com/solutions/for-your-home/electricity-plans.html",
    "4Change Energy": "https://www.4changeenergy.com/plans",
    "Frontier Utilities": "https://www.frontierutilities.com/plans",
    "Chariot Energy": "https://chariotenergy.com/plans",
    "Pulse Power": "https://pulsepower.com/plans",
    "Rhythm Energy": "https://www.gotrhythm.com/electricity-plans",
    "Express Energy": "https://www.myexpressenergy.com",
    "Discount Power": "https://www.discountpowertx.com",
    "Veteran Energy": "https://www.veteranenergy.us/plans",
    "TriEagle Energy": "https://www.trieagleenergy.com/plans",
    "Cirro Energy": "https://www.cirroenergy.com/plans",
}


def fetch_csv():
    """Download the PowerToChoose CSV export."""
    print(f"Fetching plans from {POWERTOCHOOSE_CSV_URL}...")
    req = Request(POWERTOCHOOSE_CSV_URL, headers={
        "User-Agent": "WattWise Rate Updater/1.0",
        "Accept": "text/csv,*/*"
    })
    try:
        resp = urlopen(req, timeout=30)
        raw = resp.read().decode("utf-8-sig")  # Handle BOM
        print(f"  Downloaded {len(raw):,} bytes")
        return raw
    except (URLError, HTTPError) as e:
        print(f"  ERROR fetching CSV: {e}")
        return None


def parse_plans(csv_text):
    """Parse the CSV into a list of plan dicts."""
    reader = csv.DictReader(io.StringIO(csv_text))
    plans = []
    skipped = 0

    for row in reader:
        try:
            # Normalize TDU name
            tdu_raw = (row.get("TDU_Company_Name") or row.get("tdsp_company_name") or "").strip().upper()
            tdu = None
            for key, code in TDU_MAP.items():
                if key in tdu_raw:
                    tdu = code
                    break
            if not tdu:
                skipped += 1
                continue

            # Extract provider and plan name
            provider = (row.get("Rep_Company") or row.get("rep_company") or "").strip()
            plan_name = (row.get("Product_Name") or row.get("product_name") or "").strip()
            if not provider or not plan_name:
                skipped += 1
                continue

            # Extract rate at 1000 kWh (primary), 500 kWh, 2000 kWh
            rate_1000 = safe_float(row.get("Price_per_kWh_1000") or row.get("price_kwh1000") or row.get("rate_1000"))
            rate_500 = safe_float(row.get("Price_per_kWh_500") or row.get("price_kwh500") or row.get("rate_500"))
            rate_2000 = safe_float(row.get("Price_per_kWh_2000") or row.get("price_kwh2000") or row.get("rate_2000"))

            if not rate_1000 or rate_1000 <= 0:
                skipped += 1
                continue

            # Term length
            term_raw = row.get("Contract_Length") or row.get("term_value") or "12"
            term = safe_int(re.sub(r'[^\d]', '', str(term_raw))) or 12

            # Renewable percentage
            renew_raw = row.get("Renewable_Energy_Description") or row.get("percent_renewable") or "0"
            renewable = safe_int(re.sub(r'[^\d]', '', str(renew_raw))) or 0
            if renewable > 100:
                renewable = 100

            # Cancellation fee
            cancel_raw = row.get("Cancellation_Fee") or row.get("cancel_fee") or "0"
            cancel_fee = safe_float(re.sub(r'[^\d.]', '', str(cancel_raw))) or 0

            # Plan type
            plan_type = "fixed"
            pt_raw = (row.get("Plan_Type") or row.get("product_type") or "").lower()
            if "variable" in pt_raw:
                plan_type = "variable"
            elif "indexed" in pt_raw:
                plan_type = "indexed"

            # Prepaid / Time of Use
            prepaid = "prepaid" in (row.get("Prepaid") or row.get("prepaid_plan") or "").lower()
            tou = "true" in (row.get("Time_of_Use") or row.get("tou") or "").lower()

            # Fact sheet URL
            fact_sheet = row.get("Fact_Sheet") or row.get("fact_sheet") or ""

            # Signup URL
            signup_url = SIGNUP_URLS.get(provider, "")

            plans.append({
                "tdu": tdu,
                "provider": provider,
                "plan_name": plan_name,
                "rate_kwh": rate_1000,
                "rate_500": rate_500,
                "rate_2000": rate_2000,
                "term_months": term,
                "renewable_pct": renewable,
                "cancel_fee": cancel_fee,
                "plan_type": plan_type,
                "prepaid": prepaid,
                "tou": tou,
                "fact_sheet_url": fact_sheet,
                "signup_url": signup_url,
                "source": "powertochoose",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })

        except Exception as e:
            print(f"  Warning: skipping row due to error: {e}")
            skipped += 1

    print(f"  Parsed {len(plans)} plans, skipped {skipped} rows")
    return plans


def safe_float(val):
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def safe_int(val):
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None


def supabase_request(url, key, path, method="GET", data=None):
    """Make a request to the Supabase REST API."""
    full_url = f"{url}/rest/v1/{path}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",  # Upsert behavior
    }
    if method == "POST":
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"

    body = json.dumps(data).encode("utf-8") if data else None
    req = Request(full_url, data=body, headers=headers, method=method)

    try:
        resp = urlopen(req, timeout=30)
        if resp.status in (200, 201, 204):
            try:
                return json.loads(resp.read().decode())
            except Exception:
                return True
        return True
    except HTTPError as e:
        error_body = e.read().decode() if e.fp else str(e)
        print(f"  Supabase error ({e.code}): {error_body}")
        return None


def upload_plans(plans, url, key):
    """Upload plans to Supabase via REST API with upsert."""
    print(f"Uploading {len(plans)} plans to Supabase...")

    # Delete existing plans and re-insert (cleanest approach)
    # First delete all existing
    delete_url = f"{url}/rest/v1/plans?source=eq.powertochoose"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    req = Request(delete_url, headers=headers, method="DELETE")
    try:
        urlopen(req, timeout=30)
        print("  Cleared existing plans")
    except HTTPError as e:
        print(f"  Warning clearing plans: {e.code}")

    # Insert in batches of 50
    batch_size = 50
    inserted = 0
    for i in range(0, len(plans), batch_size):
        batch = plans[i:i + batch_size]
        result = supabase_request(url, key, "plans", method="POST", data=batch)
        if result is not None:
            inserted += len(batch)
            print(f"  Inserted batch {i // batch_size + 1}: {len(batch)} plans")
        else:
            print(f"  ERROR inserting batch {i // batch_size + 1}")

    print(f"  Total inserted: {inserted}/{len(plans)}")
    return inserted


def log_scrape(url, key, plans_found, plans_inserted, status="success", error=None):
    """Log the scrape result."""
    supabase_request(url, key, "scrape_log", method="POST", data=[{
        "plans_found": plans_found,
        "plans_inserted": plans_inserted,
        "status": status,
        "error_message": error,
    }])


def main():
    # Get Supabase credentials
    sb_url = os.environ.get("SUPABASE_URL")
    sb_key = os.environ.get("SUPABASE_KEY")  # service_role key

    if not sb_url or not sb_key:
        print("ERROR: Set SUPABASE_URL and SUPABASE_KEY environment variables")
        print("  SUPABASE_URL = your project URL (e.g. https://xxxxx.supabase.co)")
        print("  SUPABASE_KEY = your service_role key (from Settings → API)")
        sys.exit(1)

    print(f"=== WattWise Rate Scraper ===")
    print(f"Time: {datetime.now(timezone.utc).isoformat()}")

    # Step 1: Fetch CSV from PowerToChoose
    csv_text = fetch_csv()
    if not csv_text:
        log_scrape(sb_url, sb_key, 0, 0, "error", "Failed to fetch CSV")
        sys.exit(1)

    # Step 2: Parse plans
    plans = parse_plans(csv_text)
    if not plans:
        log_scrape(sb_url, sb_key, 0, 0, "error", "No plans parsed from CSV")
        sys.exit(1)

    # Step 3: Filter to fixed-rate, non-prepaid plans only (WattWise focus)
    filtered = [p for p in plans if p["plan_type"] == "fixed" and not p["prepaid"] and not p["tou"]]
    print(f"  Filtered to {len(filtered)} fixed-rate, non-prepaid plans")

    # Step 4: Upload to Supabase
    inserted = upload_plans(filtered, sb_url, sb_key)

    # Step 5: Log
    log_scrape(sb_url, sb_key, len(plans), inserted, "success")
    print(f"\n✅ Done! {inserted} plans updated in Supabase.")


if __name__ == "__main__":
    main()
