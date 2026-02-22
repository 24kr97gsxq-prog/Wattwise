"""
WattWise Rate Scraper
Fetches electricity plans from PowerToChoose.org and writes to Supabase.
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

POWERTOCHOOSE_CSV_URL = "http://www.powertochoose.org/en-us/Plan/ExportToCsv"

TDU_MAP = {
    "ONCOR": "ONCOR",
    "CENTERPOINT": "CENTPT",
    "TEXAS-NEW MEXICO": "TNMP",
    "AEP TEXAS CENTRAL": "AEP_TCC",
    "AEP TEXAS NORTH": "AEP_TNC",
    "AEP TEXAS": "AEP_TCC",
}

SIGNUP_URLS = {
    "TXU Energy": "https://www.txu.com/enrollment",
    "Reliant Energy": "https://www.reliant.com/en/public/residential/electricity-plans.jsp",
    "Gexa Energy": "https://www.gexaenergy.com/electricity-plans",
    "Green Mountain Energy": "https://www.greenmountainenergy.com/plans",
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
    print(f"Fetching plans from {POWERTOCHOOSE_CSV_URL}...")
    req = Request(POWERTOCHOOSE_CSV_URL, headers={
        "User-Agent": "WattWise Rate Updater/1.0",
        "Accept": "text/csv,*/*"
    })
    try:
        resp = urlopen(req, timeout=60)
        raw = resp.read().decode("utf-8-sig")
        print(f"  Downloaded {len(raw):,} bytes")
        return raw
    except (URLError, HTTPError) as e:
        print(f"  ERROR fetching CSV: {e}")
        return None


def safe_float(val):
    try:
        return float(str(val).strip().replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        return None


def safe_int(val):
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None


def find_col(row, candidates):
    """Find a column value by trying multiple possible header names."""
    for c in candidates:
        for key in row:
            if key and c.lower() in key.lower().replace(" ", "_").replace("-", "_"):
                val = row[key]
                if val and str(val).strip():
                    return str(val).strip()
    return ""


def match_tdu(raw):
    """Match a TDU string to our codes."""
    raw_upper = raw.upper()
    for key, code in TDU_MAP.items():
        if key in raw_upper:
            return code
    return None


def parse_plans(csv_text):
    reader = csv.DictReader(io.StringIO(csv_text))

    # Print headers for debugging
    print(f"  CSV Headers: {reader.fieldnames}")

    plans = []
    skipped = 0
    errors = {}

    for row in reader:
        try:
            # Find TDU
            tdu_raw = find_col(row, ["tdu", "tdsp", "TDU_Company", "tdsp_company"])
            tdu = match_tdu(tdu_raw) if tdu_raw else None
            if not tdu:
                errors["no_tdu"] = errors.get("no_tdu", 0) + 1
                skipped += 1
                continue

            # Provider
            provider = find_col(row, ["rep_company", "Rep_Company", "company", "provider", "rep_name"])
            if not provider:
                errors["no_provider"] = errors.get("no_provider", 0) + 1
                skipped += 1
                continue

            # Plan name
            plan_name = find_col(row, ["product_name", "Product_Name", "plan_name", "plan"])
            if not plan_name:
                errors["no_plan_name"] = errors.get("no_plan_name", 0) + 1
                skipped += 1
                continue

            # Rate at 1000 kWh
            rate_str = find_col(row, ["kwh1000", "1000", "price_kwh1000", "Price_per_kWh_1000", "rate_1000"])
            rate_1000 = safe_float(rate_str)
            if not rate_1000 or rate_1000 <= 0 or rate_1000 > 100:
                errors["no_rate"] = errors.get("no_rate", 0) + 1
                skipped += 1
                continue

            # Rate at 500 and 2000
            rate_500 = safe_float(find_col(row, ["kwh500", "500", "price_kwh500", "rate_500"]))
            rate_2000 = safe_float(find_col(row, ["kwh2000", "2000", "price_kwh2000", "rate_2000"]))

            # Term
            term_str = find_col(row, ["term", "contract", "term_value", "Contract_Length"])
            term = safe_int(re.sub(r'[^\d]', '', term_str)) if term_str else 12
            if not term or term <= 0:
                term = 12

            # Renewable
            renew_str = find_col(row, ["renewable", "percent_renew", "Renewable_Energy"])
            renewable = safe_int(re.sub(r'[^\d]', '', renew_str)) if renew_str else 0
            if not renewable:
                renewable = 0
            if renewable > 100:
                renewable = 100

            # Cancel fee
            cancel_str = find_col(row, ["cancel", "termination", "etf", "Cancellation"])
            cancel_fee = safe_float(re.sub(r'[^\d.]', '', cancel_str)) if cancel_str else 0
            if not cancel_fee:
                cancel_fee = 0

            # Plan type
            plan_type = "fixed"
            pt_raw = find_col(row, ["plan_type", "product_type", "rate_type", "Plan_Type"]).lower()
            if "variable" in pt_raw:
                plan_type = "variable"
            elif "indexed" in pt_raw:
                plan_type = "indexed"

            # Prepaid
            prepaid_raw = find_col(row, ["prepaid", "Prepaid"]).lower()
            prepaid = prepaid_raw in ("true", "yes", "1")

            # Time of use
            tou_raw = find_col(row, ["time_of_use", "tou", "Time_of_Use"]).lower()
            tou = tou_raw in ("true", "yes", "1")

            # Fact sheet
            fact_sheet = find_col(row, ["fact_sheet", "Fact_Sheet", "efl"])

            # Signup URL
            signup_url = ""
            for pname, url in SIGNUP_URLS.items():
                if pname.lower() in provider.lower():
                    signup_url = url
                    break

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
            errors["exception"] = errors.get("exception", 0) + 1
            skipped += 1

    print(f"  Parsed {len(plans)} plans, skipped {skipped} rows")
    if errors:
        print(f"  Skip reasons: {errors}")

    # Print first parsed plan for debugging
    if plans:
        print(f"  Sample plan: {plans[0]}")

    return plans


def upload_plans(plans, url, key):
    print(f"Uploading {len(plans)} plans to Supabase...")

    # Delete existing plans
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
        post_url = f"{url}/rest/v1/plans"
        post_headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }
        body = json.dumps(batch).encode("utf-8")
        req = Request(post_url, data=body, headers=post_headers, method="POST")
        try:
            resp = urlopen(req, timeout=30)
            inserted += len(batch)
            print(f"  Inserted batch {i // batch_size + 1}: {len(batch)} plans")
        except HTTPError as e:
            error_body = e.read().decode() if e.fp else str(e)
            print(f"  ERROR inserting batch {i // batch_size + 1}: {e.code} {error_body}")

    print(f"  Total inserted: {inserted}/{len(plans)}")
    return inserted


def log_scrape(url, key, plans_found, plans_inserted, status="success", error=None):
    post_url = f"{url}/rest/v1/scrape_log"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    body = json.dumps([{
        "plans_found": plans_found,
        "plans_inserted": plans_inserted,
        "status": status,
        "error_message": error,
    }]).encode("utf-8")
    req = Request(post_url, data=body, headers=headers, method="POST")
    try:
        urlopen(req, timeout=30)
    except Exception as e:
        print(f"  Warning: could not log scrape: {e}")


def main():
    sb_url = os.environ.get("SUPABASE_URL")
    sb_key = os.environ.get("SUPABASE_KEY")

    if not sb_url or not sb_key:
        print("ERROR: Set SUPABASE_URL and SUPABASE_KEY environment variables")
        sys.exit(1)

    print(f"=== WattWise Rate Scraper ===")
    print(f"Time: {datetime.now(timezone.utc).isoformat()}")

    # Fetch CSV
    csv_text = fetch_csv()
    if not csv_text:
        log_scrape(sb_url, sb_key, 0, 0, "error", "Failed to fetch CSV")
        sys.exit(1)

    # Parse
    plans = parse_plans(csv_text)
    if not plans:
        log_scrape(sb_url, sb_key, 0, 0, "error", "No plans parsed")
        sys.exit(1)

    # Filter to fixed-rate non-prepaid only
    filtered = [p for p in plans if p["plan_type"] == "fixed" and not p["prepaid"] and not p["tou"]]
    print(f"  Filtered to {len(filtered)} fixed-rate non-prepaid plans")

    if not filtered:
        print("  WARNING: No fixed-rate plans found, uploading all plans instead")
        filtered = plans

    # Upload
    inserted = upload_plans(filtered, sb_url, sb_key)

    # Log
    log_scrape(sb_url, sb_key, len(plans), inserted, "success")
    print(f"\nDone! {inserted} plans updated in Supabase.")


if __name__ == "__main__":
    main()
