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
    if not val:
        return None
    try:
        return float(str(val).strip().replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        return None


def safe_int(val):
    if not val:
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None


def get(row, key):
    """Get value from row, trying both [bracketed] and plain key."""
    val = row.get(f"[{key}]", "") or row.get(key, "")
    return str(val).strip() if val else ""


def match_tdu(raw):
    raw_upper = raw.upper()
    for key, code in TDU_MAP.items():
        if key in raw_upper:
            return code
    return None


def parse_plans(csv_text):
    reader = csv.DictReader(io.StringIO(csv_text))
    print(f"  CSV Headers: {reader.fieldnames}")

    plans = []
    skipped = 0
    errors = {}

    for row in reader:
        try:
            # TDU
            tdu_raw = get(row, "TduCompanyName")
            tdu = match_tdu(tdu_raw) if tdu_raw else None
            if not tdu:
                errors["no_tdu"] = errors.get("no_tdu", 0) + 1
                skipped += 1
                continue

            # Provider
            provider = get(row, "RepCompany")
            if not provider:
                errors["no_provider"] = errors.get("no_provider", 0) + 1
                skipped += 1
                continue

            # Plan name
            plan_name = get(row, "Product")
            if not plan_name:
                errors["no_plan_name"] = errors.get("no_plan_name", 0) + 1
                skipped += 1
                continue

            # Rate at 1000 kWh
            rate_1000 = safe_float(get(row, "kwh1000"))
            if not rate_1000 or rate_1000 <= 0 or rate_1000 > 100:
                errors["no_rate"] = errors.get("no_rate", 0) + 1
                skipped += 1
                continue

            # Other rates
            rate_500 = safe_float(get(row, "kwh500"))
            rate_2000 = safe_float(get(row, "kwh2000"))

            # Term
            term_str = get(row, "TermValue")
            term = safe_int(re.sub(r'[^\d]', '', term_str)) if term_str else 12
            if not term or term <= 0:
                term = 12

            # Renewable
            renew_str = get(row, "Renewable")
            renewable = safe_int(re.sub(r'[^\d]', '', renew_str)) if renew_str else 0
            if not renewable:
                renewable = 0
            if renewable > 100:
                renewable = 100

            # Cancel fee
            cancel_str = get(row, "CancelFee")
            cancel_fee = safe_float(re.sub(r'[^\d.]', '', cancel_str)) if cancel_str else 0
            if not cancel_fee:
                cancel_fee = 0

            # Plan type
            plan_type = "fixed"
            fixed_val = get(row, "Fixed").lower()
            rate_type = get(row, "RateType").lower()
            if "variable" in rate_type or fixed_val == "false":
                plan_type = "variable"
            elif "indexed" in rate_type:
                plan_type = "indexed"

            # Prepaid
            prepaid_raw = get(row, "PrePaid").lower()
            prepaid = prepaid_raw in ("true", "yes", "1")

            # Time of use
            tou_raw = get(row, "TimeOfUse").lower()
            tou = tou_raw in ("true", "yes", "1")

            # Fact sheet
            fact_sheet = get(row, "FactsURL")

            # Enroll URL from CSV
            enroll_url = get(row, "EnrollURL") or get(row, "Website")

            # Signup URL - prefer our mapped URLs, fall back to CSV
            signup_url = ""
            for pname, url in SIGNUP_URLS.items():
                if pname.lower() in provider.lower():
                    signup_url = url
                    break
            if not signup_url:
                signup_url = enroll_url

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

    print("=== WattWise Rate Scraper ===")
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
