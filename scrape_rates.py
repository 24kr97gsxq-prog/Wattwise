#!/usr/bin/env python3
"""
WattWise Plan Scraper — PowerToChoose.org
Fetches all electricity plans, filters out garbage/teaser rates,
and upserts to Supabase.

Filters applied:
  - Rate must be between 3.0 and 50.0 cents/kWh
  - Term must be >= 3 months (rejects 1-2 month intro teasers)
  - Must have a provider name and plan name
  - Deduplicates by provider + plan name + TDU + rate
"""

import os
import json
import csv
import io
import sys
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

PTC_URL = 'http://www.powertochoose.org/en-us/Plan/ExportToCsv'

MIN_RATE = 3.0
MAX_RATE = 50.0
MIN_TERM = 3

TDU_MAP = {
    'oncor': 'ONCOR', 'oncor electric delivery': 'ONCOR',
    'centerpoint': 'CENTPT', 'centerpoint energy': 'CENTPT', 'cnp': 'CENTPT',
    'texas-new mexico power': 'TNMP', 'texas-new mexico': 'TNMP', 'tnmp': 'TNMP',
    'aep texas central': 'AEP_TCC', 'aep central': 'AEP_TCC',
    'aep texas north': 'AEP_TNC', 'aep north': 'AEP_TNC',
    'lubbock power': 'LPL', 'lubbock power & light': 'LPL',
}

def normalize_tdu(raw):
    if not raw: return None
    low = raw.strip().lower()
    for key, val in TDU_MAP.items():
        if key in low: return val
    return raw.strip().upper()[:20]

def fetch_ptc_csv():
    print("Fetching PowerToChoose.org CSV...")
    req = Request(PTC_URL, headers={
        'User-Agent': 'WattWise/3.0 (Texas Electricity Comparison Tool)',
        'Accept': 'text/csv,application/csv,text/plain,*/*'
    })
    try:
        with urlopen(req, timeout=60) as resp:
            raw = resp.read()
            try: return raw.decode('utf-8')
            except UnicodeDecodeError: return raw.decode('latin-1')
    except URLError as e:
        print(f"  Failed: {e}")
        return None

def parse_rate(val):
    if not val: return None
    try:
        cleaned = val.strip().replace('$','').replace('¢','').replace(',','')
        rate = float(cleaned)
        if 0 < rate < 1: rate = rate * 100
        return round(rate, 2)
    except (ValueError, TypeError): return None

def parse_csv(csv_text):
    print("Parsing and filtering plans...")
    reader = csv.DictReader(io.StringIO(csv_text))
    plans = []
    rejected = {'low_rate':0,'high_rate':0,'short_term':0,'no_data':0,'duplicate':0}
    seen = set()

    for row in reader:
        provider = (row.get('RepCompany') or row.get('Company') or row.get('Provider') or row.get('rep_company') or '').strip()
        plan_name = (row.get('PlanName') or row.get('Plan Name') or row.get('plan_name') or row.get('Product') or '').strip()
        rate_raw = (row.get('Price1000') or row.get('Rate1000') or row.get('price_kwh1000') or row.get('Rate') or row.get('rate_kwh') or '')
        tdu_raw = (row.get('TduCompanyName') or row.get('TDU') or row.get('tdu_company_name') or row.get('Utility') or '')
        term_raw = (row.get('TermValue') or row.get('Term') or row.get('contract_term') or row.get('ContractTerm') or '12')
        renew_raw = (row.get('RenewablePercent') or row.get('Renewable') or row.get('renewable_pct') or row.get('PercentRenewable') or '0')
        cancel_raw = (row.get('CancelFee') or row.get('EarlyTermFee') or row.get('cancel_fee') or row.get('CancellationFee') or '0')
        enroll_url = (row.get('EnrollUrl') or row.get('EnrollURL') or row.get('enroll_url') or row.get('SignupURL') or row.get('GoToUrl') or row.get('Website') or '')

        if not provider or not plan_name:
            rejected['no_data'] += 1; continue

        rate = parse_rate(rate_raw)
        if rate is None:
            rejected['no_data'] += 1; continue
        if rate < MIN_RATE:
            rejected['low_rate'] += 1; continue
        if rate > MAX_RATE:
            rejected['high_rate'] += 1; continue

        try: term = int(float(str(term_raw).strip().replace('months','').replace('mo','').strip()))
        except: term = 12
        if term < MIN_TERM:
            rejected['short_term'] += 1; continue

        try: renew = max(0, min(100, int(float(str(renew_raw).strip().replace('%','')))))
        except: renew = 0

        try: cancel = float(str(cancel_raw).strip().replace('$','').replace(',',''))
        except: cancel = 0

        tdu = normalize_tdu(tdu_raw)
        if not tdu:
            rejected['no_data'] += 1; continue

        dedup = f"{provider}|{plan_name}|{tdu}|{rate}"
        if dedup in seen:
            rejected['duplicate'] += 1; continue
        seen.add(dedup)

        plans.append({
            'provider': provider, 'plan_name': plan_name,
            'rate_kwh': rate, 'tdu': tdu, 'term_months': term,
            'renewable_pct': renew, 'cancel_fee': cancel,
            'enroll_url': enroll_url.strip(),
            'updated_at': datetime.now(timezone.utc).isoformat()
        })

    print(f"  {len(plans)} valid plans")
    print(f"  Rejected: {rejected['low_rate']} teaser (<{MIN_RATE}c), "
          f"{rejected['high_rate']} high (>{MAX_RATE}c), "
          f"{rejected['short_term']} short term (<{MIN_TERM}mo), "
          f"{rejected['duplicate']} dupes, {rejected['no_data']} bad data")
    return plans

def upload_to_supabase(plans):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Supabase not configured"); return False
    print(f"Uploading {len(plans)} plans to Supabase...")
    headers = {'Content-Type':'application/json','apikey':SUPABASE_KEY,'Authorization':f'Bearer {SUPABASE_KEY}'}

    # Delete old
    req = Request(f"{SUPABASE_URL}/rest/v1/plans?id=gt.0", method='DELETE')
    for k,v in headers.items(): req.add_header(k,v)
    try:
        with urlopen(req, timeout=15) as resp: pass
    except Exception as e: print(f"  Delete warning: {e}")

    # Insert in batches
    success = 0
    for i in range(0, len(plans), 100):
        batch = plans[i:i+100]
        req = Request(f"{SUPABASE_URL}/rest/v1/plans", data=json.dumps(batch).encode('utf-8'), method='POST')
        for k,v in headers.items(): req.add_header(k,v)
        req.add_header('Prefer','return=minimal')
        try:
            with urlopen(req, timeout=30) as resp:
                if resp.status in (200,201,204): success += len(batch)
        except Exception as e: print(f"  Batch error at {i}: {e}")

    print(f"  Uploaded {success}/{len(plans)} plans")
    return success > 0

def main():
    print("="*60)
    print("WattWise Plan Scraper — PowerToChoose.org")
    print(f"Filters: {MIN_RATE}c <= rate <= {MAX_RATE}c | term >= {MIN_TERM}mo")
    print("="*60)

    csv_text = fetch_ptc_csv()
    if not csv_text: print("Failed to fetch data"); sys.exit(1)

    plans = parse_csv(csv_text)
    if not plans: print("No valid plans"); sys.exit(1)

    rates = sorted([p['rate_kwh'] for p in plans])
    print(f"\nRate range: {rates[0]}c - {rates[-1]}c | Median: {rates[len(rates)//2]}c")

    tdus = {}
    for p in plans: tdus[p['tdu']] = tdus.get(p['tdu'],0)+1
    for tdu,cnt in sorted(tdus.items(), key=lambda x:-x[1]):
        print(f"  {tdu}: {cnt} plans")

    upload_to_supabase(plans)
    print("\nDone!")

if __name__ == '__main__':
    main()
