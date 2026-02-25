#!/usr/bin/env python3
"""
Gratuity Energy Plan Scraper v4.0 — PowerToChoose.org
=====================================================
COMPLETE DATA CAPTURE — All pricing tiers, EFL, fees, fine print.

THE PROBLEM THIS SOLVES:
  REPs game the PowerToChoose system by making plans look cheap at 
  exactly 1000 kWh (the default sort) using usage credits/rebates.
  Example: Plan shows 8.9¢ at 1000 kWh but costs 15.8¢ at 500 kWh 
  and 12.1¢ at 2000 kWh. The 8.9¢ is a mirage — bill credits kick
  in at exactly 1000 kWh to manipulate the ranking.

  Our approach:
  1. Capture ALL 3 pricing tiers (500/1000/2000 kWh)
  2. Compute weighted average = TRUE cost across real usage
  3. Flag plans with big tier spread as "gotcha" plans
  4. Flag plans with rebates/credits/minimum usage fees
  5. Include EFL (Electricity Facts Label) URL for every plan
  6. Include fee details, base charge info, special terms
  7. Sort by WEIGHTED rate, not the gaming-friendly 1000 kWh number

CSV Columns from PowerToChoose.org (all captured):
  idKey, TduCompanyName, RepCompany, Product,
  kwh500, kwh1000, kwh2000,
  Fees_Credits, PrePaid, TimeOfUse, Fixed, RateType,
  Renewable, TermValue, CancelFee, Website,
  SpecialTerms, TermsURL, Promotion, PromotionDesc,
  FactsURL, EnrollURL, PrepaidURL, EnrollPhone,
  NewCustomer, MinUsageFeesCredits
"""

import os
import json
import csv
import io
import sys
import re
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

PTC_URL = 'http://www.powertochoose.org/en-us/Plan/ExportToCsv'

MIN_RATE = 3.0
MAX_RATE = 50.0
MIN_TERM = 3

# Weighted average: most Texas homes use 1000-1500 kWh/mo
# Weight the tiers to reflect realistic usage distribution
TIER_WEIGHTS = {'500': 0.2, '1000': 0.5, '2000': 0.3}

# If rate swings more than this between tiers, flag as gotcha
GOTCHA_VARIANCE_THRESHOLD = 3.0  # cents/kWh

TDU_MAP = {
    'oncor': 'ONCOR', 'oncor electric delivery': 'ONCOR',
    'centerpoint': 'CENTPT', 'centerpoint energy': 'CENTPT', 'cnp': 'CENTPT',
    'texas-new mexico power': 'TNMP', 'texas-new mexico': 'TNMP', 'tnmp': 'TNMP',
    'aep texas central': 'AEP_TCC', 'aep central': 'AEP_TCC',
    'aep texas north': 'AEP_TNC', 'aep north': 'AEP_TNC',
    'lubbock power': 'LPL', 'lubbock power & light': 'LPL',
}

# Keywords that indicate rebate/credit gaming in fee details
REBATE_KEYWORDS = [
    'credit', 'rebate', 'bill credit', 'usage credit', 'discount',
    'bonus', 'gift', 'reward', 'cashback', 'cash back',
    'free nights', 'free weekends', 'free electricity',
    'minimum usage', 'usage fee', 'base charge',
    'surcharge', 'pass-through', 'pass through',
    'tdsp', 'tdu charge'
]

def normalize_tdu(raw):
    if not raw: return None
    low = raw.strip().lower()
    for key, val in TDU_MAP.items():
        if key in low: return val
    return raw.strip().upper()[:20]

def fetch_ptc_csv():
    print("Fetching PowerToChoose.org CSV...")
    req = Request(PTC_URL, headers={
        'User-Agent': 'GratuityEnergy/4.0 (Texas Electricity Comparison)',
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
    """Parse rate from CSV. PTC stores as $/kWh (e.g. 0.089 = 8.9 cents)"""
    if not val: return None
    try:
        cleaned = val.strip().replace('$','').replace('¢','').replace(',','')
        rate = float(cleaned)
        # PTC CSV uses $/kWh format (0.089 = 8.9 cents)
        if 0 < rate < 1:
            rate = rate * 100
        return round(rate, 2)
    except (ValueError, TypeError): return None

def parse_bool(val):
    if not val: return False
    v = str(val).strip().lower()
    return v in ('true', '1', 'yes', 'y', 't')

def detect_rebate_flags(fees_details, special_terms, promo_desc):
    """Scan fine print text for rebate/credit/fee language"""
    combined = ' '.join([
        (fees_details or ''),
        (special_terms or ''),
        (promo_desc or '')
    ]).lower()
    
    found = []
    for kw in REBATE_KEYWORDS:
        if kw in combined:
            found.append(kw)
    
    # Detect specific dollar amounts in credits
    credit_amounts = re.findall(r'\$(\d+(?:\.\d{2})?)\s*(?:credit|rebate|bonus|bill credit)', combined)
    base_charges = re.findall(r'(?:base\s*charge|monthly\s*(?:charge|fee))\s*(?:of\s*)?\$?(\d+(?:\.\d{2})?)', combined)
    min_usage = re.findall(r'(?:minimum\s*usage|less\s*than)\s*(\d+)\s*kwh', combined)
    pass_through = re.findall(r'pass[\s-]*through', combined)
    
    return {
        'has_rebate': bool(credit_amounts),
        'rebate_amount': credit_amounts[0] if credit_amounts else None,
        'has_base_charge': bool(base_charges),
        'base_charge_amount': base_charges[0] if base_charges else None,
        'has_min_usage_fee': bool(min_usage),
        'min_usage_kwh': min_usage[0] if min_usage else None,
        'has_pass_through': bool(pass_through),
        'fine_print_flags': found
    }

def parse_csv(csv_text):
    print("Parsing plans — FULL DATA + GOTCHA DETECTION...")
    reader = csv.DictReader(io.StringIO(csv_text))

    # PTC CSV wraps column headers in square brackets: [RepCompany], [kwh500], etc.
    # Strip brackets so our row.get('RepCompany') lookups work
    if reader.fieldnames:
        reader.fieldnames = [h.strip('[] ') for h in reader.fieldnames]
        print(f"  CSV columns ({len(reader.fieldnames)}): {', '.join(reader.fieldnames[:8])}...")

    plans = []
    rejected = {'low_rate':0, 'high_rate':0, 'short_term':0, 'no_data':0, 'duplicate':0}
    gotcha_count = 0
    rebate_count = 0
    seen = set()

    for row in reader:
        # === CORE FIELDS ===
        provider = (row.get('RepCompany') or row.get('Company') or row.get('Provider') or row.get('rep_company') or '').strip()
        plan_name = (row.get('Product') or row.get('PlanName') or row.get('Plan Name') or row.get('plan_name') or '').strip()
        tdu_raw = (row.get('TduCompanyName') or row.get('TDU') or row.get('tdu_company_name') or row.get('Utility') or '')

        if not provider or not plan_name:
            rejected['no_data'] += 1; continue

        # === ALL THREE PRICING TIERS ===
        rate_500_raw = row.get('kwh500') or row.get('Price500') or row.get('price_kwh500') or ''
        rate_1000_raw = row.get('kwh1000') or row.get('Price1000') or row.get('price_kwh1000') or row.get('Rate') or ''
        rate_2000_raw = row.get('kwh2000') or row.get('Price2000') or row.get('price_kwh2000') or ''

        rate_500 = parse_rate(rate_500_raw)
        rate_1000 = parse_rate(rate_1000_raw)
        rate_2000 = parse_rate(rate_2000_raw)

        if rate_1000 is None:
            rejected['no_data'] += 1; continue
        if rate_1000 < MIN_RATE:
            rejected['low_rate'] += 1; continue
        if rate_1000 > MAX_RATE:
            rejected['high_rate'] += 1; continue

        # === TERM ===
        term_raw = row.get('TermValue') or row.get('Term') or row.get('contract_term') or row.get('ContractTerm') or '12'
        try: term = int(float(str(term_raw).strip().replace('months','').replace('mo','').strip()))
        except: term = 12
        if term < MIN_TERM:
            rejected['short_term'] += 1; continue

        # === RENEWABLE ===
        renew_raw = row.get('Renewable') or row.get('RenewablePercent') or row.get('renewable_pct') or row.get('PercentRenewable') or '0'
        try: renew = max(0, min(100, int(float(str(renew_raw).strip().replace('%','')))))
        except: renew = 0

        # === CANCEL FEE ===
        cancel_raw = row.get('CancelFee') or row.get('EarlyTermFee') or row.get('cancel_fee') or row.get('CancellationFee') or '0'
        try: cancel = float(str(cancel_raw).strip().replace('$','').replace(',',''))
        except: cancel = 0

        # === RATE TYPE ===
        rate_type = (row.get('RateType') or row.get('rate_type') or 'Fixed').strip()

        # === FLAGS ===
        is_prepaid = parse_bool(row.get('PrePaid') or row.get('Prepaid') or row.get('prepaid'))
        is_tou = parse_bool(row.get('TimeOfUse') or row.get('TOU') or row.get('time_of_use'))
        is_fixed = parse_bool(row.get('Fixed') or row.get('fixed'))
        is_new_customer = parse_bool(row.get('NewCustomer') or row.get('new_customer') or row.get('New Customer'))
        is_promotion = parse_bool(row.get('Promotion') or row.get('promotion'))

        # === FEES / CREDITS / FINE PRINT ===
        has_fees_credits = parse_bool(row.get('Fees/Credits') or row.get('Fees_Credits') or row.get('fees_credits') or '')
        # MinUsageFeesCredits is a boolean flag in PTC CSV, not detail text
        has_min_usage_fees_flag = parse_bool(row.get('MinUsageFeesCredits') or '')
        fees_details_raw = (row.get('MinUsageFeesCredits') or '').strip()
        # If it's just TRUE/FALSE, don't store as detail text
        fees_details = '' if fees_details_raw.upper() in ('TRUE','FALSE','') else fees_details_raw
        special_terms = (row.get('SpecialTerms') or row.get('special_terms') or '').strip()
        promo_desc = (row.get('PromotionDesc') or row.get('promotion_desc') or row.get('PromotionDescription') or '').strip()
        # Use special_terms as fees_details if we have no other detail text
        if not fees_details and special_terms:
            fees_details = special_terms

        # === DEEP SCAN FINE PRINT FOR REBATES/BASE CHARGES/PASS-THROUGHS ===
        fine_print = detect_rebate_flags(fees_details, special_terms, promo_desc)

        # === URLS ===
        efl_url = (row.get('FactsURL') or row.get('EflUrl') or row.get('efl_url') or row.get('FactsUrl') or '').strip()
        enroll_url = (row.get('EnrollURL') or row.get('EnrollUrl') or row.get('enroll_url') or row.get('SignupURL') or row.get('GoToUrl') or '').strip()
        terms_url = (row.get('TermsURL') or row.get('TermsUrl') or row.get('terms_url') or '').strip()
        website = (row.get('Website') or row.get('website') or '').strip()
        enroll_phone = (row.get('EnrollPhone') or row.get('enroll_phone') or '').strip()

        # === TDU ===
        tdu = normalize_tdu(tdu_raw)
        if not tdu:
            rejected['no_data'] += 1; continue

        # === DEDUP ===
        dedup = f"{provider}|{plan_name}|{tdu}|{rate_1000}"
        if dedup in seen:
            rejected['duplicate'] += 1; continue
        seen.add(dedup)

        # === COMPUTED: WEIGHTED RATE ===
        r500 = rate_500 if rate_500 else rate_1000
        r2000 = rate_2000 if rate_2000 else rate_1000
        weighted_rate = round(
            r500 * TIER_WEIGHTS['500'] +
            rate_1000 * TIER_WEIGHTS['1000'] +
            r2000 * TIER_WEIGHTS['2000'], 2
        )

        # === COMPUTED: TIER SPREAD (gotcha detection) ===
        tier_rates = [r for r in [rate_500, rate_1000, rate_2000] if r]
        rate_spread = round(max(tier_rates) - min(tier_rates), 2) if len(tier_rates) >= 2 else 0

        is_gotcha = rate_spread > GOTCHA_VARIANCE_THRESHOLD
        if is_gotcha: gotcha_count += 1

        has_rebate = fine_print['has_rebate']
        if has_rebate: rebate_count += 1

        # === BUILD WARNING FLAGS FOR FRONTEND ===
        warnings = []
        if is_gotcha:
            warnings.append('price_varies_by_usage')
        if has_rebate:
            warnings.append('has_rebate_credit')
        if fine_print['has_base_charge']:
            warnings.append('has_base_charge')
        if fine_print['has_min_usage_fee'] or has_min_usage_fees_flag:
            warnings.append('has_min_usage_fee')
        if fine_print['has_pass_through']:
            warnings.append('has_pass_through')
        if has_fees_credits:
            warnings.append('has_usage_fees_credits')
        if is_new_customer:
            warnings.append('new_customers_only')
        if is_promotion:
            warnings.append('promotional_rate')
        if is_prepaid:
            warnings.append('prepaid')
        if is_tou:
            warnings.append('time_of_use')
        if not is_fixed and 'var' in rate_type.lower():
            warnings.append('variable_rate')
        if cancel > 200:
            warnings.append('high_cancel_fee')

        # === TRANSPARENCY SCORE (higher = more straightforward plan) ===
        # Starts at 100, deduct for each red flag
        transparency = 100
        if is_gotcha: transparency -= 25           # big tier spread = gaming
        if has_rebate: transparency -= 20          # rebate inflates one tier
        if fine_print['has_base_charge']: transparency -= 10
        if fine_print['has_min_usage_fee'] or has_min_usage_fees_flag: transparency -= 15
        if is_promotion: transparency -= 10
        if is_tou: transparency -= 10             # TOU pricing is unpredictable
        if not is_fixed: transparency -= 15       # variable rate risk
        if is_prepaid: transparency -= 10
        transparency = max(0, transparency)

        plans.append({
            'provider': provider,
            'plan_name': plan_name,
            'tdu': tdu,

            # ALL THREE PRICING TIERS
            'rate_kwh': rate_1000,
            'rate_kwh500': r500,
            'rate_kwh2000': r2000,
            'weighted_rate': weighted_rate,

            # PLAN DETAILS
            'term_months': term,
            'renewable_pct': renew,
            'cancel_fee': cancel,
            'rate_type': rate_type,

            # FLAGS
            'is_prepaid': is_prepaid,
            'is_tou': is_tou,
            'is_fixed': is_fixed,
            'is_new_customer': is_new_customer,
            'is_promotion': is_promotion,

            # FEES & FINE PRINT
            'has_fees_credits': has_fees_credits,
            'fees_details': fees_details[:500] if fees_details else None,
            'special_terms': special_terms[:500] if special_terms else None,
            'promo_desc': promo_desc[:300] if promo_desc else None,

            # REBATE / BASE CHARGE / PASS-THROUGH DETECTION
            'has_rebate': has_rebate,
            'rebate_amount': fine_print['rebate_amount'],
            'has_base_charge': fine_print['has_base_charge'],
            'base_charge_amount': fine_print['base_charge_amount'],
            'has_min_usage_fee': fine_print['has_min_usage_fee'] or has_min_usage_fees_flag,
            'min_usage_kwh': fine_print['min_usage_kwh'],
            'has_pass_through': fine_print['has_pass_through'],

            # GOTCHA + TRANSPARENCY
            'rate_spread': rate_spread,
            'is_gotcha': is_gotcha,
            'warnings': warnings,
            'transparency_score': transparency,

            # URLS — FACT SHEET + ENROLLMENT
            'efl_url': efl_url or None,
            'enroll_url': enroll_url or None,
            'terms_url': terms_url or None,
            'website': website or None,
            'enroll_phone': enroll_phone or None,

            'updated_at': datetime.now(timezone.utc).isoformat()
        })

    print(f"\n  ✅ {len(plans)} valid plans captured")
    print(f"  ⚠️  {gotcha_count} gotcha plans (>{GOTCHA_VARIANCE_THRESHOLD}¢ spread)")
    print(f"  💰 {rebate_count} plans with rebates/credits in fine print")
    print(f"  Rejected: {rejected['low_rate']} teaser (<{MIN_RATE}¢), "
          f"{rejected['high_rate']} high (>{MAX_RATE}¢), "
          f"{rejected['short_term']} short (<{MIN_TERM}mo), "
          f"{rejected['duplicate']} dupes, {rejected['no_data']} bad data")
    return plans

def upload_to_supabase(plans):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("\nSupabase not configured — sample output:")
        for p in sorted(plans, key=lambda x: x['weighted_rate'])[:5]:
            flag = '⚠️' if p['is_gotcha'] else '✅'
            rebate = '💰' if p['has_rebate'] else '  '
            base = '📋' if p['has_base_charge'] else '  '
            efl = '📄' if p['efl_url'] else '  '
            print(f"  {flag}{rebate}{base}{efl} {p['weighted_rate']:5.1f}¢ wtd | "
                  f"{p['rate_kwh500']:5.1f}/{p['rate_kwh']:5.1f}/{p['rate_kwh2000']:5.1f}¢ "
                  f"(500/1k/2k) | {p['transparency_score']:3d}pts | "
                  f"{p['provider'][:20]} | {p['plan_name'][:30]}")
        return False

    print(f"\nUploading {len(plans)} plans to Supabase...")
    headers = {'Content-Type':'application/json','apikey':SUPABASE_KEY,'Authorization':f'Bearer {SUPABASE_KEY}'}

    # Delete old plans
    req = Request(f"{SUPABASE_URL}/rest/v1/plans?id=gt.0", method='DELETE')
    for k,v in headers.items(): req.add_header(k,v)
    try:
        with urlopen(req, timeout=15) as resp: pass
    except Exception as e: print(f"  Delete warning: {e}")

    # Serialize warnings list as JSON string for Supabase
    for p in plans:
        p['warnings'] = json.dumps(p['warnings']) if p['warnings'] else '[]'

    # Insert in batches of 50
    success = 0
    for i in range(0, len(plans), 50):
        batch = plans[i:i+50]
        req = Request(f"{SUPABASE_URL}/rest/v1/plans", data=json.dumps(batch).encode('utf-8'), method='POST')
        for k,v in headers.items(): req.add_header(k,v)
        req.add_header('Prefer','return=minimal')
        try:
            with urlopen(req, timeout=30) as resp:
                if resp.status in (200,201,204): success += len(batch)
        except Exception as e:
            err_body = ''
            if hasattr(e, 'read'):
                try: err_body = e.read().decode('utf-8')[:500]
                except: pass
            print(f"  Batch error at {i}: {e}")
            if err_body: print(f"    Detail: {err_body}")

    print(f"  Uploaded {success}/{len(plans)} plans")
    return success > 0

def main():
    print("="*60)
    print("⚡ Gratuity Energy Plan Scraper v4.0")
    print("   FULL TRANSPARENCY — All Tiers, EFL, Fees, Fine Print")
    print(f"   Filters: {MIN_RATE}¢ ≤ rate ≤ {MAX_RATE}¢ | term ≥ {MIN_TERM}mo")
    print("="*60)

    csv_text = fetch_ptc_csv()
    if not csv_text:
        print("Failed to fetch data"); sys.exit(1)

    plans = parse_csv(csv_text)
    if not plans:
        print("No valid plans"); sys.exit(1)

    # === ANALYSIS ===
    rates_1k = sorted([p['rate_kwh'] for p in plans])
    rates_w = sorted([p['weighted_rate'] for p in plans])
    gotchas = [p for p in plans if p['is_gotcha']]
    with_efl = [p for p in plans if p.get('efl_url')]
    with_rebate = [p for p in plans if p.get('has_rebate')]
    with_base = [p for p in plans if p.get('has_base_charge')]
    with_minuse = [p for p in plans if p.get('has_min_usage_fee')]
    with_passthru = [p for p in plans if p.get('has_pass_through')]

    print(f"\n{'='*60}")
    print(f"📊 RATE ANALYSIS")
    print(f"{'='*60}")
    print(f"  1000 kWh rates:  {rates_1k[0]}¢ — {rates_1k[-1]}¢  (median {rates_1k[len(rates_1k)//2]}¢)")
    print(f"  Weighted rates:  {rates_w[0]}¢ — {rates_w[-1]}¢  (median {rates_w[len(rates_w)//2]}¢)")
    print(f"\n  📄 Plans with EFL (Fact Sheet) link: {len(with_efl)}/{len(plans)}")
    print(f"  💰 Plans with rebates/credits: {len(with_rebate)}")
    print(f"  📋 Plans with base charges: {len(with_base)}")
    print(f"  ⚡ Plans with minimum usage fees: {len(with_minuse)}")
    print(f"  🔄 Plans with pass-through charges: {len(with_passthru)}")
    print(f"  ⚠️  Gotcha plans (>{GOTCHA_VARIANCE_THRESHOLD}¢ tier spread): {len(gotchas)}")

    if gotchas:
        print(f"\n  ⚠️  WORST GOTCHA PLANS:")
        for p in sorted(gotchas, key=lambda x: -x['rate_spread'])[:5]:
            print(f"     {p['provider']} — {p['plan_name']}")
            print(f"       500: {p['rate_kwh500']}¢ | 1000: {p['rate_kwh']}¢ | 2000: {p['rate_kwh2000']}¢ | Spread: {p['rate_spread']}¢")
            if p.get('fees_details'):
                print(f"       Fine print: {p['fees_details'][:120]}")

    # Show how rankings CHANGE between PTC default sort and our weighted sort
    top10_1k = sorted(plans, key=lambda x: x['rate_kwh'])[:10]
    top10_w = sorted(plans, key=lambda x: x['weighted_rate'])[:10]

    print(f"\n{'='*60}")
    print(f"🏆 RANKING COMPARISON — PowerToChoose vs Gratuity Energy")
    print(f"{'='*60}")
    print(f"\n  PowerToChoose default (1000 kWh sort — easily gamed):")
    for i, p in enumerate(top10_1k, 1):
        flags = []
        if p['is_gotcha']: flags.append('⚠️GOTCHA')
        if p['has_rebate']: flags.append('💰REBATE')
        if p['has_base_charge']: flags.append('📋BASE$')
        f = ' '.join(flags)
        print(f"    #{i:2d}  {p['rate_kwh']:5.1f}¢  {p['provider'][:18]:18s}  {p['plan_name'][:28]:28s}  {f}")

    print(f"\n  Gratuity Energy ranking (weighted rate — TRUE cost):")
    for i, p in enumerate(top10_w, 1):
        flags = []
        if p['is_gotcha']: flags.append('⚠️GOTCHA')
        if p['has_rebate']: flags.append('💰REBATE')
        if p['has_base_charge']: flags.append('📋BASE$')
        score = p['transparency_score']
        f = ' '.join(flags)
        print(f"    #{i:2d}  {p['weighted_rate']:5.1f}¢  T:{score:3d}  {p['provider'][:18]:18s}  {p['plan_name'][:28]:28s}  {f}")

    # TDU breakdown
    tdus = {}
    for p in plans: tdus[p['tdu']] = tdus.get(p['tdu'],0)+1
    print(f"\n  TDU Distribution:")
    for tdu,cnt in sorted(tdus.items(), key=lambda x:-x[1]):
        print(f"     {tdu}: {cnt} plans")

    upload_to_supabase(plans)
    print(f"\n{'='*60}")
    print("✅ Done — Full transparency data ready")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
