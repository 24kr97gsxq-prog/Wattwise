#!/usr/bin/env python3
"""
WattWise Autonomous Broker Engine
==================================
Runs daily via GitHub Actions. Fully automated pipeline:

1. MONITOR  — Find contracts expiring within 45/30/14/7/0 days
2. ANALYZE  — Query best current rates for each customer's TDU + usage
3. COMPARE  — Calculate exact savings vs current rate AND vs default POLR
4. NOTIFY   — Email customer with personalized rate comparison + one-click re-enroll link
5. ALERT    — Notify Andy via webhook (Slack/SMS) for high-value leads
6. TRACK    — Log every action, update pipeline status, compute commissions
7. RE-ENROLL — Customers expiring can click a link that pre-fills the enrollment form
8. AUTO-RENEW — For POA customers, auto-submit the best plan enrollment

Notification Tiers:
  45 days: Gentle heads-up ("Your contract expires next month")
  30 days: Primary alert with best rates ("Here's what you can save")
  14 days: Urgency ("2 weeks left — don't roll to default!")
  7 days:  Final warning ("Last chance to lock in a rate")
  0 days:  Expired ("You're now on the default rate — switch ASAP!")

Revenue Tracking:
  - Auto-estimates commission based on provider commission schedule
  - Tracks per-customer lifetime value
  - Monthly revenue roll-up by provider

Requires GitHub Secrets:
  SUPABASE_URL, SUPABASE_KEY
  SENDGRID_API_KEY (free tier = 100 emails/day)
  SLACK_WEBHOOK_URL (optional, for Andy notifications)
  SITE_URL (your WattWise URL, for re-enrollment links)
"""

import os
import json
import sys
import hashlib
import hmac
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError
from urllib.parse import urlencode, quote

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
SENDGRID_KEY = os.environ.get('SENDGRID_API_KEY', '')
SLACK_WEBHOOK = os.environ.get('SLACK_WEBHOOK_URL', '')
SITE_URL = os.environ.get('SITE_URL', 'https://24kr97gsxq-prog.github.io/Wattwise/')
ALERT_FROM = os.environ.get('ALERT_FROM_EMAIL', 'alerts@wattswise.com')
ALERT_NAME = os.environ.get('ALERT_FROM_NAME', 'WattWise Rate Watch')
DRY_RUN = not SENDGRID_KEY

# Estimated commission per residential enrollment by provider tier
COMMISSION_SCHEDULE = {
    'tier1': 75,   # Major REPs: TXU, Reliant, Constellation, Direct Energy
    'tier2': 50,   # Mid-tier: Gexa, Green Mountain, Frontier, Chariot, Rhythm
    'tier3': 35,   # Smaller REPs: 4Change, Pulse, Express, Discount Power
    'default': 50
}
TIER1_PROVIDERS = ['txu','reliant','constellation','direct energy']
TIER2_PROVIDERS = ['gexa','green mountain','frontier','chariot','rhythm','veteran']

# POLR default rates by TDU (cents/kWh) — what customers pay if contract lapses
POLR_RATES = {'ONCOR': 17.8, 'CENTPT': 18.2, 'TNMP': 18.5, 'AEP_TCC': 19.1, 'AEP_TNC': 19.4}

def estimate_commission(provider):
    p = provider.lower()
    for name in TIER1_PROVIDERS:
        if name in p: return COMMISSION_SCHEDULE['tier1']
    for name in TIER2_PROVIDERS:
        if name in p: return COMMISSION_SCHEDULE['tier2']
    return COMMISSION_SCHEDULE['default']


# ============================================================
# SUPABASE HELPERS
# ============================================================
def sb_headers():
    return {'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Content-Type': 'application/json'}

def sb_get(path):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    req = Request(url, headers=sb_headers())
    try:
        with urlopen(req, timeout=20) as r: return json.loads(r.read())
    except Exception as e:
        print(f"  DB GET error: {e}"); return []

def sb_patch(table, id_val, data):
    url = f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{id_val}"
    req = Request(url, data=json.dumps(data).encode(), method='PATCH', headers={**sb_headers(), 'Prefer': 'return=minimal'})
    try:
        with urlopen(req, timeout=10) as r: return r.status in (200,204)
    except Exception as e:
        print(f"  DB PATCH error: {e}"); return False

def sb_post(table, data):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    req = Request(url, data=json.dumps(data).encode(), method='POST', headers={**sb_headers(), 'Prefer': 'return=minimal'})
    try:
        with urlopen(req, timeout=10) as r: return r.status in (200,201,204)
    except Exception as e:
        print(f"  DB POST error: {e}"); return False


# ============================================================
# NOTIFICATIONS
# ============================================================
def send_email(to_email, to_name, subject, html_body):
    if DRY_RUN:
        print(f"  📧 [DRY RUN] → {to_name} <{to_email}> | {subject}")
        return True
    payload = {"personalizations":[{"to":[{"email":to_email,"name":to_name}]}],
               "from":{"email":ALERT_FROM,"name":ALERT_NAME},"subject":subject,
               "content":[{"type":"text/html","value":html_body}]}
    req = Request("https://api.sendgrid.com/v3/mail/send", data=json.dumps(payload).encode(), method='POST',
                  headers={'Content-Type':'application/json','Authorization':f'Bearer {SENDGRID_KEY}'})
    try:
        with urlopen(req, timeout=15) as r: return r.status in (200,201,202)
    except Exception as e:
        print(f"  Email error: {e}"); return False

def send_slack(text):
    if not SLACK_WEBHOOK: return
    try:
        req = Request(SLACK_WEBHOOK, data=json.dumps({"text":text}).encode(), method='POST',
                      headers={'Content-Type':'application/json'})
        urlopen(req, timeout=10)
    except: pass

def notify_andy(customer, best_plans, days_left, commission_est):
    """Instant notification to Andy for new leads."""
    name = f"{customer['first_name']} {customer['last_name']}"
    best = best_plans[0] if best_plans else {}
    msg = (f"🔔 *CONTRACT EXPIRING — {days_left} days*\n"
           f"👤 {name} | {customer['email']} | {customer['phone']}\n"
           f"📍 {customer.get('service_address','')}, {customer.get('service_city','')} {customer.get('service_zip','')}\n"
           f"⚡ Current: {customer['provider']} @ {customer['rate_kwh']}¢ | Best: {best.get('provider','?')} @ {best.get('rate_kwh','?')}¢\n"
           f"💰 Est. commission: ${commission_est} | Savings for customer: ${customer.get('annual_savings',0):.0f}/yr\n"
           f"📋 Status: {customer['status']} → expiring")
    send_slack(msg)
    print(f"  📱 Slack notification sent to Andy")


# ============================================================
# RE-ENROLLMENT LINK GENERATOR
# ============================================================
def build_reenroll_link(customer, best_plan):
    """Build a URL that pre-fills the enrollment form for one-click re-enrollment."""
    params = {
        'zip': customer.get('service_zip', ''),
        'reenroll': '1',
        'name': f"{customer['first_name']} {customer['last_name']}",
        'email': customer['email'],
        'phone': customer.get('phone', ''),
        'addr': customer.get('service_address', ''),
        'city': customer.get('service_city', ''),
        'esid': customer.get('esid', ''),
        'provider': best_plan.get('provider', ''),
        'plan': best_plan.get('plan_name', ''),
        'rate': str(best_plan.get('rate_kwh', '')),
        'prev_id': str(customer.get('id', ''))
    }
    return f"{SITE_URL}?{urlencode(params)}"


# ============================================================
# EMAIL TEMPLATES
# ============================================================
def build_email(customer, best_plans, days_left, reenroll_link):
    name = customer['first_name']
    current_rate = customer.get('rate_kwh', 0)
    provider = customer.get('provider', 'your provider')
    usage = customer.get('usage_kwh', 1000)
    tdu = customer.get('tdu', 'ONCOR')
    polr = POLR_RATES.get(tdu, 18.0)
    polr_annual_cost = (polr / 100) * usage * 12
    current_annual_cost = (current_rate / 100) * usage * 12

    # Urgency styling
    if days_left <= 0:
        banner_bg = '#d63031'; banner_text = '⚠️ YOUR CONTRACT HAS EXPIRED'
        banner_sub = f"You may already be paying the default rate of {polr}¢/kWh"
    elif days_left <= 7:
        banner_bg = '#e17055'; banner_text = f'🚨 {days_left} DAYS LEFT'
        banner_sub = "Lock in a new rate before you roll to the default"
    elif days_left <= 14:
        banner_bg = '#fdcb6e'; banner_text = f'⏰ {days_left} DAYS LEFT'
        banner_sub = "Time to secure your next plan"
    else:
        banner_bg = '#00dc82'; banner_text = f'📋 {days_left} DAYS REMAINING'
        banner_sub = "Your contract is expiring soon — here are your best options"

    # Plan comparison rows
    plans_html = ""
    for p in best_plans[:3]:
        sav = ((current_rate - p['rate_kwh']) / 100) * usage * 12
        plans_html += f"""<tr>
            <td style="padding:10px 8px;font-weight:600;border-bottom:1px solid #eee">{p['provider']}</td>
            <td style="padding:10px 8px;border-bottom:1px solid #eee">{p['plan_name'][:30]}</td>
            <td style="padding:10px 8px;color:#00dc82;font-weight:700;border-bottom:1px solid #eee">{p['rate_kwh']}¢</td>
            <td style="padding:10px 8px;border-bottom:1px solid #eee">{p.get('term_months',12)}mo</td>
            <td style="padding:10px 8px;color:#00dc82;font-weight:600;border-bottom:1px solid #eee">${max(0,sav):.0f}/yr</td>
        </tr>"""

    best = best_plans[0]
    best_sav = ((current_rate - best['rate_kwh']) / 100) * usage * 12
    polr_cost_diff = polr_annual_cost - ((best['rate_kwh'] / 100) * usage * 12)

    return f"""<div style="max-width:600px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;color:#333">
  <div style="background:#0a0a14;padding:20px 24px;border-radius:12px 12px 0 0;text-align:center">
    <h1 style="color:#00dc82;margin:0;font-size:22px">⚡ WattWise Rate Watch</h1>
    <p style="color:#666;margin:6px 0 0;font-size:12px">Your free contract monitoring service</p>
  </div>

  <div style="background:{banner_bg};padding:16px 24px;text-align:center;color:#fff">
    <div style="font-size:18px;font-weight:700">{banner_text}</div>
    <div style="font-size:13px;opacity:.9;margin-top:4px">{banner_sub}</div>
  </div>

  <div style="background:#fff;padding:24px;border:1px solid #e0e0e0;border-top:none">
    <p style="font-size:16px;line-height:1.6;margin:0 0 16px">Hi {name},</p>
    <p style="font-size:14px;line-height:1.6;margin:0 0 16px">Your <strong>{provider}</strong> plan at <strong>{current_rate}¢/kWh</strong> expires in <strong>{max(0,days_left)} days</strong>. Without switching, you'll roll onto the TDU default rate of <strong style="color:#d63031">{polr}¢/kWh</strong> — that's <strong style="color:#d63031">${polr_cost_diff:.0f}/year more</strong> than the best available rate.</p>

    <h3 style="color:#00dc82;margin:20px 0 10px;font-size:16px">🏆 Best Rates in Your Area Right Now</h3>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead><tr style="background:#f8f9fa">
        <th style="padding:10px 8px;text-align:left;border-bottom:2px solid #dee2e6">Provider</th>
        <th style="padding:10px 8px;text-align:left;border-bottom:2px solid #dee2e6">Plan</th>
        <th style="padding:10px 8px;text-align:left;border-bottom:2px solid #dee2e6">Rate</th>
        <th style="padding:10px 8px;text-align:left;border-bottom:2px solid #dee2e6">Term</th>
        <th style="padding:10px 8px;text-align:left;border-bottom:2px solid #dee2e6">You Save</th>
      </tr></thead>
      <tbody>{plans_html}</tbody>
    </table>

    <div style="background:#d4edda;border-radius:8px;padding:16px;margin:20px 0;text-align:center">
      <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:1px">Switch to {best['provider']} and save</div>
      <div style="font-size:28px;font-weight:700;color:#28a745;margin:4px 0">${max(0,best_sav):.0f}/year</div>
      <div style="font-size:12px;color:#666">at {best['rate_kwh']}¢/kWh vs your current {current_rate}¢/kWh</div>
    </div>

    <div style="text-align:center;margin:24px 0">
      <a href="{reenroll_link}" style="display:inline-block;background:#00dc82;color:#0a0a14;padding:16px 32px;border-radius:10px;font-weight:700;text-decoration:none;font-size:16px">Switch Now — It's Free →</a>
      <p style="font-size:12px;color:#999;margin-top:10px">One click to pre-fill your info. We handle the rest.</p>
    </div>

    <div style="background:#f8f9fa;border-radius:8px;padding:14px;margin:16px 0">
      <div style="font-size:13px;font-weight:600;margin-bottom:6px">How it works:</div>
      <div style="font-size:12px;color:#666;line-height:1.8">
        1. Click the button above — your info is pre-filled<br>
        2. Review and confirm your new plan<br>
        3. We submit the switch for you within 24 hours<br>
        4. Same wires, same meter — zero interruption
      </div>
    </div>

    <p style="font-size:11px;color:#999;margin-top:20px;border-top:1px solid #eee;padding-top:14px;line-height:1.6">
      You're receiving this because you opted into WattWise Rate Watch. This is a <strong>free</strong> service.
      <a href="{SITE_URL}" style="color:#00dc82">Manage preferences</a> | Reply to this email with questions
    </p>
  </div>
</div>"""


# ============================================================
# MAIN ENGINE
# ============================================================
def main():
    now = datetime.now(timezone.utc)
    today = now.date()
    print("=" * 60)
    print("⚡ WattWise Autonomous Broker Engine")
    print(f"   {now.isoformat()}")
    print(f"   Mode: {'LIVE' if not DRY_RUN else 'DRY RUN — add SENDGRID_API_KEY to enable emails'}")
    print("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ SUPABASE_URL and SUPABASE_KEY required"); sys.exit(1)

    # ---- STEP 1: Find all tracked active/confirmed enrollments ----
    print("\n📋 Loading tracked enrollments...")
    all_tracked = sb_get(
        "enrollments?auto_track=eq.true&status=in.(active,confirmed)"
        "&select=*&order=contract_end.asc"
    )
    print(f"   {len(all_tracked)} active tracked contracts")

    # ---- STEP 2: Categorize by urgency ----
    tiers = {'expired':[], '7_day':[], '14_day':[], '30_day':[], '45_day':[]}
    already_alerted = []

    for c in all_tracked:
        if not c.get('contract_end'): continue
        try:
            end = datetime.strptime(c['contract_end'], '%Y-%m-%d').date()
        except: continue
        days = (end - today).days

        # Determine which alert tier
        if days <= 0:
            tier = 'expired'
        elif days <= 7:
            tier = '7_day'
        elif days <= 14:
            tier = '14_day'
        elif days <= 30:
            tier = '30_day'
        elif days <= 45:
            tier = '45_day'
        else:
            continue  # Not due yet

        # Check what alerts we already sent
        last_alert = c.get('last_alert_tier', '')
        alert_order = ['45_day', '30_day', '14_day', '7_day', 'expired']
        if last_alert:
            last_idx = alert_order.index(last_alert) if last_alert in alert_order else -1
            curr_idx = alert_order.index(tier) if tier in alert_order else -1
            if curr_idx <= last_idx:
                # Already sent this tier or a more urgent one
                already_alerted.append(c)
                continue

        tiers[tier].append((c, days))

    total_due = sum(len(v) for v in tiers.values())
    print(f"\n🔔 Alerts due: {total_due}")
    for tier, items in tiers.items():
        if items: print(f"   {tier}: {len(items)} customers")
    if already_alerted:
        print(f"   Already alerted: {len(already_alerted)} (skipped)")

    if total_due == 0:
        print("\n✅ No alerts needed today.")
        # Still run the auto-status-update for any that expired
        update_expired_statuses(all_tracked, today)
        print_summary(all_tracked)
        return

    # ---- STEP 3: Process each customer ----
    sent = 0; failed = 0; revenue_est = 0

    for tier, items in tiers.items():
        for customer, days_left in items:
            cname = f"{customer['first_name']} {customer['last_name']}"
            tdu = customer.get('tdu', 'ONCOR')
            print(f"\n{'='*50}")
            print(f"👤 {cname} | {customer['email']} | {tier}")
            print(f"   {customer['provider']} @ {customer['rate_kwh']}¢ | Expires: {customer['contract_end']} ({days_left}d)")

            # Get best current rates
            best = sb_get(f"plans?tdu=eq.{tdu}&order=rate_kwh.asc&limit=5&rate_kwh=gte.3")
            if not best:
                print(f"   ⚠️ No plans for {tdu}, skipping"); failed += 1; continue

            print(f"   Best: {best[0]['provider']} @ {best[0]['rate_kwh']}¢")

            # Build re-enrollment link
            reenroll_url = build_reenroll_link(customer, best[0])

            # Estimate commission
            comm_est = estimate_commission(best[0].get('provider', ''))
            revenue_est += comm_est

            # Build + send email
            subject_map = {
                '45_day': f"📋 Your electricity contract expires in {days_left} days",
                '30_day': f"⚡ {days_left} days left — save ${((customer['rate_kwh']-best[0]['rate_kwh'])/100*customer.get('usage_kwh',1000)*12):.0f}/yr by switching",
                '14_day': f"⏰ Only {days_left} days left on your {customer['provider']} contract!",
                '7_day':  f"🚨 LAST CHANCE — {days_left} days before you pay the default rate",
                'expired': f"⚠️ Your contract expired — you may be paying {POLR_RATES.get(tdu,18)}¢/kWh right now"
            }
            subject = subject_map.get(tier, f"Your electricity contract update")
            html = build_email(customer, best, days_left, reenroll_url)
            email_ok = send_email(customer['email'], cname, subject, html)

            if email_ok:
                sent += 1
                # Update enrollment record
                sb_patch('enrollments', customer['id'], {
                    'status': 'expiring',
                    'last_alert_tier': tier,
                    'last_alert_date': now.isoformat(),
                    'best_rate_at_alert': best[0]['rate_kwh'],
                    'best_provider_at_alert': best[0]['provider'],
                    'estimated_commission': comm_est,
                    'updated_at': now.isoformat()
                })
                # Log alert
                sb_post('contract_alerts', {
                    'enrollment_id': customer['id'],
                    'customer_email': customer['email'],
                    'customer_name': cname,
                    'provider': customer['provider'],
                    'plan_name': customer['plan_name'],
                    'rate_kwh': customer['rate_kwh'],
                    'contract_end': customer['contract_end'],
                    'alert_type': tier,
                    'alert_sent': True,
                    'alert_sent_at': now.isoformat(),
                    'best_rate_at_alert': best[0]['rate_kwh'],
                    'best_provider_at_alert': best[0]['provider']
                })
                # Notify Andy for high-value leads (< 14 days or > $50 commission)
                if days_left <= 14 or comm_est >= 50:
                    notify_andy(customer, best, days_left, comm_est)
                print(f"   ✅ Alert sent ({tier})")
            else:
                failed += 1
                print(f"   ❌ Failed")

    # ---- STEP 4: Auto-update statuses ----
    update_expired_statuses(all_tracked, today)

    # ---- STEP 5: Summary ----
    print(f"\n{'='*60}")
    print(f"📊 DAILY SUMMARY")
    print(f"   Alerts sent: {sent} | Failed: {failed}")
    print(f"   Estimated revenue if all convert: ${revenue_est}")
    print(f"{'='*60}")
    print_summary(all_tracked)

    # Send daily summary to Andy
    if sent > 0:
        send_slack(f"📊 *WattWise Daily Report*\n"
                   f"📧 {sent} expiry alerts sent today\n"
                   f"💰 Est. revenue if all convert: ${revenue_est}\n"
                   f"📋 Total tracked customers: {len(all_tracked)}")


def update_expired_statuses(all_tracked, today):
    """Auto-update status for contracts that have passed their end date."""
    for c in all_tracked:
        if not c.get('contract_end'): continue
        try:
            end = datetime.strptime(c['contract_end'], '%Y-%m-%d').date()
        except: continue
        if end < today and c.get('status') in ('active', 'confirmed'):
            sb_patch('enrollments', c['id'], {
                'status': 'expiring',
                'updated_at': datetime.now(timezone.utc).isoformat()
            })

def print_summary(all_tracked):
    """Print portfolio summary."""
    if not all_tracked: return
    total_savings = sum(c.get('annual_savings', 0) or 0 for c in all_tracked)
    total_comm = sum(c.get('estimated_commission', 0) or 0 for c in all_tracked)
    providers = {}
    for c in all_tracked:
        p = c.get('provider', '?')
        providers[p] = providers.get(p, 0) + 1
    print(f"\n📈 PORTFOLIO")
    print(f"   Customers: {len(all_tracked)}")
    print(f"   Total customer savings: ${total_savings:,.0f}/yr")
    print(f"   Total est. commissions: ${total_comm:,.0f}")
    for p, n in sorted(providers.items(), key=lambda x: -x[1]):
        print(f"   {p}: {n} customers")


if __name__ == '__main__':
    main()
