#!/usr/bin/env python3
"""
Fetch insider trades (Form 4) from SEC EDGAR for a defined portfolio.
Fixes:
  - Robust XML file detection (handles .htm primary docs)
  - Derivative transactions included (RSUs, options, awards)
  - Expanded transaction codes beyond P/S
  - Forms 3, 4, and 5 all fetched
"""

import requests, json, re, time
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

TICKERS = [
    "PLTR","NVDA","AAPL","MSFT","META","AMZN","GOOG","AVGO",
    "NOW","UNH","V","MA","MELI","PANW","FTNT","MNST",
    "LLY","ASML","AXON","MPWR","APH"
]

HEADERS  = {"User-Agent": "InsiderTradesTracker contact@example.com"}
LOOKBACK = 90
SLEEP    = 0.2
FETCH_FORMS = {"3", "4", "5"}

# ── transaction code metadata ─────────────────────────────────────────────────
TXN_META = {
    "P": ("BUY",  "Open Market Buy",        "open_market"),
    "S": ("SELL", "Open Market Sell",        "open_market"),
    "M": ("BUY",  "Option Exercise",         "derivative"),
    "X": ("BUY",  "Option Exercise (ITM)",   "derivative"),
    "A": ("BUY",  "Award / Grant",           "award"),
    "F": ("SELL", "Tax Withholding",         "tax"),
    "D": ("SELL", "Disposed to Issuer",      "other"),
    "G": ("SELL", "Gift",                    "other"),
    "J": (None,   "Other",                   "other"),
    "W": ("SELL", "Inheritance / Will",      "other"),
    "V": (None,   "10b5-1 Plan",             "other"),
    "I": ("BUY",  "Discretionary Txn",      "other"),
    "Z": (None,   "Voting Trust",            "other"),
}

# ── helpers ───────────────────────────────────────────────────────────────────

def get_cik_map():
    r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS)
    r.raise_for_status()
    return {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in r.json().values()}

def get_recent_filings(cik):
    r = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=HEADERS)
    r.raise_for_status()
    data   = r.json()
    recent = data.get("filings", {}).get("recent", {})
    cutoff = (datetime.utcnow() - timedelta(days=LOOKBACK)).strftime("%Y-%m-%d")
    results = []
    forms    = recent.get("form", [])
    accnos   = recent.get("accessionNumber", [])
    dates    = recent.get("filingDate", [])
    primdocs = recent.get("primaryDocument", [])
    for i, f in enumerate(forms):
        if f in FETCH_FORMS and dates[i] >= cutoff:
            results.append({
                "form":         f,
                "accession":    accnos[i],
                "filing_date":  dates[i],
                "primary_doc":  primdocs[i] if i < len(primdocs) else "",
            })
    return results

def find_ownership_xml_url(cik_int, accession, primary_doc):
    """
    Locate the ownershipDocument XML file in an EDGAR filing.
    Primary docs are often .htm wrappers — we need the raw .xml data file.
    """
    acc_nodash = accession.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}"

    # 1. Primary doc is already an XML file → use directly
    if primary_doc and primary_doc.lower().endswith(".xml"):
        return f"{base}/{primary_doc}"

    # 2. Scrape the filing directory index for .xml candidates
    candidates = []
    try:
        idx = requests.get(f"{base}/", headers=HEADERS, timeout=10)
        if idx.status_code == 200:
            # Full absolute paths like /Archives/edgar/data/.../file.xml
            found = re.findall(r'href="(/Archives/edgar/data/[^"]+\.xml)"', idx.text, re.I)
            for path in found:
                fname = path.split("/")[-1].lower()
                # Skip XSLT stylesheets and schema files
                if any(x in fname for x in ("xsl", ".xsd", "viewer")):
                    continue
                candidates.append(f"https://www.sec.gov{path}")
    except Exception:
        pass

    # 3. Common fallback filenames
    candidates += [
        f"{base}/{accession}.xml",
        f"{base}/form4.xml",
        f"{base}/form3.xml",
        f"{base}/form5.xml",
    ]

    return candidates  # return list; caller will try each


def fetch_xml(cik_int, accession, primary_doc):
    """Fetch and return the ownershipDocument XML string, or None."""
    urls = find_ownership_xml_url(cik_int, accession, primary_doc)
    if isinstance(urls, str):
        urls = [urls]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=12)
            if r.status_code == 200 and "<ownershipDocument" in r.text:
                return r.text
        except Exception:
            pass
    return None


# ── parser ────────────────────────────────────────────────────────────────────

def txt(node, path):
    el = node.find(path)
    return el.text.strip() if el is not None and el.text else ""

def safe_float(s):
    try:    return float(str(s).replace(",", ""))
    except: return 0.0

def parse_owner(root):
    name  = txt(root, "reportingOwner/reportingOwnerId/rptOwnerName")
    is_dir  = txt(root, "reportingOwner/reportingOwnerRelationship/isDirector") == "1"
    is_off  = txt(root, "reportingOwner/reportingOwnerRelationship/isOfficer")  == "1"
    is_10   = txt(root, "reportingOwner/reportingOwnerRelationship/isTenPercentOwner") == "1"
    title   = txt(root, "reportingOwner/reportingOwnerRelationship/officerTitle")
    if is_off and title:  role = title
    elif is_dir:          role = "Director"
    elif is_off:          role = "Officer"
    elif is_10:           role = "10% Owner"
    else:                 role = "Insider"
    return name, role

def build_trade(ticker, company, owner, role, filing_date,
                security, txn_date, code, acq_disp,
                shares, price, post_shares, is_derivative=False):
    meta = TXN_META.get(code, (None, code, "other"))
    direction = meta[0]
    if direction is None:
        direction = "BUY" if acq_disp == "A" else "SELL"
    shares_f = safe_float(shares)
    price_f  = safe_float(price)
    total    = round(shares_f * price_f, 2)
    return {
        "ticker":       ticker,
        "company":      company,
        "insider":      owner,
        "role":         role,
        "type":         direction,
        "txn_code":     code,
        "txn_label":    meta[1],
        "txn_category": meta[2],
        "shares":       shares_f,
        "price":        price_f,
        "total_value":  total,
        "security":     security,
        "is_derivative": is_derivative,
        "txn_date":     txn_date,
        "filing_date":  filing_date,
        "shares_after": post_shares,
    }

def parse_form4(xml_text, ticker, filing_date):
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    company = txt(root, "issuer/issuerName")
    sym     = txt(root, "issuer/issuerTradingSymbol") or ticker
    owner, role = parse_owner(root)
    trades = []

    # ── non-derivative transactions (direct stock buys/sells) ──
    for txn in root.findall(".//nonDerivativeTransaction"):
        code     = txt(txn, "transactionCoding/transactionCode")
        if not code: continue
        trades.append(build_trade(
            sym.upper(), company, owner, role, filing_date,
            security    = txt(txn, "securityTitle/value"),
            txn_date    = txt(txn, "transactionDate/value") or filing_date,
            code        = code,
            acq_disp    = txt(txn, "transactionAmounts/transactionAcquiredDisposedCode/value"),
            shares      = txt(txn, "transactionAmounts/transactionShares/value"),
            price       = txt(txn, "transactionAmounts/transactionPricePerShare/value"),
            post_shares = txt(txn, "postTransactionAmounts/sharesOwnedFollowingTransaction/value"),
            is_derivative = False,
        ))

    # ── derivative transactions (options, RSUs, awards) ──
    for txn in root.findall(".//derivativeTransaction"):
        code     = txt(txn, "transactionCoding/transactionCode")
        if not code: continue
        # Use underlying shares if transactionShares is 0
        shares = (txt(txn, "transactionAmounts/transactionShares/value")
               or txt(txn, "underlyingSecurity/underlyingSecurityShares/value"))
        trades.append(build_trade(
            sym.upper(), company, owner, role, filing_date,
            security    = txt(txn, "securityTitle/value"),
            txn_date    = txt(txn, "transactionDate/value") or filing_date,
            code        = code,
            acq_disp    = txt(txn, "transactionAmounts/transactionAcquiredDisposedCode/value"),
            shares      = shares,
            price       = txt(txn, "transactionAmounts/transactionPricePerShare/value")
                       or txt(txn, "conversionOrExercisePrice/value"),
            post_shares = txt(txn, "postTransactionAmounts/sharesOwnedFollowingTransaction/value"),
            is_derivative = True,
        ))

    return trades


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("Fetching CIK map...")
    cik_map = get_cik_map()

    all_trades = []
    for ticker in TICKERS:
        cik = cik_map.get(ticker.upper())
        if not cik:
            print(f"  [SKIP] {ticker}: CIK not found"); continue
        cik_int = int(cik)
        print(f"  [{ticker}] CIK={cik_int}", end="", flush=True)
        try:
            filings = get_recent_filings(cik)
            print(f" — {len(filings)} filing(s)")
            for f in filings:
                time.sleep(SLEEP)
                xml = fetch_xml(cik_int, f["accession"], f["primary_doc"])
                if not xml:
                    print(f"    ⚠ could not fetch XML for {f['accession']}")
                    continue
                trades = parse_form4(xml, ticker, f["filing_date"])
                all_trades.extend(trades)
        except Exception as e:
            print(f" ERROR: {e}")

    all_trades.sort(key=lambda x: x["txn_date"], reverse=True)
    output = {
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "trades": all_trades,
    }
    with open("trades.json", "w") as fh:
        json.dump(output, fh, indent=2)
    print(f"\nDone — {len(all_trades)} trades written to trades.json")

if __name__ == "__main__":
    main()
