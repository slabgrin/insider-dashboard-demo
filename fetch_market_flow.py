#!/usr/bin/env python3
"""
Fetch US market insider buy/sell flow from SEC quarterly Form 3/4/5 datasets.
"""
import requests, json, zipfile, io, csv
from datetime import datetime, timedelta
from collections import defaultdict

HEADERS  = {"User-Agent": "InsiderTradesTracker contact@example.com"}
BASE_URL = "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets"
LOOKBACK = 90

def completed_quarters(n=2):
    today = datetime.utcnow()
    current_q = (today.month - 1) // 3 + 1
    current_y = today.year
    q, y = current_q - 1, current_y
    if q == 0:
        q, y = 4, y - 1
    pairs = []
    for _ in range(n):
        pairs.append((y, q))
        q -= 1
        if q == 0:
            q, y = 4, y - 1
    return pairs

def fetch_zip(year, qtr):
    url = f"{BASE_URL}/{year}q{qtr}_form345.zip"
    print(f"  Downloading {year} Q{qtr}: {url}", flush=True)
    try:
        r = requests.get(url, headers=HEADERS, timeout=180, stream=True)
        if r.status_code != 200:
            print(f"  HTTP {r.status_code} — skipping"); return None
        data = b"".join(r.iter_content(chunk_size=65536))
        print(f"  Downloaded {len(data)/1e6:.1f} MB")
        return data
    except Exception as e:
        print(f"  Error: {e}"); return None

def normalise_date(raw):
    s = raw.strip()
    if not s:
        return ""
    # YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
    if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
        return s[:10]
    # MM/DD/YYYY
    if len(s) >= 10 and s[2:3] == "/" and s[5:6] == "/":
        p = s[:10].split("/")
        if len(p) == 3:
            return f"{p[2]}-{p[0]}-{p[1]}"
    # YYYYMMDD
    if len(s) >= 8 and s[:8].isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return ""

def parse_nonderiv(zip_bytes):
    rows = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        names = z.namelist()
        print(f"  ZIP contents: {names}")
        tsv = next((n for n in names if "nonderiv_trans" in n.lower()), None)
        if not tsv:
            print("  NONDERIV_TRANS not found in archive"); return rows
        print(f"  Parsing: {tsv}")
        with z.open(tsv) as fh:
            reader = csv.DictReader(
                io.TextIOWrapper(fh, encoding="utf-8", errors="replace"),
                delimiter="\t"
            )
            # Print headers so we know exact column names
            headers = reader.fieldnames
            print(f"  Columns: {headers[:10] if headers else None}")
            first_err = None
            date_col = None
            code_col = None
            acq_col  = None
            sh_col   = None
            px_col   = None
            if headers:
                for h in headers:
                    hl = h.lower().replace("_","")
                    if hl in ("transdate","transactiondate") and date_col is None: date_col = h
                    if hl in ("transcode","transactioncode") and code_col is None: code_col = h
                    if hl in ("transacquireddispcd","acquireddisposedcode") and acq_col is None: acq_col = h
                    if hl in ("transshares","transactionshares") and sh_col is None: sh_col = h
                    if hl in ("transpricepershare","transactionpricepershare") and px_col is None: px_col = h
            print(f"  Detected cols — date:{date_col} code:{code_col} acq:{acq_col} shares:{sh_col} price:{px_col}")
            for row in reader:
                try:
                    raw_date = row.get(date_col or "TRANS_DATE", "") or row.get("transactionDate", "")
                    date = normalise_date(raw_date)
                    if not date or len(date) != 10: continue
                    code = (row.get(code_col or "TRANS_CODE","") or row.get("transactionCode","")).strip().upper()
                    acq  = (row.get(acq_col  or "TRANS_ACQUIRED_DISP_CD","") or row.get("acquiredDisposedCode","")).strip().upper()
                    sh   = float((row.get(sh_col or "TRANS_SHARES","") or row.get("transactionShares","") or "0").replace(",","") or 0)
                    px   = float((row.get(px_col or "TRANS_PRICE_PER_SHARE","") or row.get("transactionPricePerShare","") or "0").replace(",","") or 0)
                    val  = sh * px
                    if code == "P" or (acq == "A" and code not in ("A","M","X","V","I","F","D","G","W")):
                        rows.append({"date": date, "is_buy": True,  "value": val})
                    elif code == "S" or (acq == "D" and code not in ("F","D","G","W","A","M","X")):
                        rows.append({"date": date, "is_buy": False, "value": val})
                except Exception as e:
                    if first_err is None:
                        first_err = str(e)
                    continue
            if first_err:
                print(f"  First exception in rows: {first_err}")
    print(f"  {len(rows):,} rows parsed")
    return rows

def main():
    cutoff = (datetime.utcnow() - timedelta(days=LOOKBACK)).strftime("%Y-%m-%d")
    quarters = completed_quarters(n=2)
    print(f"Using last 2 completed quarters: {quarters}")
    print(f"Cutoff date: {cutoff}")

    all_rows = []
    for year, qtr in quarters:
        raw = fetch_zip(year, qtr)
        if raw:
            all_rows.extend(parse_nonderiv(raw))

    by_date = defaultdict(lambda: {"buy_value":0.0,"sell_value":0.0,"buy_count":0,"sell_count":0})
    for row in all_rows:
        if row["date"] < cutoff: continue
        d = by_date[row["date"]]
        if row["is_buy"]:
            d["buy_count"]  += 1
            d["buy_value"]  += row["value"]
        else:
            d["sell_count"] += 1
            d["sell_value"] += row["value"]

    series = [{"date": dt, **v} for dt, v in sorted(by_date.items())]
    with open("market_flow.json","w") as fh:
        json.dump(series, fh, indent=2)

    total_buys  = sum(x["buy_count"]  for x in series)
    total_sells = sum(x["sell_count"] for x in series)
    total_buy   = sum(x["buy_value"]  for x in series)
    total_sell  = sum(x["sell_value"] for x in series)
    print(f"\nDone — {len(series)} days written to market_flow.json")
    print(f"  Buy trades:  {total_buys:,}")
    print(f"  Sell trades: {total_sells:,}")
    print(f"  Total buy value:  ${total_buy/1e9:.2f}B")
    print(f"  Total sell value: ${total_sell/1e9:.2f}B")
    if series:
        print(f"  First entry: {series[0]}")
        print(f"  Last entry:  {series[-1]}")

if __name__ == "__main__":
    main()
