"""
Telangana Junior College Async Scraper
=======================================
Uses asyncio + aiohttp for fast concurrent scraping.

Sources:
  1. TSBIE portal  -> college name, code, district, address, management
  2. JustDial      -> phone number
  3. Web search    -> principal name, email (optional, set ENRICH_CONTACTS=true)

Requirements:
  pip install aiohttp pandas openpyxl thefuzz[speedup] beautifulsoup4 lxml

Output:
  telangana_colleges.xlsx
"""

import asyncio
import re
import logging
import os
import time
from urllib.parse import quote_plus

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from thefuzz import fuzz

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

CONCURRENCY     = 5         # conservative to avoid blocks
REQUEST_DELAY   = 1.5       # seconds between requests
REQUEST_TIMEOUT = 30        # seconds per request
ENRICH_CONTACTS = os.environ.get("ENRICH_CONTACTS", "false").lower() == "true"
OUTPUT_FILE     = "telangana_colleges.xlsx"
DEBUG_DIR       = "debug_html"   # raw HTML saved here for inspection

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://tsbie.cgg.gov.in/",
}

DISTRICTS = [
    "Adilabad", "Bhadradri Kothagudem", "Hanumakonda", "Hyderabad",
    "Jagtial", "Jangaon", "Jayashankar Bhupalpally", "Jogulamba Gadwal",
    "Kamareddy", "Karimnagar", "Khammam", "Kumuram Bheem Asifabad",
    "Mahabubabad", "Mahbubnagar", "Mancherial", "Medak",
    "Medchal Malkajgiri", "Mulugu", "Nagarkurnool", "Nalgonda",
    "Narayanpet", "Nirmal", "Nizamabad", "Peddapalli",
    "Rajanna Sircilla", "Rangareddy", "Sangareddy", "Siddipet",
    "Suryapet", "Vikarabad", "Wanaparthy", "Warangal", "Yadadri Bhuvanagiri",
]

# ── Debug helper ───────────────────────────────────────────────────────────────

def save_debug_html(name: str, html: str):
    """Save raw HTML to debug_html/ so we can inspect responses in CI logs."""
    os.makedirs(DEBUG_DIR, exist_ok=True)
    path = os.path.join(DEBUG_DIR, f"{name}.html")
    with open(path, "w", encoding="utf-8", errors="replace") as f:
        f.write(html)
    log.info(f"  [debug] saved {path} ({len(html)} bytes)")


# ── Helpers ────────────────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    name = name.lower()
    stopwords = r"\b(junior|jr|college|telangana|andhra|govt|government|private|pvt|the|and|of|for)\b"
    name = re.sub(stopwords, "", name)
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def extract_emails(text: str) -> list:
    emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
    return [e for e in emails if not any(x in e for x in ["google", "schema", "example", "w3.org"])]


def extract_principal(text: str) -> str:
    pattern = r"(?:Principal|Director|Correspondent|Head)[:\s]+([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,3})"
    m = re.search(pattern, text)
    return m.group(1).strip() if m else ""


# ── Source 1: TSBIE ────────────────────────────────────────────────────────────

async def fetch_tsbie_district(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    district: str,
    is_first: bool = False,
) -> list:
    url = "https://tsbie.cgg.gov.in/collegeInfoPublic.do"
    payload = {
        "district": district,
        "collegeType": "ALL",
        "mgmt": "ALL",
        "submit": "Search",
    }

    async with sem:
        try:
            async with session.post(
                url, data=payload,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            ) as r:
                status = r.status
                html = await r.text()
                log.info(f"TSBIE {district}: HTTP {status}, {len(html)} bytes")
                await asyncio.sleep(REQUEST_DELAY)
        except Exception as e:
            log.warning(f"TSBIE {district}: connection error — {e}")
            return []

    # Save first district response for debugging in CI
    if is_first:
        save_debug_html(f"tsbie_{district.replace(' ', '_')}", html)

    # Check for block / captcha / empty response
    if status != 200:
        log.warning(f"TSBIE {district}: non-200 status {status}")
        return []

    if len(html) < 500:
        log.warning(f"TSBIE {district}: response too short ({len(html)} bytes) — likely blocked")
        save_debug_html(f"tsbie_blocked_{district.replace(' ', '_')}", html)
        return []

    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        log.warning(f"TSBIE {district}: no <table> found in response")
        save_debug_html(f"tsbie_notable_{district.replace(' ', '_')}", html)
        return []

    rows = table.find_all("tr")
    results = []
    for row in rows[1:]:
        cols = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        if len(cols) >= 2 and cols[1]:
            results.append({
                "college_code": cols[0] if len(cols) > 0 else "",
                "college_name": cols[1] if len(cols) > 1 else "",
                "district":     district,
                "address":      cols[2] if len(cols) > 2 else "",
                "management":   cols[3] if len(cols) > 3 else "",
            })

    log.info(f"TSBIE {district}: {len(results)} colleges parsed")
    return results


async def scrape_tsbie(session: aiohttp.ClientSession) -> pd.DataFrame:
    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [
        fetch_tsbie_district(session, sem, d, is_first=(i == 0))
        for i, d in enumerate(DISTRICTS)
    ]
    results = await asyncio.gather(*tasks)
    flat = [row for district_rows in results for row in district_rows]
    df = pd.DataFrame(flat) if flat else pd.DataFrame()
    log.info(f"TSBIE total: {len(df)} colleges")
    return df


# ── Source 1b: Fallback — tgbie.cgg.gov.in (same board, different subdomain) ──

async def scrape_tsbie_fallback(session: aiohttp.ClientSession) -> pd.DataFrame:
    """
    Try the newer TGBIE portal as a fallback if TSBIE is blocked.
    """
    log.info("Trying fallback: tgbie.cgg.gov.in ...")
    sem = asyncio.Semaphore(CONCURRENCY)

    async def fetch_one(district: str) -> list:
        url = "https://tgbie.cgg.gov.in/collegeInfoPublic.do"
        payload = {"district": district, "collegeType": "ALL", "mgmt": "ALL", "submit": "Search"}
        async with sem:
            try:
                async with session.post(url, data=payload,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as r:
                    html = await r.text()
                    await asyncio.sleep(REQUEST_DELAY)
            except Exception as e:
                log.warning(f"TGBIE fallback {district}: {e}")
                return []

        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        if not table:
            return []
        rows = table.find_all("tr")
        results = []
        for row in rows[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if len(cols) >= 2 and cols[1]:
                results.append({
                    "college_code": cols[0],
                    "college_name": cols[1],
                    "district":     district,
                    "address":      cols[2] if len(cols) > 2 else "",
                    "management":   cols[3] if len(cols) > 3 else "",
                })
        return results

    tasks = [fetch_one(d) for d in DISTRICTS]
    results = await asyncio.gather(*tasks)
    flat = [row for district_rows in results for row in district_rows]
    df = pd.DataFrame(flat) if flat else pd.DataFrame()
    log.info(f"TGBIE fallback total: {len(df)} colleges")
    return df


# ── Source 2: JustDial ─────────────────────────────────────────────────────────

async def fetch_justdial_district(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    district: str,
) -> list:
    slug = district.lower().replace(" ", "-")
    url = f"https://www.justdial.com/Telangana/Junior-Colleges-in-{slug}/nct-11469205"

    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as r:
                html = await r.text()
                await asyncio.sleep(REQUEST_DELAY)
        except Exception as e:
            log.warning(f"JustDial {district}: {e}")
            return []

    soup = BeautifulSoup(html, "lxml")
    results = []

    cards = (
        soup.find_all("li", class_=re.compile(r"cntanr", re.I)) or
        soup.find_all("div", class_=re.compile(r"jd-card|resultbox", re.I))
    )

    for card in cards:
        name_tag = card.find(["h2", "h3"], class_=re.compile(r"lng|fn|title", re.I))
        name = name_tag.get_text(strip=True) if name_tag else ""

        phone_tag = card.find(attrs={"class": re.compile(r"contact|phone|tel", re.I)})
        phone_raw = phone_tag.get_text(strip=True) if phone_tag else ""
        phone = re.sub(r"[^\d+\-\s]", "", phone_raw).strip()

        addr_tag = card.find(["p", "span"], class_=re.compile(r"address|add|locality", re.I))
        address = addr_tag.get_text(strip=True) if addr_tag else ""

        if name:
            results.append({
                "jd_name":    name,
                "jd_phone":   phone,
                "jd_address": address,
                "district":   district,
            })

    log.info(f"JustDial {district}: {len(results)} listings")
    return results


async def scrape_justdial(session: aiohttp.ClientSession) -> pd.DataFrame:
    sem = asyncio.Semaphore(3)  # very conservative for JustDial
    tasks = [fetch_justdial_district(session, sem, d) for d in DISTRICTS]
    results = await asyncio.gather(*tasks)
    flat = [row for district_rows in results for row in district_rows]
    df = pd.DataFrame(flat) if flat else pd.DataFrame()
    log.info(f"JustDial total: {len(df)} listings")
    return df


# ── Source 3: Contact enrichment ───────────────────────────────────────────────

async def enrich_college_contact(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    college_name: str,
    district: str,
) -> dict:
    result = {"principal": "", "email": ""}
    query = f'"{college_name}" {district} Telangana junior college principal email'
    url = f"https://www.google.com/search?q={quote_plus(query)}&num=5"

    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as r:
                html = await r.text()
                await asyncio.sleep(REQUEST_DELAY)
        except Exception as e:
            log.debug(f"Enrich '{college_name}': {e}")
            return result

    text = BeautifulSoup(html, "lxml").get_text()
    emails = extract_emails(text)
    if emails:
        result["email"] = emails[0]
    principal = extract_principal(text)
    if principal:
        result["principal"] = principal
    return result


async def enrich_all(session: aiohttp.ClientSession, df: pd.DataFrame) -> pd.DataFrame:
    if not ENRICH_CONTACTS:
        log.info("Skipping contact enrichment (set ENRICH_CONTACTS=true to enable)")
        df = df.copy()
        df["principal"] = ""
        df["email"] = ""
        return df

    sem = asyncio.Semaphore(5)
    log.info(f"Enriching contacts for {len(df)} colleges...")
    tasks = [
        enrich_college_contact(session, sem, row["college_name"], row["district"])
        for _, row in df.iterrows()
    ]
    contacts = await asyncio.gather(*tasks)
    df = df.copy()
    df["principal"] = [c["principal"] for c in contacts]
    df["email"]     = [c["email"]     for c in contacts]
    return df


# ── Merge ──────────────────────────────────────────────────────────────────────

def fuzzy_merge(tsbie_df: pd.DataFrame, jd_df: pd.DataFrame, threshold: int = 75) -> pd.DataFrame:
    jd_df = jd_df.copy()
    jd_df["_norm"] = jd_df["jd_name"].apply(normalize_name)

    merged = []
    for _, row in tsbie_df.iterrows():
        district   = row["district"]
        tsbie_norm = normalize_name(row["college_name"])
        candidates = jd_df[jd_df["district"] == district]

        best_score, best_jd = 0, None
        for _, jd_row in candidates.iterrows():
            score = fuzz.token_set_ratio(tsbie_norm, jd_row["_norm"])
            if score > best_score:
                best_score, best_jd = score, jd_row

        merged.append({
            "college_code": row.get("college_code", ""),
            "college_name": row.get("college_name", ""),
            "district":     district,
            "address":      row.get("address", ""),
            "management":   row.get("management", ""),
            "phone":        best_jd["jd_phone"] if best_jd is not None and best_score >= threshold else "",
            "match_score":  best_score if best_score >= threshold else 0,
        })

    return pd.DataFrame(merged)


# ── Save ───────────────────────────────────────────────────────────────────────

def save_excel(df: pd.DataFrame, path: str):
    cols_order = [
        "college_name", "district", "address", "management",
        "principal", "phone", "email", "college_code", "match_score",
    ]
    rename_map = {
        "college_name": "College Name",
        "district":     "District",
        "address":      "Location / Address",
        "management":   "Management Type",
        "principal":    "Principal Name",
        "phone":        "Phone Number",
        "email":        "Email",
        "college_code": "College Code",
        "match_score":  "Match Score",
    }
    # Only keep columns that exist
    cols = [c for c in cols_order if c in df.columns]
    final = df[cols].rename(columns=rename_map)

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        final.to_excel(writer, index=False, sheet_name="Colleges")
        ws = writer.sheets["Colleges"]
        for col in ws.columns:
            width = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(width + 4, 50)

    log.info(f"Saved {len(final)} rows to {path}")
    return final


# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    t0 = time.time()
    log.info("=" * 60)
    log.info("Telangana Junior College Async Scraper")
    log.info(f"ENRICH_CONTACTS={ENRICH_CONTACTS}")
    log.info("=" * 60)

    os.makedirs(DEBUG_DIR, exist_ok=True)

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:

        # ── Step 1: TSBIE (with fallback) ──────────────────────────────────────
        log.info("\n[1/4] Scraping TSBIE portal...")
        tsbie_df = await scrape_tsbie(session)

        if tsbie_df.empty:
            log.warning("TSBIE returned no data — trying fallback portal (tgbie.cgg.gov.in)...")
            tsbie_df = await scrape_tsbie_fallback(session)

        if tsbie_df.empty:
            log.error(
                "Both TSBIE and TGBIE returned no data.\n"
                "This usually means the portal is blocking GitHub Actions IPs.\n"
                "Check the debug_html/ folder in the artifact for the raw responses.\n"
                "Saving an empty placeholder file so the artifact upload doesn't fail."
            )
            # Save empty file so the artifact upload step still succeeds
            placeholder = pd.DataFrame(columns=[
                "College Name", "District", "Location / Address",
                "Management Type", "Principal Name", "Phone Number", "Email",
                "College Code", "Notes"
            ])
            placeholder.loc[0] = [""] * 8 + ["TSBIE portal blocked GitHub Actions IP — run locally or use a proxy"]
            placeholder.to_excel(OUTPUT_FILE, index=False)
            return

        # ── Step 2: JustDial ───────────────────────────────────────────────────
        log.info("\n[2/4] Scraping JustDial for phone numbers...")
        jd_df = await scrape_justdial(session)

        # ── Step 3: Merge ──────────────────────────────────────────────────────
        log.info("\n[3/4] Merging sources...")
        if not jd_df.empty:
            merged_df = fuzzy_merge(tsbie_df, jd_df)
        else:
            log.warning("JustDial empty — continuing with TSBIE data only")
            merged_df = tsbie_df.copy()
            merged_df["phone"] = ""
            merged_df["match_score"] = 0

        phones = (merged_df["phone"] != "").sum()
        log.info(f"Phones matched: {phones}/{len(merged_df)} ({100*phones//max(len(merged_df),1)}%)")

        # Save intermediate checkpoint (in case enrichment fails)
        merged_df["principal"] = ""
        merged_df["email"] = ""
        save_excel(merged_df, OUTPUT_FILE)
        log.info("Checkpoint saved — basic data is safe")

        # ── Step 4: Enrich ─────────────────────────────────────────────────────
        log.info("\n[4/4] Enriching with principal names and emails...")
        merged_df = await enrich_all(session, merged_df)

    # Final save (overwrites checkpoint with enriched data)
    final = save_excel(merged_df, OUTPUT_FILE)

    elapsed = round((time.time() - t0) / 60, 1)
    log.info(f"\nDone in {elapsed} mins")
    log.info(f"  Total colleges : {len(final)}")
    log.info(f"  With phone     : {(final.get('Phone Number', pd.Series()) != '').sum()}")
    log.info(f"  With principal : {(final.get('Principal Name', pd.Series()) != '').sum()}")
    log.info(f"  With email     : {(final.get('Email', pd.Series()) != '').sum()}")


if __name__ == "__main__":
    asyncio.run(main())
