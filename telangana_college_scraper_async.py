"""
Telangana Junior College Async Scraper
=======================================
Uses asyncio + aiohttp for fast concurrent scraping (~11 mins vs ~115 mins sync).

Sources:
  1. TSBIE portal  → college name, code, district, address, management
  2. JustDial      → phone number, sometimes principal name
  3. Web search    → principal name, email (per college)

Requirements:
  pip install aiohttp pandas openpyxl thefuzz[speedup] beautifulsoup4 lxml

Usage:
  python telangana_college_scraper_async.py

Output:
  telangana_colleges.xlsx
"""

import asyncio
import re
import logging
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

# ── Config ────────────────────────────────────────────────────────────────────

CONCURRENCY   = 20        # max simultaneous requests
REQUEST_DELAY = 0.5       # seconds between requests per domain (polite crawling)
REQUEST_TIMEOUT = 20      # seconds per request
ENRICH_CONTACTS = True    # set False to skip principal/email enrichment (faster)
OUTPUT_FILE   = "telangana_colleges.xlsx"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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

# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Normalize college name for fuzzy matching."""
    name = name.lower()
    stopwords = r"\b(junior|jr|college|telangana|andhra|govt|government|private|pvt|the|and|of|for)\b"
    name = re.sub(stopwords, "", name)
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def extract_emails(text: str) -> list[str]:
    emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
    return [e for e in emails if not any(x in e for x in ["google", "schema", "example", "w3.org"])]


def extract_principal(text: str) -> str:
    pattern = r"(?:Principal|Director|Correspondent|Head)[:\s]+([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,3})"
    m = re.search(pattern, text)
    return m.group(1).strip() if m else ""


# ── Source 1: TSBIE ───────────────────────────────────────────────────────────

async def fetch_tsbie_district(session: aiohttp.ClientSession, sem: asyncio.Semaphore, district: str) -> list[dict]:
    """Fetch colleges for one district from the TSBIE portal."""
    url = "https://tsbie.cgg.gov.in/collegeInfoPublic.do"
    payload = {
        "district": district,
        "collegeType": "ALL",
        "mgmt": "ALL",
        "submit": "Search",
    }

    async with sem:
        try:
            async with session.post(url, data=payload, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as r:
                html = await r.text()
                await asyncio.sleep(REQUEST_DELAY)
        except Exception as e:
            log.warning(f"TSBIE {district}: {e}")
            return []

    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        log.warning(f"TSBIE {district}: no table in response")
        return []

    rows = table.find_all("tr")
    results = []
    for row in rows[1:]:
        cols = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        if len(cols) >= 2:
            results.append({
                "college_code": cols[0] if len(cols) > 0 else "",
                "college_name": cols[1] if len(cols) > 1 else "",
                "district":     district,
                "address":      cols[2] if len(cols) > 2 else "",
                "management":   cols[3] if len(cols) > 3 else "",
            })

    log.info(f"TSBIE {district}: {len(results)} colleges")
    return results


async def scrape_tsbie(session: aiohttp.ClientSession) -> pd.DataFrame:
    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [fetch_tsbie_district(session, sem, d) for d in DISTRICTS]
    results = await asyncio.gather(*tasks)
    flat = [row for district_rows in results for row in district_rows]
    df = pd.DataFrame(flat)
    log.info(f"TSBIE total: {len(df)} colleges")
    return df


# ── Source 2: JustDial ────────────────────────────────────────────────────────

async def fetch_justdial_district(session: aiohttp.ClientSession, sem: asyncio.Semaphore, district: str) -> list[dict]:
    """Fetch JustDial listings for junior colleges in one district."""
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

    # JustDial card selectors (they change their markup periodically)
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
    # Use lower concurrency for JustDial to avoid getting rate-limited/blocked
    sem = asyncio.Semaphore(5)
    tasks = [fetch_justdial_district(session, sem, d) for d in DISTRICTS]
    results = await asyncio.gather(*tasks)
    flat = [row for district_rows in results for row in district_rows]
    df = pd.DataFrame(flat)
    log.info(f"JustDial total: {len(df)} listings")
    return df


# ── Source 3: Contact enrichment (principal + email) ─────────────────────────

async def enrich_college_contact(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    college_name: str,
    district: str,
) -> dict:
    """Search for principal name and email for a single college."""
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
        log.info("Skipping contact enrichment (ENRICH_CONTACTS=False)")
        df["principal"] = ""
        df["email"] = ""
        return df

    sem = asyncio.Semaphore(10)  # gentle on Google
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


# ── Merge ─────────────────────────────────────────────────────────────────────

def fuzzy_merge(tsbie_df: pd.DataFrame, jd_df: pd.DataFrame, threshold: int = 75) -> pd.DataFrame:
    """Merge TSBIE + JustDial on fuzzy college name within the same district."""
    # Pre-normalise JD names once for speed
    jd_df = jd_df.copy()
    jd_df["_norm"] = jd_df["jd_name"].apply(normalize_name)

    merged = []
    for _, row in tsbie_df.iterrows():
        district    = row["district"]
        tsbie_norm  = normalize_name(row["college_name"])
        candidates  = jd_df[jd_df["district"] == district]

        best_score, best_jd = 0, None
        for _, jd_row in candidates.iterrows():
            score = fuzz.token_set_ratio(tsbie_norm, jd_row["_norm"])
            if score > best_score:
                best_score, best_jd = score, jd_row

        out = {
            "college_code": row.get("college_code", ""),
            "college_name": row.get("college_name", ""),
            "district":     district,
            "address":      row.get("address", ""),
            "management":   row.get("management", ""),
            "phone":        best_jd["jd_phone"] if best_jd is not None and best_score >= threshold else "",
            "match_score":  best_score if best_score >= threshold else 0,
        }
        merged.append(out)

    return pd.DataFrame(merged)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    t0 = time.time()
    log.info("=" * 60)
    log.info("Telangana Junior College Async Scraper")
    log.info(f"Concurrency: {CONCURRENCY} | Enrich contacts: {ENRICH_CONTACTS}")
    log.info("=" * 60)

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:

        # Step 1: TSBIE
        log.info("\n[1/4] Scraping TSBIE portal...")
        tsbie_df = await scrape_tsbie(session)
        if tsbie_df.empty:
            log.error("TSBIE returned no data. Exiting.")
            return

        # Step 2: JustDial
        log.info("\n[2/4] Scraping JustDial...")
        jd_df = await scrape_justdial(session)

        # Step 3: Merge
        log.info("\n[3/4] Merging data sources...")
        if not jd_df.empty:
            merged_df = fuzzy_merge(tsbie_df, jd_df)
        else:
            log.warning("JustDial returned no data — using TSBIE only")
            merged_df = tsbie_df.copy()
            merged_df["phone"] = ""
            merged_df["match_score"] = 0

        phones = (merged_df["phone"] != "").sum()
        log.info(f"Phone numbers matched: {phones}/{len(merged_df)} ({100*phones//max(len(merged_df),1)}%)")

        # Step 4: Enrich with principal + email
        log.info("\n[4/4] Enriching with principal names and emails...")
        merged_df = await enrich_all(session, merged_df)

    # Save
    final_df = merged_df[[
        "college_name", "district", "address", "management",
        "principal", "phone", "email", "college_code", "match_score",
    ]].rename(columns={
        "college_name": "College Name",
        "district":     "District",
        "address":      "Location / Address",
        "management":   "Management Type",
        "principal":    "Principal Name",
        "phone":        "Phone Number",
        "email":        "Email",
        "college_code": "College Code",
        "match_score":  "Match Score",
    })

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="Colleges")
        ws = writer.sheets["Colleges"]
        for col in ws.columns:
            width = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(width + 4, 50)

    elapsed = round((time.time() - t0) / 60, 1)
    log.info(f"\n✓ Done in {elapsed} mins → {OUTPUT_FILE}")
    log.info(f"  Total colleges : {len(final_df)}")
    log.info(f"  With phone     : {(final_df['Phone Number'] != '').sum()}")
    log.info(f"  With principal : {(final_df['Principal Name'] != '').sum()}")
    log.info(f"  With email     : {(final_df['Email'] != '').sum()}")


if __name__ == "__main__":
    asyncio.run(main())
