"""
Scout — Web Change Detector
Monitors competitor websites for changes to key pages:
homepage, pricing, product pages, about/mission pages.

Uses Playwright for full JS-rendered page capture, then
diffs against stored snapshots to detect meaningful changes.

Install: pip install playwright && playwright install chromium
"""

import os
import json
import hashlib
import re
from datetime import datetime
from difflib import SequenceMatcher
from playwright.sync_api import sync_playwright
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Pages to monitor per competitor — customize in client config
DEFAULT_PAGES = [
    "/",                    # Homepage
    "/about",               # Mission / brand story
    "/products",            # Product listing
    "/pricing",             # Pricing (if exists)
    "/collections/all",     # Shopify catch-all
]

# Sections to extract and diff (CSS selectors)
CONTENT_SELECTORS = [
    "h1",                   # Main headline
    "h2",                   # Section headlines
    "nav",                  # Navigation (tracks new sections)
    "[class*='hero']",      # Hero section
    "[class*='headline']",  # Headline elements
    "[class*='cta']",       # Call to action text
    "title",                # Page title
    "meta[name='description']",  # Meta description
]


def clean_text(text: str) -> str:
    """Normalize whitespace and remove dynamic content."""
    text = re.sub(r'\s+', ' ', text).strip()
    # Remove things that change every load (prices with $ could be real changes, keep them)
    text = re.sub(r'\d{1,2}:\d{2}\s*(AM|PM)', '', text)  # timestamps
    return text


def extract_content(page) -> dict:
    """Extract meaningful text content from key page sections."""
    content = {}

    # Page title
    try:
        content['title'] = page.title()
    except:
        content['title'] = ''

    # Meta description
    try:
        meta = page.query_selector('meta[name="description"]')
        content['meta_description'] = meta.get_attribute('content') if meta else ''
    except:
        content['meta_description'] = ''

    # H1
    try:
        h1s = page.query_selector_all('h1')
        content['h1'] = ' | '.join([clean_text(h.inner_text()) for h in h1s if h.inner_text().strip()])
    except:
        content['h1'] = ''

    # H2s (first 5)
    try:
        h2s = page.query_selector_all('h2')
        content['h2s'] = [clean_text(h.inner_text()) for h in h2s[:5] if h.inner_text().strip()]
    except:
        content['h2s'] = []

    # Navigation links
    try:
        nav_links = page.query_selector_all('nav a')
        content['nav'] = [clean_text(a.inner_text()) for a in nav_links if a.inner_text().strip()]
    except:
        content['nav'] = []

    # Hero section text
    try:
        hero_selectors = ['[class*="hero"]', '[class*="banner"]', '[id*="hero"]', 'section:first-of-type']
        hero_text = ''
        for sel in hero_selectors:
            el = page.query_selector(sel)
            if el:
                hero_text = clean_text(el.inner_text()[:500])
                break
        content['hero'] = hero_text
    except:
        content['hero'] = ''

    # CTA buttons
    try:
        cta_selectors = ['[class*="cta"]', '[class*="btn"]', 'button', 'a[class*="button"]']
        ctas = []
        for sel in cta_selectors:
            els = page.query_selector_all(sel)
            for el in els[:10]:
                text = clean_text(el.inner_text())
                if text and len(text) < 60:
                    ctas.append(text)
        content['ctas'] = list(set(ctas))[:10]
    except:
        content['ctas'] = []

    return content


def content_hash(content: dict) -> str:
    """Create a hash of the content for change detection."""
    serialized = json.dumps(content, sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()


def similarity(a: str, b: str) -> float:
    """Return similarity ratio between two strings (0-1)."""
    return SequenceMatcher(None, a, b).ratio()


def describe_change(old_content: dict, new_content: dict) -> dict:
    """Analyze what specifically changed between two snapshots."""
    changes = []
    significance = 0

    # Check headline changes
    if old_content.get('h1') != new_content.get('h1'):
        old_h1 = old_content.get('h1', '')
        new_h1 = new_content.get('h1', '')
        if old_h1 or new_h1:
            changes.append({
                'field': 'Main Headline (H1)',
                'old': old_h1,
                'new': new_h1,
                'significance': 'high',
            })
            significance += 30

    # Check title changes
    if old_content.get('title') != new_content.get('title'):
        changes.append({
            'field': 'Page Title',
            'old': old_content.get('title', ''),
            'new': new_content.get('title', ''),
            'significance': 'medium',
        })
        significance += 15

    # Check meta description
    if old_content.get('meta_description') != new_content.get('meta_description'):
        changes.append({
            'field': 'Meta Description',
            'old': old_content.get('meta_description', ''),
            'new': new_content.get('meta_description', ''),
            'significance': 'medium',
        })
        significance += 10

    # Check H2s
    old_h2s = set(old_content.get('h2s', []))
    new_h2s = set(new_content.get('h2s', []))
    added_h2s = new_h2s - old_h2s
    removed_h2s = old_h2s - new_h2s
    if added_h2s or removed_h2s:
        changes.append({
            'field': 'Section Headlines (H2)',
            'added': list(added_h2s),
            'removed': list(removed_h2s),
            'significance': 'medium',
        })
        significance += 10 * (len(added_h2s) + len(removed_h2s))

    # Check navigation
    old_nav = set(old_content.get('nav', []))
    new_nav = set(new_content.get('nav', []))
    added_nav = new_nav - old_nav
    removed_nav = old_nav - new_nav
    if added_nav or removed_nav:
        changes.append({
            'field': 'Navigation',
            'added': list(added_nav),
            'removed': list(removed_nav),
            'significance': 'medium',
        })
        significance += 15

    # Check CTAs
    old_ctas = set(old_content.get('ctas', []))
    new_ctas = set(new_content.get('ctas', []))
    added_ctas = new_ctas - old_ctas
    removed_ctas = old_ctas - new_ctas
    if added_ctas or removed_ctas:
        changes.append({
            'field': 'Call-to-Action Buttons',
            'added': list(added_ctas),
            'removed': list(removed_ctas),
            'significance': 'high',
        })
        significance += 20

    # Hero text similarity
    old_hero = old_content.get('hero', '')
    new_hero = new_content.get('hero', '')
    if old_hero and new_hero:
        sim = similarity(old_hero, new_hero)
        if sim < 0.85:
            changes.append({
                'field': 'Hero Section',
                'old': old_hero[:200],
                'new': new_hero[:200],
                'similarity': round(sim, 2),
                'significance': 'high',
            })
            significance += 25

    return {
        'changes': changes,
        'change_count': len(changes),
        'significance_score': min(significance, 100),
        'has_significant_changes': significance >= 20,
    }


def get_stored_snapshot(client_id: str, competitor_id: str, url: str) -> dict | None:
    """Get the most recent snapshot for a URL."""
    url_hash = hashlib.md5(url.encode()).hexdigest()
    result = (
        supabase.table("signals")
        .select("data, collected_at")
        .eq("client_id", client_id)
        .eq("competitor_id", competitor_id)
        .eq("source", "web_change")
        .eq("signal_type", "page_snapshot")
        .contains("data", {"url_hash": url_hash})
        .order("collected_at", desc=True)
        .limit(1)
        .execute()
    )
    if result.data:
        return result.data[0]["data"]
    return None


def save_snapshot(client_id: str, competitor_id: str, url: str, content: dict, change_report: dict | None = None):
    """Save a page snapshot to Supabase."""
    url_hash = hashlib.md5(url.encode()).hexdigest()
    supabase.table("signals").insert({
        "client_id": client_id,
        "competitor_id": competitor_id,
        "source": "web_change",
        "signal_type": "page_snapshot",
        "data": {
            "url": url,
            "url_hash": url_hash,
            "content": content,
            "content_hash": content_hash(content),
            "change_report": change_report,
            "captured_at": datetime.utcnow().isoformat(),
        },
        "collected_at": datetime.utcnow().isoformat(),
    }).execute()


def save_change_alert(client_id: str, competitor_id: str, url: str, change_report: dict, comp_name: str):
    """Save a change detection alert as a separate signal for the synthesizer."""
    supabase.table("signals").insert({
        "client_id": client_id,
        "competitor_id": competitor_id,
        "source": "web_change",
        "signal_type": "page_change_detected",
        "data": {
            "url": url,
            "competitor_name": comp_name,
            "changes": change_report["changes"],
            "change_count": change_report["change_count"],
            "significance_score": change_report["significance_score"],
            "detected_at": datetime.utcnow().isoformat(),
        },
        "collected_at": datetime.utcnow().isoformat(),
    }).execute()


def get_client_id(slug: str) -> str | None:
    result = supabase.table("clients").select("id").eq("slug", slug).single().execute()
    return result.data["id"] if result.data else None


def get_competitor_id(client_id: str, domain: str) -> str | None:
    result = (
        supabase.table("competitors")
        .select("id")
        .eq("client_id", client_id)
        .eq("domain", domain)
        .single()
        .execute()
    )
    return result.data["id"] if result.data else None


def collect_for_client(client_slug: str):
    """Run web change detection for all competitors of a client."""
    print(f"[web_change] Starting detection for: {client_slug}")

    client_id = get_client_id(client_slug)
    if not client_id:
        print(f"[web_change] ERROR: Client '{client_slug}' not found")
        return

    result = supabase.table("clients").select("config").eq("id", client_id).single().execute()
    config = result.data.get("config", {})
    competitors = config.get("competitors", [])

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )

        for comp in competitors:
            comp_domain = comp.get("domain")
            comp_name = comp.get("name", comp_domain)
            comp_id = get_competitor_id(client_id, comp_domain)

            if not comp_id:
                print(f"[web_change] WARNING: '{comp_domain}' not in DB, skipping")
                continue

            # Pages to check — use config override or defaults
            pages_to_check = comp.get("watch_pages", DEFAULT_PAGES)

            print(f"[web_change]   Checking {len(pages_to_check)} pages for: {comp_name}")
            total_changes = 0

            for path in pages_to_check:
                url = f"https://{comp_domain}{path}"
                print(f"[web_change]     {url}")

                try:
                    page = context.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=15000)
                    page.wait_for_timeout(2000)  # Let JS render

                    new_content = extract_content(page)
                    page.close()

                    # Get previous snapshot
                    old_snapshot = get_stored_snapshot(client_id, comp_id, url)

                    if old_snapshot is None:
                        # First time seeing this page — save baseline
                        save_snapshot(client_id, comp_id, url, new_content)
                        print(f"[web_change]       Baseline saved (first run)")
                        continue

                    # Compare against previous
                    old_content = old_snapshot.get("content", {})
                    old_hash = old_snapshot.get("content_hash", "")
                    new_hash = content_hash(new_content)

                    if old_hash == new_hash:
                        print(f"[web_change]       No changes detected")
                        continue

                    # Something changed — analyze what
                    change_report = describe_change(old_content, new_content)

                    if change_report["has_significant_changes"]:
                        print(f"[web_change]       ⚠️  SIGNIFICANT CHANGES — score: {change_report['significance_score']}")
                        save_change_alert(client_id, comp_id, url, change_report, comp_name)
                        total_changes += 1
                    else:
                        print(f"[web_change]       Minor changes (score: {change_report['significance_score']})")

                    # Always save new snapshot
                    save_snapshot(client_id, comp_id, url, new_content, change_report)

                except Exception as e:
                    print(f"[web_change]     ERROR on {url}: {e}")
                    try:
                        page.close()
                    except:
                        pass

            print(f"[web_change]   {comp_name}: {total_changes} significant changes detected")

        context.close()
        browser.close()

    print(f"[web_change] Done: {client_slug}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python web_change_detector.py <client_slug>")
        sys.exit(1)
    collect_for_client(sys.argv[1])
