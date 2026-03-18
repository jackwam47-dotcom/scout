"""
Scout — Google Trends Collector
Pulls search interest data for competitor brands and category keywords.
Uses pytrends (unofficial Google Trends API wrapper) — completely free, no key needed.

Install: pip install pytrends
"""

import os
import time
from datetime import datetime
from pytrends.request import TrendReq
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_trends_client() -> TrendReq:
    """Initialize pytrends with US locale."""
    return TrendReq(hl="en-US", tz=360, timeout=(10, 25))


def get_interest_over_time(pytrends: TrendReq, keywords: list[str], timeframe: str = "now 90-d") -> dict:
    """
    Get search interest over time for up to 5 keywords.
    timeframe options: 'now 7-d', 'now 30-d', 'now 90-d', 'today 12-m', 'today 5-y'
    """
    # Google Trends only supports 5 keywords at a time
    keywords = keywords[:5]

    pytrends.build_payload(keywords, cat=0, timeframe=timeframe, geo="US", gprop="")
    df = pytrends.interest_over_time()

    if df.empty:
        return {"keywords": keywords, "data": [], "timeframe": timeframe}

    # Convert to serializable format
    data = []
    for timestamp, row in df.iterrows():
        entry = {"date": timestamp.strftime("%Y-%m-%d")}
        for kw in keywords:
            if kw in row:
                entry[kw] = int(row[kw])
        data.append(entry)

    return {
        "keywords": keywords,
        "data": data,
        "timeframe": timeframe,
        "data_points": len(data),
    }


def get_related_queries(pytrends: TrendReq, keyword: str) -> dict:
    """Get rising and top related queries for a keyword."""
    pytrends.build_payload([keyword], cat=0, timeframe="now 90-d", geo="US", gprop="")
    related = pytrends.related_queries()

    result = {"keyword": keyword, "rising": [], "top": []}

    if keyword in related:
        rising_df = related[keyword].get("rising")
        top_df = related[keyword].get("top")

        if rising_df is not None and not rising_df.empty:
            result["rising"] = [
                {"query": row["query"], "value": row["value"]}
                for _, row in rising_df.head(10).iterrows()
            ]

        if top_df is not None and not top_df.empty:
            result["top"] = [
                {"query": row["query"], "value": row["value"]}
                for _, row in top_df.head(10).iterrows()
            ]

    return result


def get_interest_by_region(pytrends: TrendReq, keyword: str) -> dict:
    """Get geographic interest breakdown (US states) for a keyword."""
    pytrends.build_payload([keyword], cat=0, timeframe="now 90-d", geo="US", gprop="")
    df = pytrends.interest_by_region(resolution="REGION", inc_low_vol=True, inc_geo_code=False)

    if df.empty:
        return {"keyword": keyword, "regions": []}

    regions = []
    for region, row in df.iterrows():
        if keyword in row and row[keyword] > 0:
            regions.append({"region": region, "value": int(row[keyword])})

    regions.sort(key=lambda x: x["value"], reverse=True)
    return {"keyword": keyword, "regions": regions[:15]}


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
    """Collect Google Trends data for a client and all their competitors."""
    print(f"[trends] Starting collection for: {client_slug}")

    client_id = get_client_id(client_slug)
    if not client_id:
        print(f"[trends] ERROR: Client '{client_slug}' not found")
        return

    result = supabase.table("clients").select("config, name").eq("id", client_id).single().execute()
    config = result.data.get("config", {})
    client_name = result.data.get("name", "")
    competitors = config.get("competitors", [])
    tracked_keywords = config.get("tracked_keywords", [])

    pytrends = get_trends_client()

    # ── 1. Brand comparison: client vs all competitors ────────────────────────
    brand_names = [client_name] + [c.get("name") for c in competitors if c.get("name")]
    brand_names = [b for b in brand_names if b][:5]  # Max 5 for Google Trends

    print(f"[trends]   Brand interest comparison: {brand_names}")
    try:
        brand_trends = get_interest_over_time(pytrends, brand_names, timeframe="now 90-d")
        # Store against each competitor
        for comp in competitors:
            comp_id = get_competitor_id(client_id, comp.get("domain"))
            if comp_id:
                supabase.table("signals").insert({
                    "client_id": client_id,
                    "competitor_id": comp_id,
                    "source": "google_trends",
                    "signal_type": "brand_interest_comparison",
                    "data": brand_trends,
                    "collected_at": datetime.utcnow().isoformat(),
                }).execute()
        time.sleep(2)  # Rate limit courtesy pause
    except Exception as e:
        print(f"[trends]   ERROR brand comparison: {e}")

    # ── 2. Category keyword trends ────────────────────────────────────────────
    if tracked_keywords:
        # Batch into groups of 5
        kw_batches = [tracked_keywords[i:i+5] for i in range(0, len(tracked_keywords), 5)]
        for batch in kw_batches[:2]:  # Max 2 batches to avoid rate limits
            print(f"[trends]   Category keywords: {batch}")
            try:
                kw_trends = get_interest_over_time(pytrends, batch, timeframe="now 90-d")
                # Store as a client-level signal (no specific competitor)
                # Use first competitor as placeholder for client-level signals
                if competitors:
                    comp_id = get_competitor_id(client_id, competitors[0].get("domain"))
                    if comp_id:
                        supabase.table("signals").insert({
                            "client_id": client_id,
                            "competitor_id": comp_id,
                            "source": "google_trends",
                            "signal_type": "category_keyword_trends",
                            "data": kw_trends,
                            "collected_at": datetime.utcnow().isoformat(),
                        }).execute()
                time.sleep(3)
            except Exception as e:
                print(f"[trends]   ERROR category keywords: {e}")

    # ── 3. Per-competitor: rising queries ─────────────────────────────────────
    for comp in competitors:
        comp_name = comp.get("name")
        comp_domain = comp.get("domain")
        comp_id = get_competitor_id(client_id, comp_domain)

        if not comp_id or not comp_name:
            continue

        print(f"[trends]   Rising queries for: {comp_name}")
        try:
            related = get_related_queries(pytrends, comp_name)
            supabase.table("signals").insert({
                "client_id": client_id,
                "competitor_id": comp_id,
                "source": "google_trends",
                "signal_type": "rising_queries",
                "data": related,
                "collected_at": datetime.utcnow().isoformat(),
            }).execute()
            time.sleep(3)  # Be polite to Google
        except Exception as e:
            print(f"[trends]   ERROR rising queries for {comp_name}: {e}")

    print(f"[trends] Done: {client_slug}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python trends_collector.py <client_slug>")
        sys.exit(1)
    collect_for_client(sys.argv[1])
