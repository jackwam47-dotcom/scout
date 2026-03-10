"""
Scout — Google News RSS Collector
Pulls news mentions for competitors using Google News RSS feed.
Completely free, no API key required.
"""

import os
import feedparser
import hashlib
from datetime import datetime
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"


def fetch_news(query: str, max_items: int = 20) -> list[dict]:
    """Fetch Google News RSS for a query."""
    url = GOOGLE_NEWS_RSS.format(query=query.replace(" ", "+"))
    feed = feedparser.parse(url)

    articles = []
    for entry in feed.entries[:max_items]:
        articles.append({
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "published": entry.get("published", ""),
            "summary": entry.get("summary", "")[:500],
            "source": entry.get("source", {}).get("title", "Unknown"),
        })

    return articles


def dedupe_key(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


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


def already_stored(client_id: str, competitor_id: str, url: str) -> bool:
    """Check if this article URL was already stored this week."""
    key = dedupe_key(url)
    result = (
        supabase.table("signals")
        .select("id")
        .eq("client_id", client_id)
        .eq("competitor_id", competitor_id)
        .eq("source", "google_news")
        .contains("data", {"url_hash": key})
        .execute()
    )
    return len(result.data) > 0


def collect_for_client(client_slug: str):
    """Collect news for all competitors of a client."""
    print(f"[news] Starting collection for: {client_slug}")

    client_id = get_client_id(client_slug)
    if not client_id:
        print(f"[news] ERROR: Client '{client_slug}' not found")
        return

    result = supabase.table("clients").select("config").eq("id", client_id).single().execute()
    config = result.data.get("config", {})
    competitors = config.get("competitors", [])

    for comp in competitors:
        comp_domain = comp.get("domain")
        comp_name = comp.get("name", comp_domain)
        comp_id = get_competitor_id(client_id, comp_domain)

        if not comp_id:
            print(f"[news] WARNING: Competitor '{comp_domain}' not in DB, skipping")
            continue

        print(f"[news]   Collecting news for: {comp_name}")

        # Search by brand name and domain
        queries = [
            comp_name,
            comp_domain.replace(".com", ""),
            f'"{comp_name}" marketing',
            f'"{comp_name}" funding OR acquisition OR launch',
        ]

        all_articles = []
        seen_urls = set()

        for query in queries:
            articles = fetch_news(query, max_items=10)
            for article in articles:
                url = article["link"]
                if url not in seen_urls:
                    seen_urls.add(url)
                    article["url_hash"] = dedupe_key(url)
                    article["query"] = query
                    all_articles.append(article)

        # Filter out already stored
        new_articles = [
            a for a in all_articles
            if not already_stored(client_id, comp_id, a["link"])
        ]

        if new_articles:
            supabase.table("signals").insert({
                "client_id": client_id,
                "competitor_id": comp_id,
                "source": "google_news",
                "signal_type": "news_mentions",
                "data": {
                    "articles": new_articles,
                    "count": len(new_articles),
                    "collected_date": datetime.utcnow().date().isoformat(),
                },
                "collected_at": datetime.utcnow().isoformat(),
            }).execute()
            print(f"[news]   Stored {len(new_articles)} new articles for {comp_name}")
        else:
            print(f"[news]   No new articles for {comp_name}")

    print(f"[news] Done: {client_slug}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python news_collector.py <client_slug>")
        sys.exit(1)
    collect_for_client(sys.argv[1])
