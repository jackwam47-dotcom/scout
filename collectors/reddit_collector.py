"""
Scout — Reddit Collector
Monitors Reddit for brand mentions, product discussions, and sentiment signals.

Uses rdt-cli (https://github.com/ninoseki/rdt-cli) — free, no API key required,
no Reddit app approval needed. Install via: pip install rdt-cli

This replaces the PRAW-based approach which requires Reddit API approval.
"""

import os
import json
import subprocess
import re
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

DEFAULT_SUBREDDITS = [
    "Mattress",
    "BuyItForLife",
    "HomeImprovement",
    "malelivingspace",
    "femalelivingspace",
    "sleep",
    "Frugal",
    "personalfinance",
]


def rdt_search(query: str, subreddit: str, limit: int = 25) -> list[dict]:
    """
    Run rdt-cli to search Reddit posts in a given subreddit.
    rdt-cli uses Reddit's public JSON API — no auth required.
    """
    try:
        cmd = [
            "rdt", "search",
            "--subreddit", subreddit,
            "--query", query,
            "--limit", str(limit),
            "--json",
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode != 0:
            print(f"[reddit]     rdt error in r/{subreddit}: {result.stderr[:200]}")
            return []

        data = json.loads(result.stdout)
        posts = data if isinstance(data, list) else data.get("data", {}).get("children", [])
        return posts

    except subprocess.TimeoutExpired:
        print(f"[reddit]     Timeout searching r/{subreddit}")
        return []
    except (json.JSONDecodeError, Exception) as e:
        print(f"[reddit]     Error in r/{subreddit}: {e}")
        return []


def rdt_subreddit_posts(subreddit: str, query: str, limit: int = 25) -> list[dict]:
    """
    Fallback: use Reddit's public JSON search API directly via rdt.
    Also tries the subreddit search endpoint.
    """
    try:
        # Try direct subreddit search via Reddit public API
        cmd = [
            "rdt", "subreddit", subreddit,
            "--sort", "new",
            "--limit", str(limit),
            "--json",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return []

        data = json.loads(result.stdout)
        posts = data if isinstance(data, list) else []

        # Filter to posts mentioning the query
        q_lower = query.lower()
        return [
            p for p in posts
            if q_lower in (p.get("title", "") + p.get("selftext", "")).lower()
        ]

    except Exception:
        return []


def normalize_post(raw: dict, subreddit: str, search_term: str) -> dict | None:
    """Normalize a raw rdt-cli post into Scout's standard format."""
    try:
        # rdt-cli can return posts in different shapes depending on version
        if "data" in raw and isinstance(raw["data"], dict):
            post = raw["data"]
        else:
            post = raw

        post_id = post.get("id") or post.get("name", "").replace("t3_", "")
        title = post.get("title", "")
        if not post_id or not title:
            return None

        score = int(post.get("score", 0) or 0)
        num_comments = int(post.get("num_comments", 0) or 0)
        created_utc = post.get("created_utc", 0)
        permalink = post.get("permalink", "")
        selftext = (post.get("selftext", "") or "")[:300]
        sub = post.get("subreddit", subreddit)
        flair = post.get("link_flair_text", "") or ""

        # Filter out old posts
        if created_utc:
            created = datetime.fromtimestamp(float(created_utc), tz=timezone.utc)
            cutoff = datetime.now(timezone.utc) - timedelta(days=14)
            if created < cutoff:
                return None
            created_str = created.isoformat()
        else:
            created_str = datetime.utcnow().isoformat()

        engagement_score = score + (num_comments * 3)
        url = f"https://reddit.com{permalink}" if permalink else f"https://reddit.com/r/{sub}"

        return {
            "post_id": post_id,
            "subreddit": sub,
            "title": title,
            "url": url,
            "score": score,
            "num_comments": num_comments,
            "engagement_score": engagement_score,
            "created_utc": created_str,
            "selftext_preview": selftext,
            "flair": flair,
            "search_term": search_term,
        }

    except Exception as e:
        print(f"[reddit]     Error normalizing post: {e}")
        return None


def search_reddit(query: str, subreddits: list[str]) -> list[dict]:
    """Search across subreddits using rdt-cli."""
    all_posts = []
    seen_ids = set()

    for subreddit in subreddits:
        raw_posts = rdt_search(query, subreddit)

        if not raw_posts:
            # Try fallback method
            raw_posts = rdt_subreddit_posts(subreddit, query)

        for raw in raw_posts:
            post = normalize_post(raw, subreddit, query)
            if post and post["post_id"] not in seen_ids:
                seen_ids.add(post["post_id"])
                all_posts.append(post)

    all_posts.sort(key=lambda x: x["engagement_score"], reverse=True)
    return all_posts


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


def check_rdt_available() -> bool:
    """Check that rdt-cli is installed and working."""
    try:
        result = subprocess.run(["rdt", "--version"], capture_output=True, text=True, timeout=10)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def collect_for_client(client_slug: str):
    """Collect Reddit mentions for all competitors of a client."""
    print(f"[reddit] Starting collection for: {client_slug}")

    if not check_rdt_available():
        print("[reddit] ERROR: rdt-cli not installed. Run: pip install rdt-cli")
        return

    client_id = get_client_id(client_slug)
    if not client_id:
        print(f"[reddit] ERROR: Client '{client_slug}' not found")
        return

    result = supabase.table("clients").select("config").eq("id", client_id).single().execute()
    config = result.data.get("config", {})
    competitors = config.get("competitors", [])
    subreddits = config.get("reddit_subreddits", DEFAULT_SUBREDDITS)

    for comp in competitors:
        comp_domain = comp.get("domain")
        comp_name = comp.get("name", comp_domain)
        comp_id = get_competitor_id(client_id, comp_domain)

        if not comp_id:
            print(f"[reddit]   WARNING: Competitor '{comp_domain}' not in DB, skipping")
            continue

        print(f"[reddit]   Collecting mentions of: {comp_name}")

        # Build search terms
        search_terms = list({
            comp_name,
            comp_domain.replace(".com", "").replace("-", " "),
            *comp.get("reddit_search_terms", []),
        })

        all_posts = []
        seen_ids = set()

        for term in search_terms:
            posts = search_reddit(term, subreddits)
            for post in posts:
                if post["post_id"] not in seen_ids:
                    seen_ids.add(post["post_id"])
                    all_posts.append(post)

        # Sort and categorize
        all_posts.sort(key=lambda x: x["engagement_score"], reverse=True)
        high_engagement = [p for p in all_posts if p["engagement_score"] > 50]
        medium_engagement = [p for p in all_posts if 10 < p["engagement_score"] <= 50]
        low_engagement = [p for p in all_posts if p["engagement_score"] <= 10]

        if all_posts:
            supabase.table("signals").insert({
                "client_id": client_id,
                "competitor_id": comp_id,
                "source": "reddit",
                "signal_type": "brand_mentions",
                "data": {
                    "posts": all_posts[:50],
                    "total_found": len(all_posts),
                    "high_engagement_count": len(high_engagement),
                    "medium_engagement_count": len(medium_engagement),
                    "low_engagement_count": len(low_engagement),
                    "top_subreddits": list({p["subreddit"] for p in high_engagement}),
                    "collected_date": datetime.utcnow().date().isoformat(),
                },
                "collected_at": datetime.utcnow().isoformat(),
            }).execute()
            print(f"[reddit]   Stored {len(all_posts)} posts for {comp_name} ({len(high_engagement)} high-engagement)")
        else:
            print(f"[reddit]   No mentions found for {comp_name}")

    print(f"[reddit] Done: {client_slug}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python reddit_collector.py <client_slug>")
        sys.exit(1)
    collect_for_client(sys.argv[1])
