"""
Scout — Reddit Collector
Monitors Reddit for brand mentions, product discussions, and sentiment signals.
Uses PRAW (Python Reddit API Wrapper) — free with Reddit app credentials.

Setup: Create a Reddit app at https://www.reddit.com/prefs/apps
Type: script | No redirect URI needed
"""

import os
import praw
from datetime import datetime, timedelta
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = "scout-ci-bot/1.0"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Health/wellness relevant subreddits — customize per client vertical
DEFAULT_SUBREDDITS = [
    "Supplements",
    "nutrition",
    "fitness",
    "bodybuilding",
    "veganfitness",
    "nattyorjuice",
    "loseit",
    "1200isplenty",
    "EatCheapAndHealthy",
    "Nootropics",
]


def get_reddit_client() -> praw.Reddit:
    return praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )


def search_reddit(reddit: praw.Reddit, query: str, subreddits: list[str], days_back: int = 7) -> list[dict]:
    """Search Reddit posts mentioning a brand/product."""
    cutoff = datetime.utcnow() - timedelta(days=days_back)
    results = []

    for sub_name in subreddits:
        try:
            subreddit = reddit.subreddit(sub_name)
            for post in subreddit.search(query, sort="new", time_filter="week", limit=25):
                created = datetime.utcfromtimestamp(post.created_utc)
                if created < cutoff:
                    continue

                # Determine sentiment signal from upvotes/comments
                engagement_score = post.score + (post.num_comments * 3)

                results.append({
                    "post_id": post.id,
                    "subreddit": sub_name,
                    "title": post.title,
                    "url": f"https://reddit.com{post.permalink}",
                    "score": post.score,
                    "num_comments": post.num_comments,
                    "engagement_score": engagement_score,
                    "created_utc": created.isoformat(),
                    "selftext_preview": post.selftext[:300] if post.selftext else "",
                    "flair": post.link_flair_text or "",
                })
        except Exception as e:
            print(f"[reddit] WARNING: Error in r/{sub_name}: {e}")
            continue

    # Sort by engagement
    results.sort(key=lambda x: x["engagement_score"], reverse=True)
    return results


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
    """Collect Reddit mentions for all competitors of a client."""
    print(f"[reddit] Starting collection for: {client_slug}")

    client_id = get_client_id(client_slug)
    if not client_id:
        print(f"[reddit] ERROR: Client '{client_slug}' not found")
        return

    result = supabase.table("clients").select("config").eq("id", client_id).single().execute()
    config = result.data.get("config", {})
    competitors = config.get("competitors", [])
    subreddits = config.get("reddit_subreddits", DEFAULT_SUBREDDITS)

    reddit = get_reddit_client()

    for comp in competitors:
        comp_domain = comp.get("domain")
        comp_name = comp.get("name", comp_domain)
        comp_id = get_competitor_id(client_id, comp_domain)

        if not comp_id:
            print(f"[reddit] WARNING: Competitor '{comp_domain}' not in DB, skipping")
            continue

        print(f"[reddit]   Collecting mentions of: {comp_name}")

        # Search terms — brand name + common variants
        search_terms = [
            comp_name,
            comp_domain.replace(".com", "").replace("-", " "),
        ]
        # Add any custom search terms from config
        search_terms += comp.get("reddit_search_terms", [])

        all_posts = []
        seen_ids = set()

        for term in search_terms:
            posts = search_reddit(reddit, term, subreddits)
            for post in posts:
                if post["post_id"] not in seen_ids:
                    seen_ids.add(post["post_id"])
                    post["search_term"] = term
                    all_posts.append(post)

        # Categorize by signal strength
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
                    "posts": all_posts[:50],  # Cap at 50 most relevant
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
