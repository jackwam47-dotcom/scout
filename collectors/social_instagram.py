"""
Scout — Instagram Social Collector
Uses Meta Graph API with a connected business Instagram account
to fetch public competitor profile data.

Requires: INSTAGRAM_ACCESS_TOKEN (long-lived token from Meta developer app)
          INSTAGRAM_BUSINESS_ACCOUNT_ID (your connected business account ID)
"""

import os
import requests
from datetime import datetime
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
ACCESS_TOKEN = os.environ.get("INSTAGRAM_ACCESS_TOKEN")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

GRAPH_BASE = "https://graph.facebook.com/v19.0"


def get_business_account_id() -> str | None:
    """Get the Instagram business account ID connected to this token."""
    resp = requests.get(f"{GRAPH_BASE}/me/accounts", params={
        "access_token": ACCESS_TOKEN,
        "fields": "instagram_business_account",
    })
    resp.raise_for_status()
    pages = resp.json().get("data", [])
    for page in pages:
        ig = page.get("instagram_business_account")
        if ig:
            return ig["id"]
    return None


def get_competitor_profile(business_account_id: str, handle: str) -> dict:
    """
    Use Instagram Graph API business discovery to fetch a competitor's public profile.
    This only works when using a connected business account token.
    """
    resp = requests.get(f"{GRAPH_BASE}/{business_account_id}", params={
        "access_token": ACCESS_TOKEN,
        "fields": f"business_discovery.fields(username,followers_count,media_count,biography,website,media{{timestamp,like_count,comments_count,media_type,caption}})",
        "username": handle,
    })

    if resp.status_code != 200:
        print(f"[instagram]     API error for @{handle}: {resp.status_code} {resp.text[:200]}")
        return {"handle": handle, "error": resp.text[:200], "method": "api_error"}

    data = resp.json().get("business_discovery", {})
    media = data.get("media", {}).get("data", [])

    # Calculate engagement from recent posts
    total_likes = sum(p.get("like_count", 0) for p in media)
    total_comments = sum(p.get("comments_count", 0) for p in media)
    post_count = len(media)
    follower_count = data.get("followers_count", 0)

    # Posts in last 30 days
    from datetime import timezone, timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    recent = [
        p for p in media
        if p.get("timestamp") and
        datetime.fromisoformat(p["timestamp"].replace("Z", "+00:00")) > cutoff
    ]

    posts = [{
        "timestamp": p.get("timestamp"),
        "likes": p.get("like_count", 0),
        "comments": p.get("comments_count", 0),
        "media_type": p.get("media_type", ""),
        "caption": (p.get("caption", "") or "")[:150],
    } for p in media[:12]]

    return {
        "handle": handle,
        "follower_count": follower_count,
        "media_count": data.get("media_count", 0),
        "biography": data.get("biography", ""),
        "website": data.get("website", ""),
        "recent_posts": posts,
        "signals": {
            "follower_count": follower_count,
            "posts_last_30d": len(recent),
            "posts_per_week": round(len(recent) / 4.3, 1),
            "avg_likes": round(total_likes / post_count) if post_count else 0,
            "avg_comments": round(total_comments / post_count) if post_count else 0,
            "engagement_rate": round(
                (total_likes + total_comments) / max(follower_count, 1) / max(post_count, 1) * 100, 3
            ),
            "video_ratio": round(
                sum(1 for p in media if p.get("media_type") == "VIDEO") / max(post_count, 1), 2
            ),
        },
        "method": "graph_api",
    }


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
    print(f"[instagram] Starting collection for: {client_slug}")

    if not ACCESS_TOKEN:
        print("[instagram] ERROR: INSTAGRAM_ACCESS_TOKEN not set")
        return

    # Get connected business account ID
    try:
        business_account_id = get_business_account_id()
        if not business_account_id:
            print("[instagram] ERROR: No Instagram business account found on this token")
            return
        print(f"[instagram] Using business account ID: {business_account_id}")
    except Exception as e:
        print(f"[instagram] ERROR getting business account: {e}")
        return

    client_id = get_client_id(client_slug)
    if not client_id:
        print(f"[instagram] ERROR: Client '{client_slug}' not found")
        return

    result = supabase.table("clients").select("config").eq("id", client_id).single().execute()
    competitors = result.data.get("config", {}).get("competitors", [])

    for comp in competitors:
        handle = comp.get("instagram_handle")
        comp_name = comp.get("name")
        comp_domain = comp.get("domain")

        if not handle:
            print(f"[instagram]   No handle for {comp_name}, skipping")
            continue

        comp_id = get_competitor_id(client_id, comp_domain)
        if not comp_id:
            print(f"[instagram]   Competitor {comp_name} not in DB, skipping")
            continue

        print(f"[instagram]   Collecting for: {comp_name} (@{handle})")

        try:
            profile = get_competitor_profile(business_account_id, handle)

            supabase.table("signals").insert({
                "client_id": client_id,
                "competitor_id": comp_id,
                "source": "instagram",
                "signal_type": "profile_snapshot",
                "data": {
                    "handle": profile.get("handle"),
                    "follower_count": profile.get("follower_count"),
                    "media_count": profile.get("media_count"),
                    "biography": profile.get("biography"),
                    "recent_posts": profile.get("recent_posts", []),
                    "signals": profile.get("signals", {}),
                    "method": profile.get("method"),
                    "collected_date": datetime.utcnow().date().isoformat(),
                },
                "collected_at": datetime.utcnow().isoformat(),
            }).execute()

            followers = profile.get("follower_count") or 0
            posts_30d = profile.get("signals", {}).get("posts_last_30d", 0)
            print(f"[instagram]   Stored: @{handle} — {followers:,} followers, {posts_30d} posts/30d")

        except Exception as e:
            print(f"[instagram]   ERROR for {comp_name}: {e}")

    print(f"[instagram] Done: {client_slug}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python social_instagram.py <client_slug>")
        sys.exit(1)
    collect_for_client(sys.argv[1])
