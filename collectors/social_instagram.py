"""
Scout — Instagram Social Collector
Tracks competitor Instagram accounts: follower count, post cadence,
recent post engagement, and content themes.

Uses Instagram Basic Display API / Graph API.
Requires: INSTAGRAM_ACCESS_TOKEN (long-lived token via Meta developer app)

Note: Instagram's API requires an approved Meta developer app with
instagram_basic and instagram_manage_insights permissions.
The token should be a long-lived token (valid 60 days, auto-refreshed).

For competitors (non-owned accounts), we use public profile scraping
via a lightweight approach that doesn't require competitor OAuth consent.
"""

import os
import requests
import json
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# Instagram Graph API token (for owned account insights)
# For competitor public data, we use the public embed approach
INSTAGRAM_TOKEN = os.environ.get("INSTAGRAM_ACCESS_TOKEN")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

IG_BASE = "https://graph.instagram.com"
IG_GRAPH = "https://graph.facebook.com/v19.0"


def get_public_profile(handle: str) -> dict:
    """
    Fetch public Instagram profile data using the oEmbed endpoint.
    This is a free, no-auth approach for basic follower-level signals.
    Returns available public data without requiring competitor OAuth.
    """
    try:
        # Instagram oEmbed — publicly available, no auth required
        oembed_url = f"https://www.instagram.com/{handle}/?__a=1&__d=dis"
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; ScoutBot/1.0)"
        }
        resp = requests.get(oembed_url, headers=headers, timeout=10)

        if resp.status_code == 200:
            try:
                data = resp.json()
                user = data.get("graphql", {}).get("user", {})
                if user:
                    edge_media = user.get("edge_owner_to_timeline_media", {})
                    recent_posts = edge_media.get("edges", [])[:12]

                    # Calculate engagement from visible posts
                    total_likes = sum(
                        p.get("node", {}).get("edge_liked_by", {}).get("count", 0)
                        for p in recent_posts
                    )
                    total_comments = sum(
                        p.get("node", {}).get("edge_media_to_comment", {}).get("count", 0)
                        for p in recent_posts
                    )
                    post_count = len(recent_posts)

                    posts = []
                    for p in recent_posts:
                        node = p.get("node", {})
                        posts.append({
                            "shortcode": node.get("shortcode"),
                            "timestamp": node.get("taken_at_timestamp"),
                            "likes": node.get("edge_liked_by", {}).get("count", 0),
                            "comments": node.get("edge_media_to_comment", {}).get("count", 0),
                            "is_video": node.get("is_video", False),
                            "caption": (node.get("edge_media_to_caption", {})
                                        .get("edges", [{}])[0]
                                        .get("node", {})
                                        .get("text", ""))[:200] if node.get("edge_media_to_caption", {}).get("edges") else "",
                        })

                    return {
                        "handle": handle,
                        "follower_count": user.get("edge_followed_by", {}).get("count", 0),
                        "following_count": user.get("edge_follow", {}).get("count", 0),
                        "total_posts": user.get("edge_owner_to_timeline_media", {}).get("count", 0),
                        "is_verified": user.get("is_verified", False),
                        "bio": user.get("biography", ""),
                        "recent_posts": posts,
                        "signals": {
                            "post_count_visible": post_count,
                            "avg_likes": round(total_likes / post_count) if post_count else 0,
                            "avg_comments": round(total_comments / post_count) if post_count else 0,
                            "total_engagement": total_likes + total_comments,
                            "engagement_rate": round((total_likes + total_comments) / max(
                                user.get("edge_followed_by", {}).get("count", 1), 1
                            ) / post_count * 100, 3) if post_count else 0,
                        },
                        "method": "public_graph",
                    }
            except (json.JSONDecodeError, KeyError):
                pass

        # Fallback: oEmbed for just the handle existence check
        return {
            "handle": handle,
            "follower_count": None,
            "signals": {},
            "method": "unavailable",
            "note": f"Public data unavailable for @{handle} — Instagram may require login to view",
        }

    except requests.RequestException as e:
        return {
            "handle": handle,
            "error": str(e),
            "method": "error",
        }


def get_post_cadence(posts: list[dict], days: int = 30) -> dict:
    """Calculate posting cadence from recent posts."""
    if not posts:
        return {"posts_per_week": 0, "posts_last_30d": 0}

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    recent = [
        p for p in posts
        if p.get("timestamp") and
        datetime.fromtimestamp(p["timestamp"], tz=timezone.utc) > cutoff
    ]

    return {
        "posts_last_30d": len(recent),
        "posts_per_week": round(len(recent) / (days / 7), 1),
        "video_ratio": round(
            sum(1 for p in recent if p.get("is_video")) / len(recent), 2
        ) if recent else 0,
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
            profile = get_public_profile(handle)
            cadence = get_post_cadence(profile.get("recent_posts", []))

            data = {
                "handle": handle,
                "follower_count": profile.get("follower_count"),
                "total_posts": profile.get("total_posts"),
                "is_verified": profile.get("is_verified"),
                "bio": profile.get("bio"),
                "recent_posts": profile.get("recent_posts", []),
                "signals": {
                    **profile.get("signals", {}),
                    **cadence,
                    "follower_count": profile.get("follower_count"),
                },
                "method": profile.get("method"),
                "collected_date": datetime.utcnow().date().isoformat(),
            }

            supabase.table("signals").insert({
                "client_id": client_id,
                "competitor_id": comp_id,
                "source": "instagram",
                "signal_type": "profile_snapshot",
                "data": data,
                "collected_at": datetime.utcnow().isoformat(),
            }).execute()

            followers = profile.get("follower_count")
            follower_str = f"{followers:,}" if followers else "unknown"
            print(f"[instagram]   Stored: @{handle} — {follower_str} followers, {cadence.get('posts_last_30d', 0)} posts/30d")

        except Exception as e:
            print(f"[instagram]   ERROR for {comp_name}: {e}")

    print(f"[instagram] Done: {client_slug}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python social_instagram.py <client_slug>")
        sys.exit(1)
    collect_for_client(sys.argv[1])
