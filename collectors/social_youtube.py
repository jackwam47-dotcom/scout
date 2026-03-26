"""
Scout — YouTube Social Collector
Tracks competitor YouTube channels: subscriber count, upload cadence,
recent video titles, view counts, and engagement signals.
Requires: YOUTUBE_API_KEY (free, via Google Cloud Console)
"""

import os
import requests
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

YT_BASE = "https://www.googleapis.com/youtube/v3"


def get_channel_stats(channel_id: str) -> dict:
    """Fetch subscriber count, total views, video count for a channel."""
    resp = requests.get(f"{YT_BASE}/channels", params={
        "key": YOUTUBE_API_KEY,
        "id": channel_id,
        "part": "statistics,snippet",
    })
    resp.raise_for_status()
    items = resp.json().get("items", [])
    if not items:
        return {}
    item = items[0]
    stats = item.get("statistics", {})
    snippet = item.get("snippet", {})
    return {
        "channel_id": channel_id,
        "channel_title": snippet.get("title", ""),
        "subscriber_count": int(stats.get("subscriberCount", 0)),
        "total_views": int(stats.get("viewCount", 0)),
        "video_count": int(stats.get("videoCount", 0)),
    }


def get_recent_videos(channel_id: str, days: int = 14, max_results: int = 10) -> list[dict]:
    """Fetch recent videos published in the last N days."""
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Search for recent uploads
    resp = requests.get(f"{YT_BASE}/search", params={
        "key": YOUTUBE_API_KEY,
        "channelId": channel_id,
        "part": "snippet",
        "order": "date",
        "type": "video",
        "publishedAfter": since,
        "maxResults": max_results,
    })
    resp.raise_for_status()
    items = resp.json().get("items", [])

    if not items:
        return []

    # Get video stats for those videos
    video_ids = ",".join(item["id"]["videoId"] for item in items)
    stats_resp = requests.get(f"{YT_BASE}/videos", params={
        "key": YOUTUBE_API_KEY,
        "id": video_ids,
        "part": "statistics,snippet",
    })
    stats_resp.raise_for_status()
    stats_items = {v["id"]: v for v in stats_resp.json().get("items", [])}

    videos = []
    for item in items:
        vid_id = item["id"]["videoId"]
        stats = stats_items.get(vid_id, {}).get("statistics", {})
        snippet = item.get("snippet", {})
        videos.append({
            "video_id": vid_id,
            "title": snippet.get("title", ""),
            "published_at": snippet.get("publishedAt", ""),
            "view_count": int(stats.get("viewCount", 0)),
            "like_count": int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0)),
            "url": f"https://youtube.com/watch?v={vid_id}",
        })

    return videos


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
    print(f"[youtube] Starting collection for: {client_slug}")

    client_id = get_client_id(client_slug)
    if not client_id:
        print(f"[youtube] ERROR: Client '{client_slug}' not found")
        return

    result = supabase.table("clients").select("config").eq("id", client_id).single().execute()
    competitors = result.data.get("config", {}).get("competitors", [])

    for comp in competitors:
        channel_id = comp.get("youtube_channel_id")
        comp_name = comp.get("name")
        comp_domain = comp.get("domain")

        if not channel_id:
            print(f"[youtube]   No channel ID for {comp_name}, skipping")
            continue

        comp_id = get_competitor_id(client_id, comp_domain)
        if not comp_id:
            print(f"[youtube]   Competitor {comp_name} not in DB, skipping")
            continue

        print(f"[youtube]   Collecting for: {comp_name} ({channel_id})")

        try:
            channel_stats = get_channel_stats(channel_id)
            recent_videos = get_recent_videos(channel_id, days=14)

            # Compute engagement signals
            total_views_14d = sum(v["view_count"] for v in recent_videos)
            upload_count_14d = len(recent_videos)
            top_video = max(recent_videos, key=lambda v: v["view_count"]) if recent_videos else None

            data = {
                "channel_stats": channel_stats,
                "recent_videos": recent_videos,
                "signals": {
                    "subscriber_count": channel_stats.get("subscriber_count", 0),
                    "upload_count_14d": upload_count_14d,
                    "total_views_14d": total_views_14d,
                    "avg_views_per_video": round(total_views_14d / upload_count_14d) if upload_count_14d else 0,
                    "top_video_title": top_video["title"] if top_video else None,
                    "top_video_views": top_video["view_count"] if top_video else 0,
                    "top_video_url": top_video["url"] if top_video else None,
                },
                "collected_date": datetime.utcnow().date().isoformat(),
            }

            supabase.table("signals").insert({
                "client_id": client_id,
                "competitor_id": comp_id,
                "source": "youtube",
                "signal_type": "channel_snapshot",
                "data": data,
                "collected_at": datetime.utcnow().isoformat(),
            }).execute()

            print(f"[youtube]   Stored: {comp_name} — {channel_stats.get('subscriber_count', 0):,} subs, {upload_count_14d} videos in 14d")

        except Exception as e:
            print(f"[youtube]   ERROR for {comp_name}: {e}")

    print(f"[youtube] Done: {client_slug}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python social_youtube.py <client_slug>")
        sys.exit(1)
    collect_for_client(sys.argv[1])
