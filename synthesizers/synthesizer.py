"""
Scout — Claude Synthesizer
Takes raw signals from Supabase and generates weekly briefings
using the Claude API.

Cost estimate: ~$0.10-0.30 per client per week at typical signal volume.
"""

import os
import json
from datetime import datetime, date, timedelta
from anthropic import Anthropic
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
anthropic = Anthropic(api_key=ANTHROPIC_API_KEY)


SYSTEM_PROMPT = """You are Scout, a competitive intelligence analyst for health and wellness brands.
Your job is to analyze raw competitive signals and produce clear, actionable weekly briefings for marketing teams.

You write in a direct, confident tone — no hedging, no filler. Every sentence should drive to an action or insight.
You think like a strategist, not a reporter. Don't just describe what happened — explain what it means and what to do about it.

Output format: Always respond with valid JSON matching the schema provided."""


def build_analysis_prompt(client_name: str, signals: dict, week_of: str) -> str:
    return f"""Analyze the following competitive intelligence signals for {client_name} (week of {week_of}).

## RAW SIGNALS
{json.dumps(signals, indent=2, default=str)}

## YOUR TASK
Produce a weekly competitive briefing as JSON with this exact schema:

{{
  "pressure_score": <integer 0-100, overall competitive pressure this week>,
  "pressure_components": {{
    "organic_search": <0-100>,
    "paid_search": <0-100>,
    "content_velocity": <0-100>,
    "social_buzz": <0-100>
  }},
  "executive_summary": "<2-3 sentence plain-English summary of the week's most important competitive movement>",
  "top_developments": [
    {{
      "type": "alert|watch|opportunity",
      "competitor": "<name>",
      "headline": "<10 words max>",
      "detail": "<2-3 sentences — what this means strategically>",
      "recommended_action": "<1 specific, actionable thing to do this week>",
      "urgency": "immediate|this_week|this_month"
    }}
  ],
  "keyword_movements": [
    {{
      "competitor": "<name>",
      "keyword": "<keyword>",
      "position_change": <integer>,
      "current_position": <integer>,
      "overlap_with_client": <true|false>,
      "monthly_volume": <integer>
    }}
  ],
  "content_signals": [
    {{
      "competitor": "<name>",
      "signal": "<what they published/launched>",
      "implication": "<what this signals strategically>"
    }}
  ],
  "social_signals": [
    {{
      "competitor": "<name>",
      "platform": "youtube|instagram",
      "metric": "<key metric observed>",
      "implication": "<what this signals strategically>"
    }}
  ],
  "reddit_intelligence": [
    {{
      "competitor": "<name>",
      "theme": "<what people are saying>",
      "sentiment": "positive|negative|neutral|mixed",
      "top_post_title": "<title>",
      "engagement_level": "high|medium|low",
      "strategic_note": "<what this means for positioning>"
    }}
  ],
  "week_over_week_changes": {{
    "pressure_score_delta": <integer>,
    "notable_changes": ["<change 1>", "<change 2>"]
  }}
}}

Rules:
- top_developments: 3-5 items sorted by urgency
- alert = immediate threats, watch = trends to monitor, opportunity = gaps to exploit
- Reference actual competitor names, keywords, and numbers
- pressure_score: 0-30 calm, 31-60 moderate, 61-85 elevated, 86-100 high alert"""


def slim_signal(source: str, signal: dict) -> dict:
    """
    Reduce each signal to only what Claude needs for synthesis.
    This prevents token overflow when signal volume is high.
    """
    comp = signal.get("competitor", "Unknown")
    data = signal.get("data", {})

    if source == "semrush":
        # Keep keyword movements, trim to top 20 by volume
        keywords = data.get("keywords", [])
        keywords = sorted(keywords, key=lambda k: k.get("volume", 0), reverse=True)[:20]
        return {
            "competitor": comp,
            "keywords": [
                {
                    "keyword": k.get("keyword"),
                    "position": k.get("position"),
                    "volume": k.get("volume"),
                    "position_change": k.get("position_change", 0),
                }
                for k in keywords
            ],
        }

    elif source == "google_news":
        # Keep top 5 articles by recency, trim to title + source only
        articles = data.get("articles", [])[:5]
        return {
            "competitor": comp,
            "articles": [
                {
                    "title": a.get("title", "")[:120],
                    "source": a.get("source", ""),
                    "published": a.get("published", ""),
                }
                for a in articles
            ],
        }

    elif source == "web_change":
        score = data.get("significance_score")
        if not score or int(score) == 0:
            return None  # Skip baseline snapshots entirely
        changes = data.get("changes", [])
        return {
            "competitor": comp,
            "url": data.get("url", ""),
            "significance_score": score,
            "changes": [
                {
                    "field": c.get("field"),
                    "added": c.get("added", [])[:3],
                    "removed": c.get("removed", [])[:3],
                }
                for c in changes
            ],
        }

    elif source == "reddit":
        posts = data.get("posts", [])[:5]
        return {
            "competitor": comp,
            "subreddit": data.get("subreddit"),
            "posts": [
                {
                    "title": p.get("title", "")[:100],
                    "score": p.get("score", 0),
                    "num_comments": p.get("num_comments", 0),
                }
                for p in posts
            ],
        }

    elif source == "youtube":
        signals = data.get("signals", {})
        return {
            "competitor": comp,
            "subscriber_count": signals.get("subscriber_count"),
            "uploads_14d": signals.get("upload_count_14d"),
            "views_14d": signals.get("total_views_14d"),
            "top_video": signals.get("top_video_title"),
            "top_video_views": signals.get("top_video_views"),
        }

    elif source == "instagram":
        signals = data.get("signals", {})
        return {
            "competitor": comp,
            "followers": signals.get("follower_count"),
            "posts_30d": signals.get("posts_last_30d"),
            "avg_likes": signals.get("avg_likes"),
            "engagement_rate": signals.get("engagement_rate"),
        }

    else:
        # Generic trim — keep data but cap size
        return {
            "competitor": comp,
            "signal_type": signal.get("signal_type"),
            "summary": str(data)[:300],
        }


def fetch_week_signals(client_id: str, days_back: int = 7) -> dict:
    """Pull all signals from the past week, slim them, and organize by source."""
    cutoff = (datetime.utcnow() - timedelta(days=days_back)).isoformat()

    result = (
        supabase.table("signals")
        .select("*, competitors(name, domain)")
        .eq("client_id", client_id)
        .gte("collected_at", cutoff)
        .execute()
    )

    signals = result.data or []

    organized = {
        "semrush": [],
        "google_news": [],
        "web_changes": [],
        "reddit": [],
        "youtube": [],
        "instagram": [],
    }

    for signal in signals:
        comp_name = signal.get("competitors", {}).get("name", "Unknown")
        source = signal.get("source", "other")

        entry = {
            "competitor": comp_name,
            "signal_type": signal["signal_type"],
            "data": signal["data"],
            "collected_at": signal["collected_at"],
        }

        slimmed = slim_signal(source, entry)
        if slimmed is None:
            continue  # Skip filtered signals (e.g. score-0 web changes)

        if source in ("semrush", "semrush_csv"):
            organized["semrush"].append(slimmed)
        elif source == "google_news":
            organized["google_news"].append(slimmed)
        elif source == "web_change":
            organized["web_changes"].append(slimmed)
        elif source == "reddit":
            organized["reddit"].append(slimmed)
        elif source == "youtube":
            organized["youtube"].append(slimmed)
        elif source == "instagram":
            organized["instagram"].append(slimmed)

    # Cap total signals per category to avoid token overflow
    for key in organized:
        organized[key] = organized[key][:15]

    return organized


def get_last_week_score(client_id: str) -> int | None:
    last_week = (date.today() - timedelta(days=7)).isoformat()
    result = (
        supabase.table("briefings")
        .select("pressure_score")
        .eq("client_id", client_id)
        .lte("week_of", last_week)
        .order("week_of", desc=True)
        .limit(1)
        .execute()
    )
    if result.data:
        return result.data[0]["pressure_score"]
    return None


def synthesize_for_client(client_slug: str):
    """Generate weekly briefing for a client."""
    print(f"[synthesizer] Starting synthesis for: {client_slug}")

    result = supabase.table("clients").select("id, name, config").eq("slug", client_slug).single().execute()
    if not result.data:
        print(f"[synthesizer] ERROR: Client '{client_slug}' not found")
        return

    client_id = result.data["id"]
    client_name = result.data["name"]
    week_of = date.today().isoformat()

    existing = (
        supabase.table("briefings")
        .select("id")
        .eq("client_id", client_id)
        .eq("week_of", week_of)
        .execute()
    )
    if existing.data:
        print(f"[synthesizer] Briefing already exists for {client_slug} week of {week_of}, skipping")
        return

    signals = fetch_week_signals(client_id)
    total_signals = sum(len(v) for v in signals.values())
    print(f"[synthesizer] Found {total_signals} signals across all sources")

    # Estimate token size before sending
    prompt = build_analysis_prompt(client_name, signals, week_of)
    estimated_tokens = len(prompt) // 4  # rough estimate
    print(f"[synthesizer] Estimated prompt tokens: ~{estimated_tokens:,}")

    if estimated_tokens > 150000:
        print(f"[synthesizer] WARNING: Prompt is large ({estimated_tokens:,} tokens), trimming further")
        for key in signals:
            signals[key] = signals[key][:5]
        prompt = build_analysis_prompt(client_name, signals, week_of)

    print(f"[synthesizer] Calling Claude API...")
    message = anthropic.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    raw_response = message.content[0].text

    try:
        clean = raw_response.strip()
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
        briefing_data = json.loads(clean.strip())
    except json.JSONDecodeError as e:
        print(f"[synthesizer] ERROR: Failed to parse Claude response as JSON: {e}")
        print(f"Raw response: {raw_response[:500]}")
        return

    last_score = get_last_week_score(client_id)
    if last_score is not None:
        briefing_data["week_over_week_changes"]["pressure_score_delta"] = (
            briefing_data.get("pressure_score", 50) - last_score
        )

    supabase.table("briefings").insert({
        "client_id": client_id,
        "week_of": week_of,
        "pressure_score": briefing_data.get("pressure_score", 50),
        "summary": briefing_data.get("executive_summary", ""),
        "developments": briefing_data.get("top_developments", []),
        "full_report": json.dumps(briefing_data),
        "created_at": datetime.utcnow().isoformat(),
    }).execute()

    print(f"[synthesizer] Briefing saved for {client_slug} — Pressure score: {briefing_data.get('pressure_score')}")
    return briefing_data


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python synthesizer.py <client_slug>")
        sys.exit(1)
    result = synthesize_for_client(sys.argv[1])
    if result:
        print(f"\nExecutive Summary: {result.get('executive_summary')}")
        print(f"Top Developments: {len(result.get('top_developments', []))} items")
