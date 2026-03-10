"""
Scout — Claude Synthesizer
Takes raw signals from Supabase and generates weekly briefings
using the Claude API (claude-sonnet-4-20250514).

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


# ── Prompt Templates ──────────────────────────────────────────────────────────

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
      "headline": "<10 words max — what happened>",
      "detail": "<2-3 sentences — what this means strategically>",
      "recommended_action": "<1 specific, actionable thing to do this week>",
      "urgency": "immediate|this_week|this_month"
    }}
  ],
  "keyword_movements": [
    {{
      "competitor": "<name>",
      "keyword": "<keyword>",
      "position_change": <integer, positive = moved up, negative = moved down>,
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
  "reddit_intelligence": [
    {{
      "competitor": "<name>",
      "theme": "<what people are saying>",
      "sentiment": "positive|negative|neutral|mixed",
      "top_post_title": "<title of highest engagement post>",
      "engagement_level": "high|medium|low",
      "strategic_note": "<what this means for positioning>"
    }}
  ],
  "week_over_week_changes": {{
    "pressure_score_delta": <integer, change from last week>,
    "notable_changes": ["<change 1>", "<change 2>"]
  }}
}}

Rules:
- top_developments should have 3-5 items, sorted by urgency
- Use "alert" for threats needing immediate attention, "watch" for trends to monitor, "opportunity" for gaps to exploit
- Be specific — reference actual competitor names, keywords, and numbers from the signals
- If signals are sparse, note gaps but still produce a useful briefing
- pressure_score: 0-30 is calm, 31-60 is moderate, 61-85 is elevated, 86-100 is high alert"""


# ── Signal Aggregation ────────────────────────────────────────────────────────

def fetch_week_signals(client_id: str, days_back: int = 7) -> dict:
    """Pull all signals from the past week and organize by type."""
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
        "reddit": [],
        "other": [],
    }

    for signal in signals:
        comp_name = signal.get("competitors", {}).get("name", "Unknown")
        entry = {
            "competitor": comp_name,
            "signal_type": signal["signal_type"],
            "data": signal["data"],
            "collected_at": signal["collected_at"],
        }
        source = signal.get("source", "other")
        if source in ("semrush", "semrush_csv"):
            organized["semrush"].append(entry)
        elif source == "google_news":
            organized["google_news"].append(entry)
        elif source == "reddit":
            organized["reddit"].append(entry)
        else:
            organized["other"].append(entry)

    return organized


def get_last_week_score(client_id: str) -> int | None:
    """Get last week's pressure score for delta calculation."""
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


# ── Main Synthesis Flow ───────────────────────────────────────────────────────

def synthesize_for_client(client_slug: str):
    """Generate weekly briefing for a client."""
    print(f"[synthesizer] Starting synthesis for: {client_slug}")

    # Get client info
    result = supabase.table("clients").select("id, name, config").eq("slug", client_slug).single().execute()
    if not result.data:
        print(f"[synthesizer] ERROR: Client '{client_slug}' not found")
        return

    client_id = result.data["id"]
    client_name = result.data["name"]
    week_of = date.today().isoformat()

    # Check if briefing already exists for this week
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

    # Fetch signals
    signals = fetch_week_signals(client_id)
    total_signals = sum(len(v) for v in signals.values())
    print(f"[synthesizer] Found {total_signals} signals across all sources")

    if total_signals == 0:
        print(f"[synthesizer] WARNING: No signals found for {client_slug}, generating placeholder briefing")

    # Build prompt and call Claude
    prompt = build_analysis_prompt(client_name, signals, week_of)

    print(f"[synthesizer] Calling Claude API...")
    message = anthropic.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    raw_response = message.content[0].text

    # Parse JSON response
    try:
        # Strip any markdown code fences if present
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

    # Inject last week delta
    last_score = get_last_week_score(client_id)
    if last_score is not None:
        briefing_data["week_over_week_changes"]["pressure_score_delta"] = (
            briefing_data.get("pressure_score", 50) - last_score
        )

    # Save to Supabase
    supabase.table("briefings").insert({
        "client_id": client_id,
        "week_of": week_of,
        "pressure_score": briefing_data.get("pressure_score", 50),
        "summary": briefing_data.get("executive_summary", ""),
        "developments": briefing_data.get("top_developments", []),
        "full_report": json.dumps(briefing_data),
        "created_at": datetime.utcnow().isoformat(),
    }).execute()

    print(f"[synthesizer] ✓ Briefing saved for {client_slug} — Pressure score: {briefing_data.get('pressure_score')}")
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
