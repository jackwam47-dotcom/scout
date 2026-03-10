"""
Scout — Dashboard Builder
Pulls latest briefing from Supabase and injects real data
into the Scout HTML dashboard template.

Outputs: index.html (deployed to Netlify via GitHub Actions)
"""

import os
import json
import re
from datetime import date
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
CLIENT_SLUG = os.environ.get("SCOUT_CLIENT_SLUG", "apex-nutrition")
TEMPLATE_PATH = os.environ.get("TEMPLATE_PATH", "templates/index.html")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "index.html")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_latest_briefing(client_slug: str) -> dict | None:
    """Fetch the most recent briefing for a client."""
    result = supabase.table("clients").select("id, name").eq("slug", client_slug).single().execute()
    if not result.data:
        print(f"ERROR: Client '{client_slug}' not found")
        return None

    client_id = result.data["id"]

    briefing = (
        supabase.table("briefings")
        .select("*")
        .eq("client_id", client_id)
        .order("week_of", desc=True)
        .limit(1)
        .execute()
    )

    if not briefing.data:
        print(f"WARNING: No briefings found for '{client_slug}'")
        return None

    b = briefing.data[0]
    full_report = json.loads(b["full_report"]) if b.get("full_report") else {}
    full_report["week_of"] = b["week_of"]
    full_report["pressure_score"] = b["pressure_score"]
    full_report["client_name"] = result.data["name"]
    return full_report


def get_historical_scores(client_id: str, weeks: int = 12) -> list[dict]:
    """Get pressure score history for trend chart."""
    result = (
        supabase.table("briefings")
        .select("week_of, pressure_score")
        .eq("client_id", client_id)
        .order("week_of", desc=True)
        .limit(weeks)
        .execute()
    )
    return list(reversed(result.data or []))


def inject_data_into_template(template_html: str, briefing: dict, history: list) -> str:
    """
    Replace the SCOUT_DATA placeholder in the template with real data.
    The template should contain a JS block like:
    // SCOUT_DATA_START
    const scoutData = { ... demo data ... };
    // SCOUT_DATA_END
    """
    # Build the data payload
    scout_data = {
        "meta": {
            "clientName": briefing.get("client_name", ""),
            "weekOf": briefing.get("week_of", date.today().isoformat()),
            "generatedAt": date.today().isoformat(),
        },
        "pressureScore": briefing.get("pressure_score", 50),
        "pressureComponents": briefing.get("pressure_components", {
            "organic_search": 50,
            "paid_search": 50,
            "content_velocity": 50,
            "social_buzz": 50,
        }),
        "executiveSummary": briefing.get("executive_summary", ""),
        "topDevelopments": briefing.get("top_developments", []),
        "keywordMovements": briefing.get("keyword_movements", []),
        "contentSignals": briefing.get("content_signals", []),
        "redditIntelligence": briefing.get("reddit_intelligence", []),
        "weekOverWeek": briefing.get("week_over_week_changes", {}),
        "pressureHistory": [
            {"date": h["week_of"], "score": h["pressure_score"]}
            for h in history
        ],
    }

    data_js = f"const scoutData = {json.dumps(scout_data, indent=2)};"

    # Replace the data block in the template
    pattern = r"// SCOUT_DATA_START.*?// SCOUT_DATA_END"
    replacement = f"// SCOUT_DATA_START\n        {data_js}\n        // SCOUT_DATA_END"

    updated = re.sub(pattern, replacement, template_html, flags=re.DOTALL)

    if updated == template_html:
        print("WARNING: SCOUT_DATA placeholder not found in template — data not injected")
        print("Make sure your template has // SCOUT_DATA_START and // SCOUT_DATA_END comments")

    return updated


def build_dashboard(client_slug: str):
    """Main build function — fetch data and generate output HTML."""
    print(f"[builder] Building dashboard for: {client_slug}")

    briefing = get_latest_briefing(client_slug)
    if not briefing:
        print("[builder] ERROR: No briefing data available")
        return False

    # Get client ID for history
    result = supabase.table("clients").select("id").eq("slug", client_slug).single().execute()
    client_id = result.data["id"]
    history = get_historical_scores(client_id)

    # Read template
    if not os.path.exists(TEMPLATE_PATH):
        print(f"[builder] ERROR: Template not found at {TEMPLATE_PATH}")
        return False

    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        template_html = f.read()

    # Inject data
    output_html = inject_data_into_template(template_html, briefing, history)

    # Write output
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(output_html)

    print(f"[builder] ✓ Dashboard written to {OUTPUT_PATH}")
    print(f"[builder]   Week of: {briefing.get('week_of')}")
    print(f"[builder]   Pressure score: {briefing.get('pressure_score')}")
    print(f"[builder]   Developments: {len(briefing.get('top_developments', []))}")
    return True


if __name__ == "__main__":
    import sys
    slug = sys.argv[1] if len(sys.argv) > 1 else CLIENT_SLUG
    success = build_dashboard(slug)
    sys.exit(0 if success else 1)
