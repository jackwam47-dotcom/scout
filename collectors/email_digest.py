"""
Scout — Weekly Email Digest
Pulls the latest briefing from Supabase and sends a formatted
HTML email to account leads via Resend.com.

Requires:
  RESEND_API_KEY — from resend.com
  SCOUT_DIGEST_RECIPIENTS — comma-separated email addresses
"""

import os
import json
import requests
from datetime import datetime, date, timedelta
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY")
RECIPIENTS = os.environ.get("SCOUT_DIGEST_RECIPIENTS", "").split(",")
FROM_EMAIL = os.environ.get("SCOUT_FROM_EMAIL", "scout@parallelpath.com")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_latest_briefing(client_slug: str) -> dict | None:
    result = supabase.table("clients").select("id, name").eq("slug", client_slug).single().execute()
    if not result.data:
        return None
    client_id = result.data["id"]
    client_name = result.data["name"]

    briefing = (
        supabase.table("briefings")
        .select("*")
        .eq("client_id", client_id)
        .order("week_of", desc=True)
        .limit(1)
        .single()
        .execute()
    )
    if not briefing.data:
        return None

    full = briefing.data.get("full_report")
    if isinstance(full, str):
        full = json.loads(full)

    return {
        "client_name": client_name,
        "week_of": briefing.data["week_of"],
        "pressure_score": briefing.data["pressure_score"],
        "summary": briefing.data["summary"],
        "developments": full.get("top_developments", []),
        "keyword_movements": full.get("keyword_movements", []),
        "content_signals": full.get("content_signals", []),
        "week_over_week": full.get("week_over_week_changes", {}),
        "pressure_components": full.get("pressure_components", {}),
    }


def score_color(score: int) -> str:
    if score >= 86:
        return "#E1885E"  # coral — high alert
    if score >= 61:
        return "#DCBE4C"  # mustard — elevated
    if score >= 31:
        return "#8CBEC6"  # bubble — moderate
    return "#C6EB4C"  # spring — calm


def urgency_badge(urgency: str) -> str:
    colors = {
        "immediate": ("#E1885E", "#fff8f5"),
        "this_week": ("#DCBE4C", "#fdf9ee"),
        "this_month": ("#8CBEC6", "#f0f7f8"),
    }
    color, bg = colors.get(urgency, ("#8CBEC6", "#f0f7f8"))
    label = urgency.replace("_", " ").title()
    return f'<span style="background:{bg};color:{color};border:1px solid {color};padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;">{label}</span>'


def type_badge(type_: str) -> tuple[str, str]:
    icons = {
        "alert": ("🔴", "#E1885E"),
        "watch": ("🟡", "#DCBE4C"),
        "opportunity": ("🟢", "#4CAF50"),
    }
    return icons.get(type_, ("⚪", "#888"))


def build_html(data: dict) -> str:
    score = data["pressure_score"]
    color = score_color(score)
    week = data["week_of"]
    client = data["client_name"]
    delta = data["week_over_week"].get("pressure_score_delta", 0)
    delta_str = f"↑ +{delta}" if delta > 0 else f"↓ {delta}" if delta < 0 else "→ No change"
    delta_color = "#E1885E" if delta > 0 else "#4CAF50" if delta < 0 else "#888"
    pc = data["pressure_components"]

    # Developments
    devs_html = ""
    for dev in data["developments"][:5]:
        icon, icon_color = type_badge(dev.get("type", "watch"))
        badge = urgency_badge(dev.get("urgency", "this_week"))
        devs_html += f"""
        <tr>
          <td style="padding:18px 0;border-bottom:1px solid #eee;">
            <div style="display:flex;align-items:flex-start;gap:12px;">
              <span style="font-size:18px;line-height:1;margin-top:2px;">{icon}</span>
              <div style="flex:1;">
                <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;flex-wrap:wrap;">
                  <span style="font-family:sans-serif;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.08em;color:#888;">{dev.get('competitor','')}</span>
                  {badge}
                </div>
                <div style="font-family:sans-serif;font-size:15px;font-weight:600;color:#1a1a1a;margin-bottom:6px;line-height:1.4;">{dev.get('headline','')}</div>
                <div style="font-family:Georgia,serif;font-size:13px;color:#555;line-height:1.65;margin-bottom:8px;">{dev.get('detail','')}</div>
                <div style="background:#f7f9f7;border-left:3px solid #4CAF50;padding:10px 14px;border-radius:0 6px 6px 0;">
                  <span style="font-family:sans-serif;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;color:#4CAF50;display:block;margin-bottom:4px;">Recommended Action</span>
                  <span style="font-family:Georgia,serif;font-size:13px;color:#333;line-height:1.5;">{dev.get('recommended_action','')}</span>
                </div>
              </div>
            </div>
          </td>
        </tr>"""

    # Component scores
    comps = [
        ("Organic", pc.get("organic_search", 0)),
        ("Paid", pc.get("paid_search", 0)),
        ("Content", pc.get("content_velocity", 0)),
        ("Social", pc.get("social_buzz", 0)),
    ]
    comps_html = "".join([
        f'<td style="text-align:center;padding:0 12px;">'
        f'<div style="font-family:sans-serif;font-size:22px;font-weight:700;color:{score_color(v)}">{v}</div>'
        f'<div style="font-family:sans-serif;font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.1em;color:#999;margin-top:3px;">{k}</div>'
        f'</td>'
        for k, v in comps
    ])

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:sans-serif;">

<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:32px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">

  <!-- Header -->
  <tr>
    <td style="background:#1d3341;border-radius:12px 12px 0 0;padding:28px 36px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td>
            <span style="font-family:sans-serif;font-size:22px;font-weight:700;color:#F6EDE6;letter-spacing:-0.3px;">Scout</span>
            <span style="font-family:sans-serif;font-size:11px;font-weight:600;color:#8CBEC6;text-transform:uppercase;letter-spacing:0.12em;margin-left:10px;">by Parallel Path</span>
          </td>
          <td align="right">
            <span style="font-family:sans-serif;font-size:11px;color:#8fb0b5;text-transform:uppercase;letter-spacing:0.08em;">Week of {week}</span>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Pressure Score Hero -->
  <tr>
    <td style="background:#3C6E71;padding:32px 36px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td style="width:140px;text-align:center;padding-right:24px;border-right:1px solid rgba(246,237,230,0.2);">
            <div style="font-family:sans-serif;font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.14em;color:rgba(246,237,230,0.55);margin-bottom:6px;">Competitive Pressure</div>
            <div style="font-family:sans-serif;font-size:72px;font-weight:700;color:{color};line-height:1;letter-spacing:-2px;">{score}</div>
            <div style="font-family:sans-serif;font-size:11px;font-weight:700;color:{delta_color};text-transform:uppercase;letter-spacing:0.07em;margin-top:6px;">{delta_str}</div>
          </td>
          <td style="padding-left:24px;">
            <div style="font-family:sans-serif;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.1em;color:rgba(246,237,230,0.55);margin-bottom:8px;">{client}</div>
            <div style="font-family:Georgia,serif;font-size:14px;color:rgba(246,237,230,0.85);line-height:1.65;">{data['summary']}</div>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Component Scores -->
  <tr>
    <td style="background:#243f50;padding:18px 36px;border-bottom:1px solid rgba(246,237,230,0.08);">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>{comps_html}</tr>
      </table>
    </td>
  </tr>

  <!-- Body -->
  <tr>
    <td style="background:#fff;padding:32px 36px;">

      <h2 style="font-family:sans-serif;font-size:18px;font-weight:600;color:#1a1a1a;margin:0 0 4px;">Top Developments This Week</h2>
      <p style="font-family:sans-serif;font-size:12px;color:#999;text-transform:uppercase;letter-spacing:0.08em;margin:0 0 20px;">AI-synthesized · Scout · {week}</p>

      <table width="100%" cellpadding="0" cellspacing="0">
        {devs_html}
      </table>

    </td>
  </tr>

  <!-- CTA -->
  <tr>
    <td style="background:#fff;padding:0 36px 32px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td style="background:#f7f9fc;border-radius:10px;padding:20px 24px;text-align:center;">
            <p style="font-family:sans-serif;font-size:13px;color:#555;margin:0 0 14px;">View the full interactive briefing including keyword movements, web changes, and demand trends.</p>
            <a href="https://jackwam47-dotcom.github.io/scout/" style="display:inline-block;background:#3C6E71;color:#F6EDE6;font-family:sans-serif;font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:0.08em;padding:12px 28px;border-radius:6px;text-decoration:none;">Open Scout Dashboard</a>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Footer -->
  <tr>
    <td style="background:#1d3341;border-radius:0 0 12px 12px;padding:20px 36px;text-align:center;">
      <p style="font-family:sans-serif;font-size:11px;color:#8fb0b5;margin:0;">Scout by Parallel Path · Competitive Intelligence · This digest is for internal account team use only.</p>
    </td>
  </tr>

</table>
</td></tr>
</table>

</body>
</html>"""


def send_digest(client_slug: str):
    print(f"[email] Starting digest for: {client_slug}")

    if not RESEND_API_KEY:
        print("[email] ERROR: RESEND_API_KEY not set")
        return

    recipients = [r.strip() for r in RECIPIENTS if r.strip()]
    if not recipients:
        print("[email] ERROR: SCOUT_DIGEST_RECIPIENTS not set")
        return

    data = get_latest_briefing(client_slug)
    if not data:
        print(f"[email] ERROR: No briefing found for {client_slug}")
        return

    html = build_html(data)
    subject = f"Scout: {data['client_name']} — Competitive Pressure {data['pressure_score']}/100 · Week of {data['week_of']}"

    resp = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": FROM_EMAIL,
            "to": recipients,
            "subject": subject,
            "html": html,
        },
    )

    if resp.status_code == 200:
        print(f"[email] Sent to {recipients} — subject: {subject}")
    else:
        print(f"[email] ERROR {resp.status_code}: {resp.text}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python email_digest.py <client_slug>")
        sys.exit(1)
    send_digest(sys.argv[1])
