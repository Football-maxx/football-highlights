#!/usr/bin/env python3
"""
Automated Football Highlight Video System — v2 (fully rewritten)
─────────────────────────────────────────────────────────────────
• Fetches finished matches (Premier League, La Liga, Bundesliga,
  Champions League) via football-data.org
• Builds a 1280×720 thumbnail:
    – LEFT  1/3  (426 px): dark panel, home logo (top), score, away logo (bottom)
    – RIGHT 2/3  (854 px): live action photo of a key player from that match
      (searched via Google Custom Search JSON API; falls back to
       generic_stadium.jpg if nothing found)
• Generates an AI-quality narration script (GPT-4o when available,
  otherwise a rich hand-crafted template)
• Converts script → speech (VoiceRSS, British-English)
• Builds final video: competition intro + highlight clip + narration audio
• Uploads to YouTube with per-competition tags, rich description,
  priority ordering, and custom thumbnail
• Records uploaded match IDs in Supabase to avoid duplicates
• Every step is fault-tolerant — one match failure never kills the run
"""

import os, sys, re, json, time, io, random, textwrap
import requests
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus

from moviepy import VideoFileClip, AudioFileClip, ImageClip, concatenate_videoclips
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ── optional imports ──────────────────────────────────────────────────────────
try:
    from supabase import create_client, Client as SupabaseClient
    _SUPABASE_OK = True
except ImportError:
    _SUPABASE_OK = False

try:
    import openai as _openai_module
    _OPENAI_AVAILABLE = True
except ImportError:
    _OPENAI_AVAILABLE = False

try:
    from gnews import GNews
    _GNEWS_OK = True
except ImportError:
    _GNEWS_OK = False

try:
    from bs4 import BeautifulSoup
    _BS4_OK = True
except ImportError:
    _BS4_OK = False

# ── env vars ──────────────────────────────────────────────────────────────────
FOOTBALL_API_KEY      = os.environ.get("FOOTBALL_API_KEY", "")
VOICERSS_API_KEY      = os.environ.get("VOICERSS_API_KEY", "")
SUPABASE_URL          = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY     = os.environ.get("SUPABASE_ANON_KEY", "")
YOUTUBE_TOKEN_JSON    = os.environ.get("YOUTUBE_TOKEN_JSON", "")
YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID", "")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET", "")
OPENAI_API_KEY        = os.environ.get("OPENAI_API_KEY", "")
GOOGLE_CSE_API_KEY    = os.environ.get("GOOGLE_CSE_API_KEY", "")   # for player images
GOOGLE_CSE_CX         = os.environ.get("GOOGLE_CSE_CX", "")        # Custom Search Engine ID
DRY_RUN               = os.environ.get("DRY_RUN", "false").lower() == "true"

# ── lazy singletons (created once, used everywhere) ───────────────────────────
_supabase  = None
_openai_cl = None

def get_supabase():
    global _supabase
    if _supabase is None and _SUPABASE_OK and SUPABASE_URL and SUPABASE_ANON_KEY:
        _supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    return _supabase

def get_openai():
    global _openai_cl
    if _openai_cl is None and _OPENAI_AVAILABLE and OPENAI_API_KEY:
        _openai_cl = _openai_module.OpenAI(api_key=OPENAI_API_KEY)
    return _openai_cl

# ── competition metadata ──────────────────────────────────────────────────────
COMPETITIONS = [
    {"id": "PL",  "name": "Premier League",    "country": "England"},
    {"id": "PD",  "name": "La Liga",            "country": "Spain"},
    {"id": "BL1", "name": "Bundesliga",         "country": "Germany"},
    {"id": "CL",  "name": "Champions League",   "country": "Europe"},
]

COMPETITION_INTROS = {
    "PL":  "assets/intros/premier_league.mp4",
    "PD":  "assets/intros/laliga.mp4",
    "BL1": "assets/intros/bundesliga.mp4",
    "CL":  "assets/intros/champions_league.mp4",
}

COMPETITION_TAGS = {
    "PL":  ["Premier League", "EPL", "English Football", "BPL",
            "Premier League Highlights", "English Premier League"],
    "PD":  ["La Liga", "LaLiga", "Spanish Football", "BBVA",
            "La Liga Highlights", "Spain Football"],
    "BL1": ["Bundesliga", "German Football", "DFL",
            "Bundesliga Highlights", "Germany Football"],
    "CL":  ["Champions League", "UCL", "UEFA Champions League",
            "European Football", "Champions League Highlights"],
}

COMPETITION_EMOJIS = {
    "PL": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "PD": "🇪🇸", "BL1": "🇩🇪", "CL": "🏆",
}

# Clubs ordered by global popularity / upload priority
PRIORITY_CLUBS = [
    "Manchester City", "Manchester United", "Arsenal", "Liverpool",
    "Chelsea", "Tottenham Hotspur", "Tottenham",
    "Real Madrid", "Barcelona", "Atletico Madrid",
    "Bayern Munich", "Borussia Dortmund", "RB Leipzig",
    "Paris Saint-Germain", "PSG",
    "AC Milan", "Inter Milan", "Juventus",
    "Aston Villa", "Newcastle United", "Newcastle",
    "Bayer Leverkusen", "Eintracht Frankfurt",
    "PSV", "Ajax", "Feyenoord",
    "Celtic", "Rangers",
    "Marseille", "Lyon",
    "Porto", "Benfica", "Sporting CP",
]

# ── logging ───────────────────────────────────────────────────────────────────
DEBUG_LOG = "debug.log"
with open(DEBUG_LOG, "w") as _f:
    _f.write("Debug log started\n")

def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line); sys.stdout.flush()
    with open(DEBUG_LOG, "a") as f:
        f.write(line + "\n"); f.flush()

def sanitize(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|/\s]', '_', name).strip()

# ── Supabase helpers ──────────────────────────────────────────────────────────
def get_posted_ids() -> set:
    sb = get_supabase()
    if not sb:
        return set()
    try:
        rows = sb.table("highlights_matches").select("fixture_id").eq("posted", 1).execute()
        return {r["fixture_id"] for r in rows.data}
    except Exception as e:
        log(f"Supabase read error: {e}")
        return set()

def upsert_match(row: dict):
    sb = get_supabase()
    if not sb:
        return
    try:
        sb.table("highlights_matches").upsert(row, on_conflict="fixture_id").execute()
    except Exception as e:
        log(f"Supabase upsert error: {e}")

def mark_posted(fixture_id):
    sb = get_supabase()
    if not sb:
        return
    try:
        sb.table("highlights_matches").update({"posted": 1}).eq("fixture_id", fixture_id).execute()
    except Exception as e:
        log(f"Supabase update error: {e}")

# ── fetch matches ─────────────────────────────────────────────────────────────
def fetch_finished_matches() -> list:
    """
    Returns a list of finished matches sorted by club priority.
    Each entry: (fixture_id, home, away, home_score, away_score, comp_id, comp_name, match_date)
    """
    headers   = {"X-Auth-Token": FOOTBALL_API_KEY}
    posted    = get_posted_ids()
    cutoff    = datetime.now(timezone.utc) - timedelta(hours=24)
    results   = []

    for comp in COMPETITIONS:
        cid, cname = comp["id"], comp["name"]
        log(f"Fetching {cname} ({cid}) …")
        url = f"https://api.football-data.org/v4/competitions/{cid}/matches"
        try:
            r = requests.get(url, headers=headers, timeout=15)
            r.raise_for_status()
            data = r.json()
            time.sleep(6)          # respect free-tier rate limit
        except requests.HTTPError as e:
            code = e.response.status_code if e.response else "?"
            log(f"  {cname}: HTTP {code} — skipping")
            continue
        except Exception as e:
            log(f"  {cname}: {e} — skipping")
            continue

        for m in data.get("matches", []):
            if m["status"] != "FINISHED":
                continue
            try:
                mdate = datetime.fromisoformat(m["utcDate"].replace("Z", "+00:00"))
            except Exception:
                mdate = datetime.now(timezone.utc)
            if mdate < cutoff:
                continue

            fid  = m["id"]
            home = m["homeTeam"]["name"]
            away = m["awayTeam"]["name"]
            hs   = m["score"]["fullTime"]["home"] or 0
            as_  = m["score"]["fullTime"]["away"] or 0

            if fid in posted:
                log(f"  Already posted: {home} vs {away}")
                continue

            # Filter: at least one priority club OR Champions League
            if cid != "CL" and home not in PRIORITY_CLUBS and away not in PRIORITY_CLUBS:
                log(f"  Skipping low-priority: {home} vs {away}")
                continue

            upsert_match({
                "fixture_id": fid, "home_team": home, "away_team": away,
                "match_date": m["utcDate"], "status": "FINISHED",
                "home_score": hs, "away_score": as_,
                "competition": cid, "posted": 0,
            })
            log(f"  ✅ Queued: {home} {hs}–{as_} {away}")
            results.append((fid, home, away, hs, as_, cid, cname, mdate))

    # Sort by priority: match that has the highest-ranked club goes first
    def priority_score(entry):
        _, home, away, *_ = entry
        h = PRIORITY_CLUBS.index(home) if home in PRIORITY_CLUBS else 999
        a = PRIORITY_CLUBS.index(away) if away in PRIORITY_CLUBS else 999
        return min(h, a)

    results.sort(key=priority_score)
    log(f"Total matches to process: {len(results)}")
    return results

# ── goal data ─────────────────────────────────────────────────────────────────
def get_match_goals(fixture_id) -> list:
    url = f"https://api.football-data.org/v4/matches/{fixture_id}"
    try:
        r = requests.get(url, headers={"X-Auth-Token": FOOTBALL_API_KEY}, timeout=10)
        data = r.json()
        goals = []
        for g in data.get("goals", []):
            scorer = g.get("scorer", {}).get("name", "Unknown")
            minute = g.get("minute", "?")
            team   = g.get("team", {}).get("name", "")
            goals.append({"player": scorer, "minute": minute, "team": team})
        return goals
    except Exception as e:
        log(f"Goals fetch failed: {e}")
        return []

# ── player image for thumbnail ────────────────────────────────────────────────
def fetch_player_image(home: str, away: str, goals: list) -> Image.Image | None:
    """
    Try to get a compelling in-match action photo:
    1. Google Custom Search Image API (needs GOOGLE_CSE_API_KEY + GOOGLE_CSE_CX)
    2. Fallback: assets/generic_stadium.jpg
    3. Fallback: plain dark gradient
    """
    # Pick the most famous goal-scorer name for the query
    scorer_name = None
    for g in goals:
        for club in PRIORITY_CLUBS:
            if g["team"] and club in g["team"]:
                scorer_name = g["player"]
                break
        if scorer_name:
            break
    if not scorer_name and goals:
        scorer_name = goals[0]["player"]

    query_parts = [home, "vs", away, "match action 2024 2025"]
    if scorer_name:
        query_parts = [scorer_name, home, "vs", away, "goal celebration"]
    query = " ".join(query_parts)

    # ── Google CSE ────────────────────────────────────────────────────────────
    if GOOGLE_CSE_API_KEY and GOOGLE_CSE_CX:
        try:
            cse_url = (
                "https://www.googleapis.com/customsearch/v1"
                f"?key={GOOGLE_CSE_API_KEY}"
                f"&cx={GOOGLE_CSE_CX}"
                f"&q={quote_plus(query)}"
                "&searchType=image"
                "&imgSize=LARGE"
                "&imgType=photo"
                "&num=5"
                "&safe=active"
            )
            r = requests.get(cse_url, timeout=10)
            r.raise_for_status()
            items = r.json().get("items", [])
            for item in items:
                img_url = item.get("link", "")
                try:
                    ir = requests.get(img_url, timeout=10,
                                      headers={"User-Agent": "Mozilla/5.0"})
                    ir.raise_for_status()
                    img = Image.open(io.BytesIO(ir.content)).convert("RGB")
                    log(f"Player image fetched via CSE: {img_url[:80]}")
                    return img
                except Exception:
                    continue
        except Exception as e:
            log(f"CSE image search failed: {e}")

    # ── generic stadium fallback ──────────────────────────────────────────────
    for path in ["assets/generic_stadium.jpg", "assets/placeholder_thumbnail.jpg"]:
        if os.path.exists(path):
            try:
                return Image.open(path).convert("RGB")
            except Exception:
                pass
    return None

# ── thumbnail ─────────────────────────────────────────────────────────────────
def _load_logo(team: str, size: int = 180) -> Image.Image | None:
    """Try several filename variations for the team logo."""
    candidates = [
        f"logos/{team}.png",
        f"logos/{team}.jpg",
        f"assets/logos/{team}.png",
        f"assets/logos/{team}.jpg",
        f"logos/{sanitize(team)}.png",
        f"logos/{sanitize(team)}.jpg",
    ]
    for p in candidates:
        if os.path.exists(p):
            try:
                img = Image.open(p).convert("RGBA")
                img.thumbnail((size, size), Image.LANCZOS)
                return img
            except Exception:
                pass
    return None

def _best_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Try several common font locations; fall back to PIL default."""
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "arial.ttf",
        "Arial.ttf",
    ]
    for c in candidates:
        try:
            return ImageFont.truetype(c, size)
        except Exception:
            pass
    return ImageFont.load_default()

def generate_thumbnail(
    home: str, away: str,
    home_score: int, away_score: int,
    goals: list,
    comp_id: str,
    matchday: str = "MATCHDAY",
) -> str | None:
    """
    1280 × 720 thumbnail
    ┌──────────────────┬───────────────────────────────────────────────────────┐
    │  LEFT 1/3 (426)  │              RIGHT 2/3 (854)                          │
    │  dark gradient   │  action photo of key player (or stadium)              │
    │  [home logo]     │                                                       │
    │  [score]         │  subtle "HIGHLIGHTS" watermark top-right              │
    │  [away logo]     │  matchday badge bottom-right                          │
    └──────────────────┴───────────────────────────────────────────────────────┘
    """
    W, H = 1280, 720
    LEFT  = 426          # 1/3
    RIGHT = W - LEFT     # 2/3 = 854

    thumb = Image.new("RGB", (W, H), (10, 10, 20))
    draw  = ImageDraw.Draw(thumb)

    # ── RIGHT panel: player / stadium photo ──────────────────────────────────
    player_img = fetch_player_image(home, away, goals)
    if player_img:
        # crop to fill right panel
        pw, ph = player_img.size
        scale  = max(RIGHT / pw, H / ph)
        nw, nh = int(pw * scale), int(ph * scale)
        player_img = player_img.resize((nw, nh), Image.LANCZOS)
        x_off = (nw - RIGHT) // 2
        y_off = (nh - H) // 2
        player_img = player_img.crop((x_off, y_off, x_off + RIGHT, y_off + H))
        # slight brightness/contrast boost for "pop"
        player_img = ImageEnhance.Contrast(player_img).enhance(1.15)
        player_img = ImageEnhance.Brightness(player_img).enhance(1.05)
        thumb.paste(player_img, (LEFT, 0))
    else:
        # gradient fallback
        for i in range(RIGHT):
            c = int(20 + i * 60 / RIGHT)
            draw.line([(LEFT + i, 0), (LEFT + i, H)], fill=(c, c + 10, c + 30))

    # ── LEFT panel: gradient background ──────────────────────────────────────
    comp_colors = {
        "PL":  [(114, 0, 255), (50, 0, 100)],
        "PD":  [(200, 30, 30),  (80, 0, 0)],
        "BL1": [(220, 30, 0),   (100, 10, 0)],
        "CL":  [(0, 80, 200),   (0, 20, 80)],
    }
    top_col, bot_col = comp_colors.get(comp_id, [(40, 40, 80), (10, 10, 30)])
    for y in range(H):
        t = y / H
        r = int(top_col[0] + t * (bot_col[0] - top_col[0]))
        g = int(top_col[1] + t * (bot_col[1] - top_col[1]))
        b = int(top_col[2] + t * (bot_col[2] - top_col[2]))
        draw.line([(0, y), (LEFT - 1, y)], fill=(r, g, b))

    # thin glowing separator line
    for y in range(H):
        t  = y / H
        br = int(100 + 155 * abs(0.5 - t) * 2)
        draw.line([(LEFT - 2, y), (LEFT + 2, y)], fill=(br, br, 255))

    # ── logos ─────────────────────────────────────────────────────────────────
    logo_size = 160
    home_logo = _load_logo(home, logo_size)
    away_logo = _load_logo(away, logo_size)

    home_logo_y = 60
    away_logo_y = H - logo_size - 60

    if home_logo:
        x = (LEFT - home_logo.width) // 2
        thumb.paste(home_logo, (x, home_logo_y), home_logo)
    else:
        # text fallback
        fn = _best_font(32)
        draw.text((10, home_logo_y + 60), home[:18], fill="white", font=fn)

    if away_logo:
        x = (LEFT - away_logo.width) // 2
        thumb.paste(away_logo, (x, away_logo_y), away_logo)
    else:
        fn = _best_font(32)
        draw.text((10, away_logo_y + 60), away[:18], fill="white", font=fn)

    # ── score ─────────────────────────────────────────────────────────────────
    score_text = f"{home_score}  –  {away_score}"
    score_font = _best_font(96)
    bb   = draw.textbbox((0, 0), score_text, font=score_font)
    sw   = bb[2] - bb[0]
    sx   = (LEFT - sw) // 2
    sy   = H // 2 - (bb[3] - bb[1]) // 2
    # glow
    for offset in range(4, 0, -1):
        draw.text((sx - offset, sy), score_text, fill=(255, 220, 0, 80), font=score_font)
        draw.text((sx + offset, sy), score_text, fill=(255, 220, 0, 80), font=score_font)
    draw.text((sx, sy), score_text, fill=(255, 220, 50),
              font=score_font, stroke_width=4, stroke_fill=(0, 0, 0))

    # small team name labels under/above logos
    lbl_font = _best_font(22)
    if home_logo:
        bb2 = draw.textbbox((0, 0), home, font=lbl_font)
        lx  = max(4, (LEFT - (bb2[2] - bb2[0])) // 2)
        draw.text((lx, home_logo_y + logo_size + 5), home, fill=(220, 220, 220),
                  font=lbl_font, stroke_width=1, stroke_fill="black")
    if away_logo:
        bb2 = draw.textbbox((0, 0), away, font=lbl_font)
        lx  = max(4, (LEFT - (bb2[2] - bb2[0])) // 2)
        draw.text((lx, away_logo_y - 28), away, fill=(220, 220, 220),
                  font=lbl_font, stroke_width=1, stroke_fill="black")

    # ── RIGHT overlay text ────────────────────────────────────────────────────
    hl_font = _best_font(68)
    hl_text = "HIGHLIGHTS"
    bb3 = draw.textbbox((0, 0), hl_text, font=hl_font)
    hx  = LEFT + (RIGHT - (bb3[2] - bb3[0])) // 2
    draw.text((hx, 28), hl_text, fill="white",
              font=hl_font, stroke_width=3, stroke_fill="black")

    # matchday badge (bottom right)
    md_font = _best_font(36)
    md_text = matchday.upper()
    bb4  = draw.textbbox((0, 0), md_text, font=md_font)
    mdw  = bb4[2] - bb4[0]
    mdh  = bb4[3] - bb4[1]
    mx   = W - mdw - 20
    my   = H - mdh - 20
    draw.rectangle([mx - 8, my - 6, mx + mdw + 8, my + mdh + 6],
                   fill=(0, 0, 0, 160))
    draw.text((mx, my), md_text, fill=(255, 215, 0), font=md_font,
              stroke_width=1, stroke_fill="black")

    # competition watermark bottom-left of right panel
    emoji = COMPETITION_EMOJIS.get(comp_id, "⚽")
    wm_font = _best_font(28)
    comp_info = COMPETITIONS
    cname = next((c["name"] for c in comp_info if c["id"] == comp_id), comp_id)
    wm_text = f"{cname}"
    draw.text((LEFT + 12, H - 46), wm_text, fill=(200, 200, 200, 200),
              font=wm_font, stroke_width=1, stroke_fill="black")

    out_path = f"thumbnail_{sanitize(home)}_vs_{sanitize(away)}.jpg"
    thumb.save(out_path, "JPEG", quality=95)
    log(f"Thumbnail saved: {out_path}")
    return out_path

# ── narration script ──────────────────────────────────────────────────────────
def _result_phrase(home: str, away: str, hs: int, as_: int) -> str:
    if hs > as_:
        return f"{home} claimed a {hs}–{as_} victory over {away}"
    elif as_ > hs:
        return f"{away} edged out {home} with a {as_}–{hs} win"
    else:
        return f"{home} and {away} played out a {hs}–{as_} draw"

def _goal_sentences(goals: list, home: str, away: str) -> str:
    if not goals:
        return ""
    lines = []
    for g in goals:
        min_ = g.get("minute", "?")
        pl   = g.get("player", "Unknown")
        team = g.get("team", "")
        lines.append(f"{pl} for {team} in the {min_}th minute")
    if len(lines) == 1:
        return f"The only goal came from {lines[0]}. "
    joined = "; ".join(lines[:-1]) + f"; and {lines[-1]}"
    return f"The goals came from: {joined}. "

def generate_audio_script(home: str, away: str, hs: int, as_: int,
                           goals: list, comp_id: str) -> str:
    cname = next((c["name"] for c in COMPETITIONS if c["id"] == comp_id), "Football")

    # ── GPT-4o path ───────────────────────────────────────────────────────────
    oai = get_openai()
    if oai:
        scorers_text = _goal_sentences(goals, home, away)
        prompt = (
            f"You are an energetic, professional football TV commentator. "
            f"Write a 60–80 word spoken narration for a YouTube short highlight video. "
            f"Competition: {cname}. "
            f"Match: {home} vs {away}, final score {hs}–{as_}. "
            f"{scorers_text}"
            "Start with an exciting greeting, mention the competition, recap the goals dramatically, "
            "end with a call-to-action to like and subscribe. "
            "Do NOT use markdown, headers, or bullet points. Plain spoken English only."
        )
        try:
            resp = oai.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
                temperature=0.85,
            )
            script = resp.choices[0].message.content.strip()
            log(f"GPT-4o script generated ({len(script)} chars)")
            return script
        except Exception as e:
            log(f"OpenAI script generation failed: {e}")

    # ── high-quality hand-crafted fallback ────────────────────────────────────
    greetings = [
        "What a match that was!",
        "Football fans, this one had it all!",
        "Good day everyone, and welcome to today's highlights!",
        "Hello and welcome — what a game to cover!",
    ]
    result = _result_phrase(home, away, hs, as_)
    goal_text = _goal_sentences(goals, home, away)
    sign_offs = [
        f"That wraps up our {cname} coverage. Don't forget to like, subscribe, and hit the bell icon for more football highlights every single day!",
        f"That's your {cname} update! Smash that subscribe button so you never miss a goal!",
        f"More {cname} action coming your way — subscribe now and be the first to watch!",
    ]
    script = (
        f"{random.choice(greetings)} "
        f"Welcome to your {cname} highlights. "
        f"{result}. "
        f"{goal_text}"
        f"{random.choice(sign_offs)}"
    )
    log(f"Fallback script generated ({len(script)} chars)")
    return script

# ── text-to-speech ────────────────────────────────────────────────────────────
def text_to_speech(text: str, filename: str) -> bool:
    encoded = quote_plus(text)
    url = (
        f"http://api.voicerss.org/"
        f"?key={VOICERSS_API_KEY}"
        f"&hl=en-gb"
        f"&v=Alice"          # British female voice
        f"&r=0"              # normal speed
        f"&c=mp3"
        f"&f=44khz_16bit_stereo"
        f"&src={encoded}"
    )
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        if b"ERROR" in r.content[:50]:
            raise ValueError(r.content[:100].decode())
        with open(filename, "wb") as f:
            f.write(r.content)
        # quick validation
        clip = AudioFileClip(filename)
        dur  = clip.duration
        clip.close()
        log(f"TTS audio saved: {filename} ({dur:.1f}s)")
        return True
    except Exception as e:
        log(f"TTS failed ({e}). Generating silent fallback audio.")
        os.system(
            f'ffmpeg -y -f lavfi -i "anullsrc=r=44100:cl=stereo" '
            f'-t 12 -q:a 9 -acodec libmp3lame "{filename}" > /dev/null 2>&1'
        )
        return False

# ── Scorebat highlights ───────────────────────────────────────────────────────
def get_highlight_url(home: str, away: str) -> str | None:
    try:
        r = requests.get("https://www.scorebat.com/video-api/v1/", timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log(f"Scorebat failed: {e}")
        return None

    for entry in data:
        if "warning" in entry:
            continue
        s1 = entry.get("side1", {}).get("name", "")
        s2 = entry.get("side2", {}).get("name", "")
        title = entry.get("title", "")
        if (home in (title, s1, s2) or any(home in x for x in [title, s1, s2])) and \
           (away in (title, s1, s2) or any(away in x for x in [title, s1, s2])):
            for vid in entry.get("videos", []):
                embed = vid.get("embed", "")
                if _BS4_OK:
                    soup   = BeautifulSoup(embed, "html.parser")
                    iframe = soup.find("iframe")
                    if iframe and iframe.get("src"):
                        log(f"Scorebat hit: {iframe['src'][:80]}")
                        return iframe["src"]
    log(f"No Scorebat highlight for {home} vs {away}")
    return None

def download_video(url: str, path: str) -> bool:
    try:
        r = requests.get(url, stream=True, timeout=30,
                         headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        log(f"Video downloaded: {path}")
        return True
    except Exception as e:
        log(f"Video download failed: {e}")
        return False

# ── video build ───────────────────────────────────────────────────────────────
def build_video(intro_path: str, highlight_path: str | None,
                audio_path: str, output_path: str) -> bool:
    if not os.path.exists(intro_path):
        log(f"Intro not found: {intro_path}")
        return False
    try:
        intro  = VideoFileClip(intro_path)
        clips  = [intro]

        if highlight_path and os.path.exists(highlight_path):
            hl = VideoFileClip(highlight_path)
            if hl.duration > 30:
                hl = hl.subclipped(0, 30)
            # match resolution to intro
            if hl.size != intro.size:
                hl = hl.resized(intro.size)
            clips.append(hl)

        final = concatenate_videoclips(clips, method="compose")

        if os.path.exists(audio_path):
            audio = AudioFileClip(audio_path)
            # loop audio if video is longer, or trim if audio is longer
            if audio.duration < final.duration:
                repeats = int(final.duration // audio.duration) + 1
                from moviepy import concatenate_audioclips
                audio = concatenate_audioclips([audio] * repeats).subclipped(0, final.duration)
            else:
                audio = audio.subclipped(0, final.duration)
            final = final.with_audio(audio)

        final.write_videofile(
            output_path,
            codec="libx264",
            audio_codec="aac",
            preset="fast",
            threads=4,
            logger=None,
        )
        log(f"Video built: {output_path}")
        return True
    except Exception as e:
        log(f"Video build error: {e}")
        return False

# ── YouTube upload ────────────────────────────────────────────────────────────
def _build_youtube_title(home: str, away: str, hs: int, as_: int, cname: str) -> str:
    now  = datetime.now()
    date = now.strftime("%d %b %Y")
    return f"{home} {hs}–{as_} {away} | {cname} Highlights | {date}"[:100]

def _build_youtube_description(home: str, away: str, hs: int, as_: int,
                                goals: list, comp_id: str, cname: str) -> str:
    goal_lines = "\n".join(
        f"  ⚽ {g['player']} ({g['minute']}') — {g['team']}" for g in goals
    ) or "  ⚽ Goal data unavailable"

    comp_emoji = COMPETITION_EMOJIS.get(comp_id, "🏆")
    return textwrap.dedent(f"""
        {comp_emoji} {cname} | {home} vs {away}

        📊 Final Score: {home} {hs} – {as_} {away}

        ⚽ Goals:
        {goal_lines}

        🎬 Watch every key moment: all goals, assists, and match-winning plays
        in this fast-paced professional highlight reel.

        ════════════════════════════════════
        🔔 SUBSCRIBE & turn on notifications — new highlights every match day!
        👍 LIKE if you enjoyed this video
        💬 COMMENT your thoughts — who was man of the match?
        🔁 SHARE with fellow football fans
        ════════════════════════════════════

        #Football #Soccer #{home.replace(' ','')} #{away.replace(' ','')} #{cname.replace(' ','')}
        #Highlights #Goals #MatchHighlights #{comp_id} #FootballHighlights
        #PremierLeague #LaLiga #Bundesliga #ChampionsLeague #UCL
        #GoalOfTheDay #Matchday #FootballFans #SoccerHighlights #Sports
        #SportNews #FootballGoals #WeeklyHighlights #AllGoals
    """).strip()

def _build_tags(home: str, away: str, comp_id: str, goals: list) -> list:
    base = [
        "Football", "Soccer", "Highlights", "Match Highlights",
        "Football Highlights", "Soccer Highlights",
        "Goal", "Goals", "Best Goals", "Goal of the Day",
        "Football Goals", "Soccer Goals",
        home, away, f"{home} vs {away}", f"{home} {away}",
        "Matchday", "Match Recap", "Full Highlights",
        "Sports", "Sports News", "Football News",
        "Football Match", "Soccer Match", "Football Fan",
        "Subscribe", "Viral Football", "Football Moments",
    ]
    base += COMPETITION_TAGS.get(comp_id, [])
    for g in goals:
        if g.get("player"):
            base.append(g["player"])
    # Deduplicate, trim to 500 chars total (YouTube limit)
    seen, out = set(), []
    for t in base:
        if t.lower() not in seen and len(t) < 50:
            seen.add(t.lower())
            out.append(t)
    return out[:75]   # YouTube allows up to 500 chars total in tags

def upload_to_youtube(video_file: str, title: str, description: str,
                       tags: list, thumbnail_path: str) -> bool:
    if DRY_RUN:
        log(f"DRY RUN — would upload: {title}")
        return True
    if not YOUTUBE_TOKEN_JSON:
        log("YouTube token missing — skipping upload.")
        return False

    creds_data = json.loads(YOUTUBE_TOKEN_JSON)
    if "client_id" in creds_data:
        creds = Credentials.from_authorized_user_info(creds_data)
    else:
        creds = Credentials(
            token=creds_data.get("access_token"),
            refresh_token=creds_data.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=YOUTUBE_CLIENT_ID,
            client_secret=YOUTUBE_CLIENT_SECRET,
        )
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    yt = build("youtube", "v3", credentials=creds)
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags,
            "categoryId": "17",   # Sports
            "defaultLanguage": "en",
            "defaultAudioLanguage": "en",
        },
        "status": {
            "privacyStatus": "public",
            "selfDeclaredMadeForKids": False,
        },
    }
    media = MediaFileUpload(video_file, chunksize=-1, resumable=True)
    req   = yt.videos().insert(part="snippet,status", body=body, media_body=media)
    try:
        resp     = req.execute()
        video_id = resp["id"]
        log(f"✅ Uploaded! https://youtube.com/watch?v={video_id}")
        if thumbnail_path and os.path.exists(thumbnail_path):
            yt.thumbnails().set(
                videoId=video_id,
                media_body=MediaFileUpload(thumbnail_path),
            ).execute()
            log("Thumbnail set.")
        return True
    except Exception as e:
        log(f"YouTube upload failed: {e}")
        return False

# ── main per-match pipeline ───────────────────────────────────────────────────
def process_match(fixture_id, home, away, hs, as_, comp_id, comp_name):
    log(f"\n{'='*60}\nProcessing: {home} {hs}–{as_} {away} [{comp_name}]\n{'='*60}")

    goals = get_match_goals(fixture_id)
    log(f"Goals: {goals}")

    # 1. Thumbnail
    thumb_path = generate_thumbnail(home, away, hs, as_, goals, comp_id,
                                    matchday=f"Matchday")
    if not thumb_path:
        log("❌ Thumbnail failed — skipping match.")
        return False

    # 2. Audio narration
    script     = generate_audio_script(home, away, hs, as_, goals, comp_id)
    audio_file = f"audio_{fixture_id}.mp3"
    text_to_speech(script, audio_file)

    # 3. Highlight clip (optional)
    hl_url  = get_highlight_url(home, away)
    hl_path = None
    if hl_url:
        hl_path = f"highlight_{fixture_id}.mp4"
        if not download_video(hl_url, hl_path):
            hl_path = None

    # 4. Build video
    intro_path   = COMPETITION_INTROS.get(comp_id, "assets/intros/premier_league.mp4")
    output_video = f"final_{sanitize(home)}_vs_{sanitize(away)}_{fixture_id}.mp4"
    ok = build_video(intro_path, hl_path, audio_file, output_video)
    if not ok:
        log("❌ Video build failed — skipping upload.")
        return False

    # 5. Metadata
    title       = _build_youtube_title(home, away, hs, as_, comp_name)
    description = _build_youtube_description(home, away, hs, as_, goals, comp_id, comp_name)
    tags        = _build_tags(home, away, comp_id, goals)
    log(f"Title: {title}")

    # 6. Upload
    success = upload_to_youtube(output_video, title, description, tags, thumb_path)

    # 7. Cleanup temp files
    for fp in [audio_file, output_video, thumb_path, hl_path]:
        if fp and os.path.exists(fp):
            try:
                os.remove(fp)
            except Exception:
                pass

    return success

# ── entry point ───────────────────────────────────────────────────────────────
def main():
    log("DEBUG: main() started")
    if DRY_RUN:
        log("⚠️  DRY RUN mode — no YouTube uploads will occur.")

    matches = fetch_finished_matches()
    if not matches:
        log("No new matches to process.")
        return

    uploaded = 0
    for entry in matches:
        fixture_id, home, away, hs, as_, comp_id, comp_name, _ = entry
        try:
            ok = process_match(fixture_id, home, away, hs, as_, comp_id, comp_name)
            if ok:
                mark_posted(fixture_id)
                uploaded += 1
                log(f"✅ Done: {home} vs {away} — total uploaded this run: {uploaded}")
                # YouTube allows ~6 uploads/day on new accounts; be safe
                if uploaded >= 5:
                    log("Reached upload limit for this run (5). Stopping.")
                    break
                time.sleep(10)   # small pause between uploads
        except Exception as e:
            log(f"❌ Unhandled error for {home} vs {away}: {e}")
            continue   # never let one match crash the whole run

    log(f"\nRun complete. {uploaded} video(s) uploaded.")

if __name__ == "__main__":
    main()
