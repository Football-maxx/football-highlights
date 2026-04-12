#!/usr/bin/env python3
"""
Automated Football Highlight Video System
- Fetches finished matches from Football-Data.org
- Retrieves video highlights from Scorebat API
- Generates a custom thumbnail (team logos + result + real match image)
- Builds a video (intro + highlights + audio narration)
- Uploads to YouTube automatically
- Filters to priority teams and competitions only
- Only processes matches from the last 24 hours
"""

import os
import sys
import requests
import json
import time
import re
from datetime import datetime, timedelta, timezone
from moviepy import VideoFileClip, AudioFileClip, concatenate_videoclips
from PIL import Image, ImageDraw, ImageFont
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from supabase import create_client, Client
from gnews import GNews
from bs4 import BeautifulSoup
import openai

# ---------- CONFIGURATION (ENVIRONMENT VARIABLES) ----------
FOOTBALL_API_KEY = os.environ.get("FOOTBALL_API_KEY")
VOICERSS_API_KEY = os.environ.get("VOICERSS_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")
YOUTUBE_TOKEN_JSON = os.environ.get("YOUTUBE_TOKEN_JSON")
YOUTUBE_CLIENT_ID = os.environ.get("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
# ------------------------------------------------------------

# OpenAI client (optional, for AI metadata)
openai_client = openai.OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# Competition → intro video mapping (only competitions available on free tier)
COMPETITION_INTROS = {
    "PL": "assets/intros/premier_league.mp4",
    "PD": "assets/intros/laliga.mp4",
    "BL1": "assets/intros/bundesliga.mp4",
    "CL": "assets/intros/champions_league.mp4",
}

# Initialize Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

# Debug log
DEBUG_LOG = "debug.log"
with open(DEBUG_LOG, "w") as f:
    f.write("Debug log started\n")


def debug_print(msg):
    print(msg)
    sys.stdout.flush()
    with open(DEBUG_LOG, "a") as f:
        f.write(msg + "\n")
        f.flush()


def sanitize_filename(name):
    """Remove invalid characters from a string to make it safe for a filename."""
    return re.sub(r'[\\/*?:"<>|/]', '_', name).strip()


def fetch_finished_matches():
    """
    Fetch finished matches from Football-Data.org for all tracked competitions.
    Returns a list of matches that have not yet been posted and are from the last 24 hours.
    """
    headers = {"X-Auth-Token": FOOTBALL_API_KEY}
    posted_response = supabase.table("highlights_matches").select("fixture_id").eq("posted", 1).execute()
    posted_ids = [row["fixture_id"] for row in posted_response.data]

    # Priority teams
    PRIORITY_TEAMS = [
        "Arsenal", "Liverpool", "Manchester United", "Manchester City",
        "Chelsea", "Tottenham", "Newcastle", "Aston Villa",
        "Real Madrid", "Barcelona", "Atletico Madrid",
        "Bayern Munich", "Borussia Dortmund", "RB Leipzig",
        "Paris Saint-Germain", "Marseille",
        "AC Milan", "Inter Milan", "Juventus",
        "PSV", "Ajax", "Feyenoord",
        "Celtic", "Rangers"
    ]

    # Only competitions available on free tier
    competitions = [
        {"id": "PL", "name": "Premier League"},
        {"id": "PD", "name": "LaLiga"},
        {"id": "BL1", "name": "Bundesliga"},
        {"id": "CL", "name": "Champions League"},
    ]

    matches_to_process = []
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)

    for comp in competitions:
        comp_id = comp["id"]
        comp_name = comp["name"]
        debug_print(f"Fetching {comp_name} ({comp_id})")
        url = f"https://api.football-data.org/v4/competitions/{comp_id}/matches"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            time.sleep(6)
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                debug_print(f"Skipping {comp_name} – not available on free tier (403)")
            else:
                debug_print(f"HTTP error fetching {comp_name}: {e}")
            continue
        except Exception as e:
            debug_print(f"Error fetching {comp_name}: {e}")
            continue

        for match in data.get("matches", []):
            fixture_id = match["id"]
            home = match["homeTeam"]["name"]
            away = match["awayTeam"]["name"]
            status = match["status"]
            home_score = match["score"]["fullTime"]["home"] or 0
            away_score = match["score"]["fullTime"]["away"] or 0
            match_date_str = match["utcDate"]
            try:
                # Parse ISO 8601 string to timezone-aware datetime (UTC)
                match_date = datetime.fromisoformat(match_date_str.replace("Z", "+00:00"))
            except:
                match_date = datetime.now(timezone.utc)

            # Skip matches older than 24 hours
            if match_date < cutoff_time:
                debug_print(f"Skipping old match: {home} vs {away} ({match_date})")
                continue

            # Filter by priority teams
            if home not in PRIORITY_TEAMS and away not in PRIORITY_TEAMS:
                debug_print(f"Skipping {home} vs {away} – neither team is in priority list")
                continue

            if fixture_id in posted_ids:
                continue

            # Insert/update match in Supabase
            data_row = {
                "fixture_id": fixture_id,
                "home_team": home,
                "away_team": away,
                "match_date": match_date_str,
                "status": status,
                "home_score": home_score,
                "away_score": away_score,
                "competition": comp_id,
                "posted": 0,
            }
            supabase.table("highlights_matches").upsert(data_row, on_conflict="fixture_id").execute()

            if status == "FINISHED":
                debug_print(f"Found finished match: {home} vs {away} ({comp_name})")
                matches_to_process.append((fixture_id, home, away, home_score, away_score, comp_id))

    return matches_to_process


def get_highlights_from_scorebat(home, away):
    """Query the free Scorebat API for video highlights."""
    try:
        url = "https://www.scorebat.com/video-api/v1/"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        debug_print(f"Scorebat API request failed: {e}")
        return None

    for entry in data:
        if "warning" in entry:
            continue
        title = entry.get("title", "")
        side1 = entry.get("side1", {}).get("name", "")
        side2 = entry.get("side2", {}).get("name", "")
        if home in (title, side1, side2) and away in (title, side1, side2):
            videos = entry.get("videos", [])
            if videos:
                embed_code = videos[0].get("embed", "")
                soup = BeautifulSoup(embed_code, "html.parser")
                iframe = soup.find("iframe")
                if iframe and iframe.get("src"):
                    video_url = iframe["src"]
                    debug_print(f"Found Scorebat highlight: {video_url}")
                    return video_url
    debug_print(f"No Scorebat highlights found for {home} vs {away}")
    return None


def download_video(url, output_path):
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        debug_print(f"Video downloaded: {output_path}")
        return True
    except Exception as e:
        debug_print(f"Video download failed: {e}")
        return False


def fetch_match_image(home, away):
    try:
        gn = GNews(language="en", country="US", max_results=1)
        query = f"{home} vs {away} football match"
        articles = gn.get_news(query)
        if articles and articles[0].get("image"):
            img_url = articles[0]["image"]
            response = requests.get(img_url, timeout=10)
            if response.status_code == 200:
                img_path = f"temp_match_{sanitize_filename(home)}_{sanitize_filename(away)}.jpg"
                with open(img_path, "wb") as f:
                    f.write(response.content)
                debug_print(f"Downloaded match image from GNews: {img_url}")
                return img_path
    except Exception as e:
        debug_print(f"GNews image fetch failed: {e}")
    return "assets/generic_stadium.jpg"


def generate_thumbnail(home, away, home_score, away_score, image_path):
    safe_home = sanitize_filename(home)
    safe_away = sanitize_filename(away)
    output_path = f"thumbnail_{safe_home}_{safe_away}.jpg"

    try:
        background = Image.open("assets/background.png").convert("RGB")
        background = background.resize((1280, 720))
    except Exception as e:
        debug_print(f"Could not load background: {e}")
        return None

    # Load logos (if available)
    home_logo_path = f"assets/logos/{home}.png"
    away_logo_path = f"assets/logos/{away}.png"
    home_logo = None
    away_logo = None
    try:
        home_logo = Image.open(home_logo_path).convert("RGBA").resize((150, 150))
    except:
        pass
    try:
        away_logo = Image.open(away_logo_path).convert("RGBA").resize((150, 150))
    except:
        pass

    if home_logo:
        background.paste(home_logo, (80, 250), home_logo)
    if away_logo:
        background.paste(away_logo, (1280 - 230, 250), away_logo)

    # Draw score
    draw = ImageDraw.Draw(background)
    try:
        font = ImageFont.truetype("arial.ttf", 80)
    except:
        font = ImageFont.load_default()
    score_text = f"{home_score} – {away_score}"
    bbox = draw.textbbox((0, 0), score_text, font=font)
    tw = bbox[2] - bbox[0]
    draw.text(((1280 - tw) // 2, 380), score_text, fill="white", font=font, stroke_width=2, stroke_fill="black")

    # Paste match image
    try:
        if image_path and os.path.exists(image_path):
            match_img = Image.open(image_path).convert("RGB")
            match_img.thumbnail((1280, 400))
            x = (1280 - match_img.width) // 2
            background.paste(match_img, (x, 480))
        else:
            debug_print("Match image not available, using blank area")
    except Exception as e:
        debug_print(f"Could not paste match image: {e}")

    background.save(output_path)
    debug_print(f"Thumbnail saved: {output_path}")
    return output_path


def get_match_goals(fixture_id):
    url = f"https://api.football-data.org/v4/matches/{fixture_id}"
    headers = {"X-Auth-Token": FOOTBALL_API_KEY}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        data = resp.json()
        goals = []
        for goal in data.get("goals", []):
            scorer = goal.get("scorer", {}).get("name")
            if scorer:
                goals.append({"player": scorer, "minute": goal.get("minute")})
        return goals
    except Exception as e:
        debug_print(f"Could not fetch goals: {e}")
        return []


def generate_audio_script(home, away, home_score, away_score, goals):
    script = f"Here are the highlights from the match between {home} and {away}. "
    script += f"The final score was {home_score} to {away_score}. "
    if goals:
        script += "The goal scorers were: "
        for g in goals:
            script += f"{g['player']} in the {g['minute']}th minute, "
    else:
        script += "No goals were scored. "
    script += "Thanks for watching!"
    return script


def text_to_speech(text, filename):
    url = f"http://api.voicerss.org/?key={VOICERSS_API_KEY}&hl=en-gb&src={text}&f=44khz_16bit_stereo"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        with open(filename, "wb") as f:
            f.write(response.content)
        debug_print(f"Audio saved to {filename}")
        test = AudioFileClip(filename)
        test.close()
    except Exception as e:
        debug_print(f"TTS failed: {e}. Creating silent audio.")
        os.system(f"ffmpeg -f lavfi -i anullsrc=r=44100:cl=mono -t 10 -q:a 9 -acodec libmp3lame -y {filename}")


def build_video(intro_path, highlight_path, audio_path, output_path):
    if not os.path.exists(intro_path):
        debug_print(f"Intro video not found: {intro_path}")
        return
    intro = VideoFileClip(intro_path)
    clips = [intro]
    if highlight_path and os.path.exists(highlight_path):
        highlight = VideoFileClip(highlight_path)
        if highlight.duration > 20:
            highlight = highlight.subclipped(0, 20)
        clips.append(highlight)
    final = concatenate_videoclips(clips, method="compose")
    audio = AudioFileClip(audio_path)
    final = final.with_audio(audio)
    final.write_videofile(
        output_path,
        codec="libx264",
        audio_codec="aac",
        preset="ultrafast",
        threads=4,
        logger=None,
    )
    debug_print(f"Video saved to {output_path}")


def upload_to_youtube(video_file, title, description, tags, thumbnail_path):
    if DRY_RUN:
        debug_print(f"DRY RUN: Would upload '{title}'")
        return
    if not YOUTUBE_TOKEN_JSON:
        debug_print("YouTube token missing. Cannot upload.")
        return
    creds_data = json.loads(YOUTUBE_TOKEN_JSON)
    if "client_id" not in creds_data:
        creds = Credentials(
            token=creds_data.get("access_token"),
            refresh_token=creds_data.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=YOUTUBE_CLIENT_ID,
            client_secret=YOUTUBE_CLIENT_SECRET,
        )
    else:
        creds = Credentials.from_authorized_user_info(creds_data)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    youtube = build("youtube", "v3", credentials=creds)

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags,
            "categoryId": "17",
        },
        "status": {"privacyStatus": "public"},
    }
    media = MediaFileUpload(video_file, chunksize=-1, resumable=True)
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    try:
        response = request.execute()
        video_id = response["id"]
        debug_print(f"Upload successful! Video ID: {video_id}")
        if thumbnail_path and os.path.exists(thumbnail_path):
            youtube.thumbnails().set(videoId=video_id, media_body=thumbnail_path).execute()
            debug_print("Thumbnail uploaded.")
    except Exception as e:
        debug_print(f"Upload failed: {e}")


def process_match(fixture_id, home, away, home_score, away_score, comp_id):
    debug_print(f"Processing match: {home} vs {away} (fixture {fixture_id})")
    # 1. Fetch a real match image
    match_image = fetch_match_image(home, away)
    # 2. Generate thumbnail (skip if filename invalid)
    thumb_path = generate_thumbnail(home, away, home_score, away_score, match_image)
    if not thumb_path:
        debug_print("Skipping match due to thumbnail generation error")
        return
    # 3. Fetch goals and create audio narration
    goals = get_match_goals(fixture_id)
    script = generate_audio_script(home, away, home_score, away_score, goals)
    audio_file = f"audio_{fixture_id}.mp3"
    text_to_speech(script, audio_file)
    # 4. Get video highlights from Scorebat
    highlight_url = get_highlights_from_scorebat(home, away)
    highlight_path = None
    if highlight_url:
        highlight_path = f"highlight_{fixture_id}.mp4"
        if not download_video(highlight_url, highlight_path):
            highlight_path = None
    # 5. Build final video
    intro_path = COMPETITION_INTROS.get(comp_id, "assets/intros/premier_league.mp4")
    output_video = f"final_{fixture_id}.mp4"
    build_video(intro_path, highlight_path, audio_file, output_video)
    # 6. Prepare YouTube metadata
    if openai_client:
        title = f"{home} {home_score} – {away_score} {away} | Highlights"
        description = f"Highlights of {home} vs {away}. Goals: {', '.join([g['player'] for g in goals])}"
        tags = ["Football", "Highlights", home.replace(" ", ""), away.replace(" ", "")]
    else:
        title = f"{home} {home_score} – {away_score} {away} - {datetime.now().strftime('%Y%m%d-%H%M')}"
        description = f"Highlights of {home} vs {away}. #Football #Highlights"
        tags = ["Football", "Highlights"]
    # 7. Upload to YouTube
    upload_to_youtube(output_video, title, description, tags, thumb_path)
    # 8. Clean up temporary files
    for f in [audio_file, output_video, thumb_path, match_image, highlight_path]:
        if f and os.path.exists(f) and f not in ["assets/generic_stadium.jpg", "assets/placeholder_thumbnail.jpg"]:
            os.remove(f)


def main():
    try:
        debug_print("DEBUG: main() started")
        if DRY_RUN:
            debug_print("DRY RUN mode – no YouTube uploads.")
        matches = fetch_finished_matches()
        for (fixture_id, home, away, home_score, away_score, comp_id) in matches:
            process_match(fixture_id, home, away, home_score, away_score, comp_id)
            supabase.table("highlights_matches").update({"posted": 1}).eq("fixture_id", fixture_id).execute()
    except Exception as e:
        debug_print(f"FATAL ERROR: {e}")
        raise


if __name__ == "__main__":
    main()