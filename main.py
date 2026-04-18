#!/usr/bin/env python3
"""
Automated Football Highlight Video System
- Fetches finished matches from Football-Data.org
- Retrieves video highlights from Scorebat API
- Generates a custom thumbnail (team logos + score + video frame + highlights text)
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

openai_client = openai.OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

COMPETITION_INTROS = {
    "PL": "assets/intros/premier_league.mp4",
    "PD": "assets/intros/laliga.mp4",
    "BL1": "assets/intros/bundesliga.mp4",
    "CL": "assets/intros/champions_league.mp4",
}

supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

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
    return re.sub(r'[\\/*?:"<>|/]', '_', name).strip()

def fetch_finished_matches():
    headers = {"X-Auth-Token": FOOTBALL_API_KEY}
    posted_response = supabase.table("highlights_matches").select("fixture_id").eq("posted", 1).execute()
    posted_ids = [row["fixture_id"] for row in posted_response.data]

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
                match_date = datetime.fromisoformat(match_date_str.replace("Z", "+00:00"))
            except:
                match_date = datetime.now(timezone.utc)

            if match_date < cutoff_time:
                debug_print(f"Skipping old match: {home} vs {away} ({match_date})")
                continue

            if home not in PRIORITY_TEAMS and away not in PRIORITY_TEAMS:
                debug_print(f"Skipping {home} vs {away} – neither team is in priority list")
                continue

            if fixture_id in posted_ids:
                continue

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

def get_match_goals(fixture_id):
    url = f"https://api.football-data.org/v4/matches/{fixture_id}"
    headers = {"X-Auth-Token": FOOTBALL_API_KEY}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        data = resp.json()
        goals = []
        for goal in data.get("goals", []):
            scorer = goal.get("scorer", {}).get("name")
            minute = goal.get("minute")
            if scorer:
                goals.append({"player": scorer, "minute": minute})
                debug_print(f"Found goal: {scorer} at {minute}'")
        return goals
    except Exception as e:
        debug_print(f"Could not fetch goals: {e}")
        return []

def generate_audio_script(home, away, home_score, away_score, goals):
    script = f"Here are the highlights from the match between {home} and {away}. "
    script += f"The final score was {home_score} to {away_score}. "
    if goals and len(goals) > 0:
        script += "The goal scorers were: "
        for i, g in enumerate(goals):
            if i > 0:
                script += ", "
            script += f"{g['player']} in the {g['minute']}th minute"
        script += ". "
    else:
        script += "No goals were scored. "
    script += "Thanks for watching!"
    debug_print(f"Generated script: {script}")
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

def generate_thumbnail(home, away, home_score, away_score, video_clip_path, matchday=None):
    """
    Create a thumbnail like the example:
    - Left 1/3: Team logos stacked vertically, score in middle
    - Right 2/3: Frame from video with "HIGHLIGHTS" and matchday text overlay
    """
    width, height = 1280, 720
    # Create a base dark background
    thumbnail = Image.new("RGB", (width, height), color=(20, 20, 30))
    draw = ImageDraw.Draw(thumbnail)

    # ---- Left 1/3 (0-426) ----
    left_width = 426
    # Load logos
    home_logo_path = f"assets/logos/{home}.png"
    away_logo_path = f"assets/logos/{away}.png"
    try:
        home_logo = Image.open(home_logo_path).convert("RGBA").resize((180, 180))
    except:
        home_logo = None
    try:
        away_logo = Image.open(away_logo_path).convert("RGBA").resize((180, 180))
    except:
        away_logo = None

    # Paste logos (centered horizontally in left section)
    if home_logo:
        x_home = (left_width - home_logo.width) // 2
        thumbnail.paste(home_logo, (x_home, 140), home_logo)
    if away_logo:
        x_away = (left_width - away_logo.width) // 2
        thumbnail.paste(away_logo, (x_away, 380), away_logo)

    # Draw score in the middle of left section
    try:
        font = ImageFont.truetype("arial.ttf", 100)
    except:
        font = ImageFont.load_default()
    score_text = f"{home_score} - {away_score}"
    bbox = draw.textbbox((0, 0), score_text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    score_x = (left_width - tw) // 2
    score_y = 320 - th//2
    draw.text((score_x, score_y), score_text, fill="yellow", font=font, stroke_width=3, stroke_fill="black")

    # ---- Right 2/3 (427-1280) ----
    right_width = width - left_width
    right_start = left_width

    # Extract a frame from the video clip
    if video_clip_path and os.path.exists(video_clip_path):
        try:
            from moviepy import VideoFileClip
            clip = VideoFileClip(video_clip_path)
            # Get a frame at 20% into the video (to avoid black frames)
            frame_time = min(clip.duration * 0.2, clip.duration - 0.1)
            frame = clip.get_frame(frame_time)
            frame_img = Image.fromarray(frame)
            frame_img = frame_img.resize((right_width, height))
            thumbnail.paste(frame_img, (right_start, 0))
            clip.close()
        except Exception as e:
            debug_print(f"Could not extract video frame: {e}")
            # Fallback: gradient
            for i in range(right_width):
                color = (40 + i//5, 40 + i//5, 60 + i//5)
                draw.line([(right_start + i, 0), (right_start + i, height)], fill=color)
    else:
        # No video – use gradient
        for i in range(right_width):
            color = (40 + i//5, 40 + i//5, 60 + i//5)
            draw.line([(right_start + i, 0), (right_start + i, height)], fill=color)

    # Overlay text "HIGHLIGHTS" and matchday on the right side
    try:
        highlight_font = ImageFont.truetype("arial.ttf", 80)
        small_font = ImageFont.truetype("arial.ttf", 40)
    except:
        highlight_font = ImageFont.load_default()
        small_font = ImageFont.load_default()

    # "HIGHLIGHTS" text at top right
    highlights_text = "HIGHLIGHTS"
    bbox = draw.textbbox((0, 0), highlights_text, font=highlight_font)
    tw = bbox[2] - bbox[0]
    text_x = right_start + (right_width - tw) // 2
    text_y = 100
    draw.text((text_x, text_y), highlights_text, fill="white", font=highlight_font, stroke_width=2, stroke_fill="black")

    # Matchday info (if available, else use competition)
    if not matchday:
        matchday = "MATCHDAY"
    matchday_text = matchday.upper()
    bbox = draw.textbbox((0, 0), matchday_text, font=small_font)
    tw = bbox[2] - bbox[0]
    text_x = right_start + (right_width - tw) // 2
    text_y = height - 80
    draw.text((text_x, text_y), matchday_text, fill="yellow", font=small_font, stroke_width=1, stroke_fill="black")

    # Optional: add a small timestamp (like 1:10) – we can use video duration
    if video_clip_path and os.path.exists(video_clip_path):
        try:
            from moviepy import VideoFileClip
            clip = VideoFileClip(video_clip_path)
            duration = int(clip.duration)
            minutes = duration // 60
            seconds = duration % 60
            timestamp = f"{minutes}:{seconds:02d}"
            clip.close()
        except:
            timestamp = "0:30"
    else:
        timestamp = "0:30"

    bbox = draw.textbbox((0, 0), timestamp, font=small_font)
    tw = bbox[2] - bbox[0]
    text_x = right_start + right_width - tw - 20
    text_y = height - 80
    draw.text((text_x, text_y), timestamp, fill="white", font=small_font, stroke_width=1, stroke_fill="black")

    output_path = f"thumbnail_{sanitize_filename(home)}_{sanitize_filename(away)}.jpg"
    thumbnail.save(output_path)
    debug_print(f"Thumbnail saved: {output_path}")
    return output_path

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

    enhanced_description = f"""{title}

🎬 Match Highlights from {home} vs {away}

⚽ Final Score: {home_score} - {away_score}

📋 Key Moments:
- Full match highlights
- All goals and key plays
- Professional analysis

🔔 Subscribe for more football highlights!

#Football #Soccer #Highlights #MatchRecap #Goal #PremierLeague #LaLiga #Bundesliga #ChampionsLeague #FootballHighlights #Sports #UCL #FootballFan #GoalOfTheDay #MatchDay

👍 Like and share if you enjoyed this video!
💬 Comment your thoughts below!
🔔 Turn on notifications to never miss a highlight!
"""

    enhanced_tags = [
        "Football", "Soccer", "Highlights", "Match Recap", "Goal",
        "Premier League", "LaLiga", "Bundesliga", "Champions League",
        f"{home} vs {away}", f"{home} {away}", "Football Highlights",
        "Sports Highlights", "Football Match", "Soccer Highlights",
        "Goal Highlights", "Best Goals", "Football Goals", "Soccer Goals",
        "Matchday", "Football Fans", "Sports News", "Football Updates"
    ]

    body = {
        "snippet": {
            "title": title,
            "description": enhanced_description,
            "tags": enhanced_tags,
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

    # Get video highlights
    highlight_url = get_highlights_from_scorebat(home, away)
    highlight_path = None
    if highlight_url:
        highlight_path = f"highlight_{fixture_id}.mp4"
        if not download_video(highlight_url, highlight_path):
            highlight_path = None

    # Generate thumbnail using the video clip
    thumb_path = generate_thumbnail(home, away, home_score, away_score, highlight_path, matchday="MATCHDAY")
    if not thumb_path:
        debug_print("Skipping match due to thumbnail generation error")
        return

    # Fetch goals and create audio narration
    goals = get_match_goals(fixture_id)
    script = generate_audio_script(home, away, home_score, away_score, goals)
    audio_file = f"audio_{fixture_id}.mp3"
    text_to_speech(script, audio_file)

    # Build final video
    intro_path = COMPETITION_INTROS.get(comp_id, "assets/intros/premier_league.mp4")
    output_video = f"final_{fixture_id}.mp4"
    build_video(intro_path, highlight_path, audio_file, output_video)

    # Prepare YouTube metadata
    if openai_client:
        title = f"{home} {home_score} – {away_score} {away} | Highlights"
        description = f"Highlights of {home} vs {away}. Goals: {', '.join([g['player'] for g in goals])}"
        tags = ["Football", "Highlights", home.replace(" ", ""), away.replace(" ", "")]
    else:
        title = f"{home} {home_score} – {away_score} {away} - {datetime.now().strftime('%Y%m%d-%H%M')}"
        description = f"Highlights of {home} vs {away}. #Football #Highlights"
        tags = ["Football", "Highlights"]

    # Upload to YouTube (pass home, away, home_score, away_score for description)
    upload_to_youtube(output_video, title, description, tags, thumb_path)

    # Cleanup
    for f in [audio_file, output_video, thumb_path, highlight_path]:
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