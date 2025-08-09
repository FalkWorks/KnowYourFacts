import os
import hashlib
import subprocess
from urllib.parse import urlparse, parse_qs

import httpx
from dash import Dash, html, dcc, Input, Output, State
from flask import send_file
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

DG_KEY = os.getenv("DEEPGRAM_API_KEY")
if not DG_KEY:
    print("WARN: DEEPGRAM_API_KEY fehlt – Deepgram-Fallback wird fehlschlagen.")

app = Dash(__name__)
server = app.server  # Render

# ---------- Helpers ----------
def is_valid_youtube_url(url: str) -> bool:
    if not url:
        return False
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False
        host = (p.netloc or "").lower()
        path = (p.path or "")
        if "youtube.com" in host:
            qs = parse_qs(p.query or "")
            if path.startswith("/watch"):  # ?v=ID
                return "v" in qs and len(qs["v"][0]) > 5
            if path.startswith("/shorts/") and len(path.split("/")[2]) >= 5:
                return True
            if path.startswith("/live/") and len(path.split("/")[2]) >= 5:
                return True
            return False
        if "youtu.be" in host:  # youtu.be/ID
            parts = [seg for seg in (p.path or "").split("/") if seg]
            return len(parts) == 1 and len(parts[0]) >= 5
        return False
    except Exception:
        return False

def get_video_id(url: str) -> str | None:
    p = urlparse(url)
    host = (p.netloc or "").lower()
    path = (p.path or "")
    if "youtube.com" in host and path.startswith("/watch"):
        qs = parse_qs(p.query or "")
        return qs.get("v", [None])[0]
    if "youtu.be" in host:
        parts = [seg for seg in path.split("/") if seg]
        return parts[0] if parts else None
    if "youtube.com" in host and path.startswith("/shorts/"):
        return path.split("/")[2]
    if "youtube.com" in host and path.startswith("/live/"):
        return path.split("/")[2]
    return None

def fetch_youtube_captions(video_id: str, prefer_langs=("de","en")) -> str | None:
    """A: Captions-First. Holt vorhandene Untertitel (auch auto) ohne Login. Gibt reinen Text zurück."""
    try:
        tl = YouTubeTranscriptApi.list_transcripts(video_id)
        # 1) Manuell gepflegte Transkripte in bevorzugter Sprache
        for lang in prefer_langs:
            try:
                tr = tl.find_transcript([lang])
                chunks = tr.fetch()
                txt = " ".join(c["text"] for c in chunks if c.get("text")).strip()
                if txt:
                    return txt
            except Exception:
                pass
        # 2) Auto-generierte in bevorzugter Sprache
        for lang in prefer_langs:
            try:
                tr = tl.find_manually_created_transcript([lang])  # if exists, we already tried; continue
            except Exception:
                pass
            try:
                # auto transcript (might need translate if source differs)
                # try any transcript then translate
                for tr in tl:
                    try:
                        tr2 = tr.translate(lang)
                        chunks = tr2.fetch()
                        txt = " ".join(c["text"] for c in chunks if c.get("text")).strip()
                        if txt:
                            return txt
                    except Exception:
                        continue
            except Exception:
                pass
        # 3) Fallback: nimm irgendein verfügbares Transcript
        try:
            any_tr = next(iter(tl))
            chunks = any_tr.fetch()
            txt = " ".join(c["text"] for c in chunks if c.get("text")).strip()
            return txt or None
        except Exception:
            return None
    except (TranscriptsDisabled, NoTranscriptFound):
        return None
    except Exception:
        return None

def url_key(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]

def paths_for(url: str):
    key = url_key(url)
    tmp = "/tmp"
    raw = os.path.join(tmp, f"raw_{key}.m4a")
    wav = os.path.join(tmp, f"audio_{key}.wav")  # 16k/mono
    return key, raw, wav

def download_audio(youtube_url: str, raw_out: str):
    # B: Nur wenn keine Captions: yt-dlp versuchen
    from yt_dlp import YoutubeDL
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": raw_out,
        "quiet": True,
        "geo_bypass": True,
        "nocheckcertificate": True,
        # "extractor_args": {"youtube": {"player_client": ["android","web"]}},  # optional
    }
    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([youtube_url])

def to_wav_16k_mono(inp: str, outp: str):
    subprocess.run(
        ["ffmpeg", "-y", "-i", inp, "-ac", "1", "-ar", "16000", outp],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

async def deepgram_transcribe(wav_path: str) -> str:
    """C: Deepgram-Fallback – gibt reinen Text zurück."""
    headers = {"Authorization": f"Token {DG_KEY}"}
    params = {
        "punctuate": "true",
        "smart_format": "true",
        "detect_language": "true",
    }
    async with httpx.AsyncClient(timeout=300) as cx:
        with open(wav_path, "rb") as f:
            r = await cx.post(
                "https://api.deepgram.com/v1/listen",
                headers=headers, params=params, content=f.read()
            )
    r.raise_for_status()
    data = r.json()
    alt = (
        data.get("results", {})
            .get("channels", [{}])[0]
            .get("alternatives", [{}])[0]
    )
    transcript = alt.get("transcript") or ""
    if not transcript:
        paras = alt.get("paragraphs", {}).get("paragraphs", [])
        transcript = " ".join(p.get("text","") for p in paras if p.get("text"))
    return (transcript or "").strip()

# ---------- Serve WAV if we used fallback ----------
@server.route("/audio/<key>.wav")
def serve_audio(key):
    wav = os.path.join("/tmp", f"audio_{key}.wav")
    if not os.path.exists(wav):
        return ("Not found", 404)
    return send_file(wav, mimetype="audio/wav", as_attachment=False)

# ---------- UI ----------
app.layout = html.Div(
    style={"maxWidth": 780, "margin": "40px auto", "fontFamily": "system-ui, Arial"},
    children=[
        html.H2("Schritt 3 – Captions‑First → Fallback Deepgram"),
        html.Div(
            style={"display": "flex", "gap": "10px", "alignItems": "center"},
            children=[
                dcc.Input(
                    id="url",
                    type="url",
                    placeholder="https://www.youtube.com/watch?v=...",
                    style={"flex": 1, "padding": "12px", "borderRadius": "10px", "border": "1px solid #ccc"},
                ),
                html.Button(
                    "Absenden",
                    id="submit",
                    n_clicks=0,
                    style={"padding": "12px 16px", "border": "none", "borderRadius": "10px",
                           "background": "#2563eb", "color": "white", "fontWeight": 600, "cursor": "pointer"},
                ),
            ],
        ),
        html.Div(id="status_url", style={"marginTop": "10px"}),
        html.Hr(),
        html.Div(
            style={"display": "flex", "gap": "10px", "alignItems": "center"},
            children=[
                html.Button(
                    "Transkribieren",
                    id="transcribe_btn",
                    n_clicks=0,
                    style={"padding": "10px 14px", "borderRadius": "8px", "border": "1px solid #ddd", "cursor": "pointer"}
                ),
                html.Span(id="transcribe_status", style={"color": "#555"})
            ],
        ),
        html.Audio(id="player", src="", controls=True, style={"width": "100%", "marginTop": "8px"}),
        html.Div(id="transcript_text", style={"marginTop": "16px", "whiteSpace": "pre-wrap"}),
        dcc.Store(id="video_id_store"),
        dcc.Store(id="audio_key_store"),
    ],
)

# ---------- Callbacks ----------
@app.callback(
    Output("status_url", "children"),
    Output("video_id_store", "data"),
    Output("audio_key_store", "data"),
    Input("submit", "n_clicks"),
    Input("url", "n_submit"),
    State("url", "value"),
    prevent_initial_call=True,
)
def validate_url(n_clicks, n_submit, url):
    _ = (n_clicks, n_submit)
    if not url or not is_valid_youtube_url(url):
        return ("❌ Keine gültige YouTube‑URL.", None, None)
    vid = get_video_id(url)
    key, raw, wav = paths_for(url)
    # Wir EXTRAHIEREN NOCH NICHT — erst, wenn Captions fehlen
    return ("✅ Gültige YouTube‑URL.", {"video_id": vid, "url": url}, {"key": key, "url": url})

@app.callback(
    Output("transcribe_status", "children"),
    Output("transcript_text", "children"),
    Output("player", "src"),
    Input("transcribe_btn", "n_clicks"),
    State("video_id_store", "data"),
    State("audio_key_store", "data"),
    prevent_initial_call=True,
)
def transcribe_flow(n, vid_data, key_data):
    if not vid_data or not key_data:
        return ("Bitte zuerst URL prüfen.", "", "")

    video_id = vid_data.get("video_id")
    url = vid_data.get("url")
    key = key_data.get("key")
    raw, wav = paths_for(url)[1:]

    # A) Captions-First
    caps = None
    try:
        caps = fetch_youtube_captions(video_id, ("de","en"))
    except Exception:
        caps = None

    if caps:
        # Bypass Audio – direkt Captions anzeigen
        return ("Untertitel gefunden (Quelle: YouTube).", caps, "")

    # B) Kein Caption → Audio extrahieren (yt-dlp)
    try:
        if not os.path.exists(raw):
            download_audio(url, raw)
        if not os.path.exists(wav):
            to_wav_16k_mono(raw, wav)
    except Exception as e:
        msg = str(e)
        if "Sign in to confirm you’re not a bot" in msg or "confirm you're not a bot" in msg:
            return ("Dieses Video blockiert automatisches Abrufen. Bitte anderes Video testen.", "", "")
        return (f"Extraktion fehlgeschlagen: {e}", "", "")

    # C) Deepgram-Transkription
    if not DG_KEY:
        return ("Deepgram-Key fehlt – kann nicht transkribieren.", "", f"/audio/{key}.wav")
    try:
        import asyncio
        text = asyncio.run(deepgram_transcribe(wav))
        return ("Transkription (Deepgram) fertig.", text or "(leer)", f"/audio/{key}.wav")
    except Exception as e:
        return (f"Transkription fehlgeschlagen: {e}", "", f"/audio/{key}.wav")

# optional Healthcheck
@server.route("/healthz")
def healthz():
    return "ok", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
