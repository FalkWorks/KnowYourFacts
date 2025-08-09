import os
import hashlib
import subprocess
from urllib.parse import urlparse, parse_qs

import httpx
from dash import Dash, html, dcc, Input, Output, State
from flask import send_file

DG_KEY = os.getenv("DEEPGRAM_API_KEY")
if not DG_KEY:
    print("WARN: DEEPGRAM_API_KEY fehlt – Transkription wird fehlschlagen.")

app = Dash(__name__)
server = app.server  # für Render

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
            if path.startswith("/watch"):
                return "v" in qs and len(qs["v"][0]) > 5
            if path.startswith("/shorts/") and len(path.split("/")[2]) >= 5:
                return True
            if path.startswith("/live/") and len(path.split("/")[2]) >= 5:
                return True
            return False
        if "youtu.be" in host:
            parts = [seg for seg in path.split("/") if seg]
            return len(parts) == 1 and len(parts[0]) >= 5
        return False
    except Exception:
        return False

def url_key(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]

def paths_for(url: str):
    key = url_key(url)
    tmp = "/tmp"
    raw = os.path.join(tmp, f"raw_{key}.m4a")
    wav = os.path.join(tmp, f"audio_{key}.wav")  # 16k/mono
    return key, raw, wav

def download_audio(youtube_url: str, raw_out: str):
    from yt_dlp import YoutubeDL
    ydl_opts = {"format": "bestaudio/best", "outtmpl": raw_out, "quiet": True}
    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([youtube_url])

def to_wav_16k_mono(inp: str, outp: str):
    subprocess.run(
        ["ffmpeg", "-y", "-i", inp, "-ac", "1", "-ar", "16000", outp],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

async def deepgram_transcribe(wav_path: str) -> str:
    """Gibt reinen Text zurück (ein String)."""
    headers = {"Authorization": f"Token {DG_KEY}"}
    params = {
        "punctuate": "true",
        "smart_format": "true",
        # nur Text: keine Diarisierung nötig; Sprache automatisch erkennen
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
    # Robuster Zugriff auf den Gesamttext
    alt = (
        data.get("results", {})
            .get("channels", [{}])[0]
            .get("alternatives", [{}])[0]
    )
    transcript = alt.get("transcript") or ""
    # Fallback: paragraphs zusammensetzen
    if not transcript:
        paras = alt.get("paragraphs", {}).get("paragraphs", [])
        transcript = " ".join([p.get("text","") for p in paras if p.get("text")])
    return transcript.strip()

# ---------- Static serving: WAV ----------
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
        html.H2("Schritt 2/3 – Tonspur + Transkript (Deepgram)"),
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
        html.Div(id="validity", style={"marginTop": "12px", "fontSize": "16px"}),
        html.Hr(),
        html.Div(id="extract_status", style={"marginTop": "8px", "fontSize": "14px", "color": "#555"}),
        html.Audio(id="player", src="", controls=True, style={"width": "100%", "marginTop": "8px"}),
        html.Div(style={"marginTop": "10px"}, children=[
            html.Button(
                "Transkribieren",
                id="transcribe_btn",
                n_clicks=0,
                style={"padding": "10px 14px", "borderRadius": "8px", "border": "1px solid #ddd", "cursor": "pointer"}
            ),
            html.Span(id="transcribe_status", style={"marginLeft": "12px", "color": "#555"})
        ]),
        html.Div(id="transcript_text", style={"marginTop": "16px", "whiteSpace": "pre-wrap"}),
        dcc.Store(id="audio_src"),
        dcc.Store(id="audio_key"),
    ],
)

# ---------- Callbacks ----------
@app.callback(
    Output("validity", "children"),
    Output("validity", "style"),
    Output("extract_status", "children"),
    Output("audio_src", "data"),
    Output("audio_key", "data"),
    Input("submit", "n_clicks"),
    Input("url", "n_submit"),
    State("url", "value"),
    prevent_initial_call=True,
)
def validate_and_extract(n_clicks, n_submit, url):
    _ = (n_clicks, n_submit)
    if not url or not is_valid_youtube_url(url):
        return (
            "❌ Keine gültige YouTube‑URL.",
            {"color": "#b00020"}, "", None, None
        )
    key, raw, wav = paths_for(url)
    try:
        if not os.path.exists(raw):
            download_audio(url, raw)
        if not os.path.exists(wav):
            to_wav_16k_mono(raw, wav)
    except Exception as e:
        return (
            "⚠️ URL gültig, aber Extraktion fehlgeschlagen.",
            {"color": "#b06500"}, f"Fehler: {e}", None, None
        )
    audio_url = f"/audio/{key}.wav"
    return (
        "✅ Gültige YouTube‑URL.",
        {"color": "#0a7f3f"},
        "Tonspur extrahiert (WAV 16kHz/mono).",
        {"src": audio_url},
        {"key": key},
    )

@app.callback(
    Output("player", "src"),
    Input("audio_src", "data"),
)
def set_player_src(data):
    if not data:
        return ""
    return data.get("src", "")

@app.callback(
    Output("transcribe_status", "children"),
    Output("transcript_text", "children"),
    Input("transcribe_btn", "n_clicks"),
    State("audio_key", "data"),
    prevent_initial_call=True,
)
def do_transcribe(n, key_data):
    if not key_data:
        return ("Bitte zuerst Tonspur extrahieren.", "")
    if not DG_KEY:
        return ("Deepgram API‑Key fehlt.", "")
    key = key_data.get("key")
    wav = os.path.join("/tmp", f"audio_{key}.wav")
    if not os.path.exists(wav):
        return ("Audio nicht gefunden. Bitte erneut extrahieren.", "")
    try:
        import asyncio
        text = asyncio.run(deepgram_transcribe(wav))  # Dash callback ist sync → einmal run
        if not text:
            return ("Transkription fertig (leer).", "")
        return ("Transkription fertig.", text)
    except Exception as e:
        return (f"Transkription fehlgeschlagen: {e}", "")

# optional: Healthcheck für Render
@server.route("/healthz")
def healthz():
    return "ok", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)