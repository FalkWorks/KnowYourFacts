import os
import hashlib
import subprocess
from urllib.parse import urlparse, parse_qs

from dash import Dash, html, dcc, Input, Output, State, ctx
from flask import send_file

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
    tmp = "/tmp"  # auf Render schreibbar
    raw = os.path.join(tmp, f"raw_{key}.m4a")
    wav = os.path.join(tmp, f"audio_{key}.wav")  # 16k/mono für Deepgram + Playback
    return key, raw, wav

def download_audio(youtube_url: str, raw_out: str):
    # bestaudio -> m4a (ohne direkte Konvertierung; ffmpeg übernimmt danach)
    from yt_dlp import YoutubeDL
    ydl_opts = {"format": "bestaudio/best", "outtmpl": raw_out, "quiet": True}
    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([youtube_url])

def to_wav_16k_mono(inp: str, outp: str):
    # 16 kHz / Mono – ideal für Deepgram
    # Browser kann WAV problemlos abspielen (größer als mp3, aber passt fürs MVP)
    subprocess.run(
        ["ffmpeg", "-y", "-i", inp, "-ac", "1", "-ar", "16000", outp],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

# ---------- Flask route: serve audio ----------
@server.route("/audio/<key>.wav")
def serve_audio(key):
    wav = os.path.join("/tmp", f"audio_{key}.wav")
    if not os.path.exists(wav):
        return ("Not found", 404)
    # Achtung: für Produktion ggf. Cache-Header setzen
    return send_file(wav, mimetype="audio/wav", as_attachment=False)

# ---------- UI ----------
app.layout = html.Div(
    style={"maxWidth": 780, "margin": "40px auto", "fontFamily": "system-ui, Arial"},
    children=[
        html.H2("Schritt 2 – YouTube Tonspur extrahieren + Miniplayer"),
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
        html.Div(
            id="player_wrap",
            style={"marginTop": "12px"},
            children=[
                # MVP: nativer Player (Play + Positions-Slider inklusive)
                html.Audio(id="player", src="", controls=True, style={"width": "100%"}),
                # später (Schritt 2b): custom Button + Slider, wenn gewünscht
            ],
        ),
        dcc.Store(id="audio_src"),  # speichert die URL zum WAV
    ],
)

# ---------- Callbacks ----------

# 1) Validierung + Extraktion triggern (Button oder Enter im Feld)
@app.callback(
    Output("validity", "children"),
    Output("validity", "style"),
    Output("extract_status", "children"),
    Output("audio_src", "data"),
    Input("submit", "n_clicks"),
    Input("url", "n_submit"),
    State("url", "value"),
    prevent_initial_call=True,
)
def validate_and_extract(n_clicks, n_submit, url):
    _ = (n_clicks, n_submit)
    if not url or not is_valid_youtube_url(url):
        return (
            html.Span(["❌ ", html.Strong("Keine gültige YouTube‑URL.")]),
            {"color": "#b00020"},
            "",
            None,
        )
    # Gültig → extrahieren
    key, raw, wav = paths_for(url)
    try:
        # Download nur, wenn noch nicht vorhanden (schneller bei wiederholten Tests)
        if not os.path.exists(raw):
            download_audio(url, raw)
        # Konvertieren ins Deepgram-kompatible WAV
        if not os.path.exists(wav):
            to_wav_16k_mono(raw, wav)
    except Exception as e:
        return (
            html.Span(["⚠️ ", html.Strong("URL gültig, aber Extraktion fehlgeschlagen.")]),
            {"color": "#b06500"},
            f"Fehler: {e}",
            None,
        )
    # Erfolg
    audio_url = f"/audio/{key}.wav"
    return (
        html.Span(["✅ ", html.Strong("Gültige YouTube‑URL.")]),
        {"color": "#0a7f3f"},
        f"Tonspur extrahiert. Format: WAV (16kHz, mono).",
        {"src": audio_url},
    )

# 2) Player-Quelle setzen, sobald die Datei bereit ist
@app.callback(
    Output("player", "src"),
    Input("audio_src", "data"),
)
def set_player_src(data):
    if not data:
        return ""
    return data.get("src", "")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
