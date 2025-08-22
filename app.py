import os
import json
import time
import random
import threading
from urllib.parse import urlparse, parse_qs

import httpx
from dash import Dash, html, dcc, dash_table, Input, Output, State
from flask import jsonify
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
from youtube_transcript_api.proxies import WebshareProxyConfig  # nur hier für YT nutzen

# =========================
# Konfiguration / Globals
# =========================
OPENAI_KEY = os.getenv("OPENAI_API_KEY")  # <--- NIEMALS hardcoden
YTT_PROXY_USERNAME = os.getenv("YTT_PROXY_USERNAME", "").strip()
YTT_PROXY_PASSWORD = os.getenv("YTT_PROXY_PASSWORD", "").strip()

# Serialisierung: Ein Job zur Zeit (verhindert parallele Transkript-Fetches)
FACTCHECK_LOCK = threading.Lock()

# Cache-Verzeichnis (persistiert pro Kaltstart; auf Render /tmp möglich)
CACHE_DIR = "/tmp/yt_caps_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# Realistischer User-Agent nur für YT
YT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# =========================
# Utilities
# =========================
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

def cache_path(video_id: str) -> str:
    return os.path.join(CACHE_DIR, f"{video_id}.json")

def get_cached_transcript(video_id: str):
    fp = cache_path(video_id)
    if os.path.exists(fp):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("text"), data.get("lang")
        except Exception:
            return None, None
    return None, None

def set_cached_transcript(video_id: str, text: str, lang: str | None):
    try:
        with open(cache_path(video_id), "w", encoding="utf-8") as f:
            json.dump({"text": text, "lang": lang}, f)
    except Exception:
        pass

def build_ytt_api(video_id: str) -> YouTubeTranscriptApi:
    """
    Proxy NUR für YT nutzen. Sticky Session optional durch Username-Variation.
    """
    if YTT_PROXY_USERNAME and YTT_PROXY_PASSWORD:
        # Session leicht variieren, aber stabil pro Video (minimiert Flagging)
        session_suffix = video_id[-6:] if video_id else "s1"
        username = f"{YTT_PROXY_USERNAME}-session-{session_suffix}"
        proxy_url = f"http://{username}:{YTT_PROXY_PASSWORD}@p.webshare.io:80"

        proxy_cfg = WebshareProxyConfig(proxy_username = YTT_PROXY_USERNAME, proxy_password=YTT_PROXY_PASSWORD)
        proxy_cfg.http = proxy_url
        proxy_cfg.https = proxy_url

        return YouTubeTranscriptApi(proxy_config=proxy_cfg)

def fetch_captions_once(ytt: YouTubeTranscriptApi, video_id: str):
    """
    EIN Versuch, EIN Request: eine Sprachliste ['de','en'].
    """
    chunks = ytt.fetch(video_id, languages=['de', 'en'])
    text = " ".join(c.text for c in chunks if c.text).strip()
    return (text or None), None  # Sprache ist hier nicht sicher bestimmbar

def fetch_with_retry(video_id: str, max_tries: int = 2, base_delay: float = 1.0):
    """
    Streng seriell, kleine Retries mit Jitter.
    """
    ytt = build_ytt_api(video_id)
    last_er = None
    for attempt in range(1, max_tries + 1):
        try:
            text, lang = fetch_captions_once(ytt, video_id)
            if text:
                return text, (lang or "de/en")
            # Keine Transkripte vorhanden -> kein weiterer Retry nötig
            return None, None
        except (NoTranscriptFound, TranscriptsDisabled):
            return None, None
        except Exception as e:
            last_err = e
            if attempt < max_tries:
                delay = base_delay * (2 ** (attempt - 1)) * random.uniform(0.9, 1.2)
                time.sleep(min(delay, 6.0))
                continue
    # nach max_tries gescheitert
    print(f"Transcript fetch failed after {max_tries} tries. Last error: {last_err}")
    raise RuntimeError(f"Transcript fetch failed after {max_tries} tries. Last error: {last_err}")

def normalize_urls(urls: list[str]) -> list[str]:
    out = []
    for u in urls or []:
        u = (u or "").strip()
        u = u.rstrip(".,;:)]}›’”\"'")
        if u.startswith("www."):
            u = "https://" + u
        out.append(u)
    return out

# =========================
# OpenAI – synchron, ohne Proxy
# =========================
def openai_facts(text: str, lang_hint: str = "de"):
    """
    Synchronously call OpenAI (structured outputs, robust JSON).
    """
    if not OPENAI_KEY:
        raise RuntimeError("OPENAI_API_KEY fehlt.")

    system = (
        "Du bist ein Faktenprüf-Assistent. "
        "Extrahiere NUR objektiv überprüfbare Aussagen (Zahlen/Daten/Fakten). "
        "Bewerte jede Aussage als 'richtig' | 'falsch' | 'unklar'. "
        "Gib pro Eintrag 1–3 GLAUBWÜRDIGE QUELLEN als vollständige, direkte URLs mit Protokoll an "
        "(z.B. https://bundesregierung.de/...; keine Startseiten, keine Kurz-URLs, keine Platzhalter). "
        "Wenn keine spezifische Quelle sicher ist, setze verdict='unklar' und sources=[]. "
        "Keine Erklärsätze außerhalb der JSON-Struktur."
    )

    clipped = text[:12000]
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": f"Sprache: {lang_hint}\nTranskript:\n{clipped}"}
    ]

    schema = {
        "name": "facts_response",
        "schema": {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["claim", "verdict", "sources"],
                        "properties": {
                            "claim":   {"type": "string"},
                            "verdict": {"type": "string", "enum": ["richtig", "falsch", "unklar"]},
                            "sources": {
                                "type": "array",
                                "items": {
                                    "type": "string",
                                    "pattern": "^https?://[^\\s)\\]}]+$"
                                },
                                "minItems": 0,
                                "maxItems": 3
                            }
                        },
                        "additionalProperties": False
                    }
                }
            },
            "required": ["items"],
            "additionalProperties": False
        },
        "strict": True
    }

    body = {
        "model": "gpt-4o-mini",  # Structured Outputs fähig und günstig
        "temperature": 0.1,
        "messages": messages,
        "response_format": {"type": "json_schema", "json_schema": schema},
        "max_tokens": 800,
    }

    headers = {"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"}
    with httpx.Client(timeout=120) as cx:  # kein Proxy!
        r = cx.post("https://api.openai.com/v1/chat/completions", headers=headers, json=body)
    if r.status_code >= 400:
        raise RuntimeError(f"OpenAI error {r.status_code}: {r.text}")

    content = r.json()["choices"][0]["message"]["content"]
    parsed = json.loads(content)  # dank Schema: valides JSON-Objekt
    items = parsed.get("items", [])

    # Normalize
    out = []
    for x in items:
        claim = x.get("claim", "")
        verdict = x.get("verdict", "")
        sources = normalize_urls(x.get("sources", []))
        out.append({"claim": claim, "verdict": verdict, "sources": sources})
    return out

# =========================
# Dash App
# =========================
app = Dash(__name__)
server = app.server

app.layout = html.Div(
    style={"maxWidth": 900, "margin": "40px auto", "fontFamily": "system-ui, Arial"},
    children=[
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
                    "Fakten prüfen",
                    id="analyze",
                    n_clicks=0,
                    disabled=False,  # wird im Callback via Lock „virtuell“ serialisiert
                    style={"padding": "12px 16px", "border": "none", "borderRadius": "10px",
                           "background": "#2563eb", "color": "white", "fontWeight": 600, "cursor": "pointer"},
                ),
            ],
        ),
        html.Div(id="status", style={"marginTop": "10px", "color": "#555"}),
        html.Div(id="caption_info", style={"marginTop": "6px", "color": "#0a7f3f"}),
        html.Div(id="warning_info", style={"marginTop": "6px", "color": "#b06500"}),
        html.Hr(),
        dash_table.DataTable(
            id="facts_table",
            columns=[
                {"name": "Aussage", "id": "claim"},
                {"name": "Bewertung", "id": "verdict"},
                {"name": "Quellen", "id": "sources"},
            ],
            data=[],
            style_cell={"whiteSpace": "pre-line", "fontSize": 14},
            style_table={"overflowX": "auto"},
        ),
    ],
)

# =========================
# Callback
# =========================
@app.callback(
    Output("status", "children"),
    Output("caption_info", "children"),
    Output("warning_info", "children"),
    Output("facts_table", "data"),
    Input("analyze", "n_clicks"),
    Input("url", "n_submit"),
    State("url", "value"),
    prevent_initial_call=True,
)
def run_pipeline(n_clicks, n_submit, url):
    # (6) Sofortiger Doppel-Trigger-Schutz & Serialisierung
    if not FACTCHECK_LOCK.acquire(blocking=False):
        return ("Bitte warten – ein anderer Auftrag läuft bereits.", "", "", [])

    try:
        # Basic Validierungen
        if not url or not is_valid_youtube_url(url):
            return ("❌ Keine gültige YouTube‑URL.", "", "", [])

        if not OPENAI_KEY:
            return ("⚠️ OPENAI_API_KEY fehlt.", "", "", [])

        video_id = get_video_id(url)
        if not video_id:
            return ("❌ Konnte Video‑ID nicht ermitteln.", "", "", [])

        # (2) Cache prüfen
        cached_text, cached_lang = get_cached_transcript(video_id)
        if cached_text:
            facts = openai_facts(cached_text, lang_hint=cached_lang or "de")
            return ("Faktenprüfung abgeschlossen (aus Cache).",
                    f"Untertitel (Cache) – Sprache: {cached_lang or 'de/en'}.",
                    "",
                    [{"claim": x["claim"], "verdict": x["verdict"], "sources": "\n".join(x["sources"])} for x in facts])

        # (4)+(3)+(1) Strenger, bot-sicherer Fetch der Untertitel (seriell, 2 Retries, 1 Sprachliste, Proxy nur für YT)
        try:
            text, lang = fetch_with_retry(video_id, max_tries=2, base_delay=1.0)
        except RuntimeError as e:
            # Zu viele 429 / Sorry-Pages / Netzfehler
            return ("", "", "YouTube hat den Abruf gedrosselt. Bitte später erneut versuchen oder anderes Video testen.", [])

        if not text:
            # Keine Untertitel vorhanden oder deaktiviert
            return ("", "", "Für dieses Video sind keine Untertitel verfügbar. Bitte anderes Video probieren.", [])

        # (2) Cache setzen
        set_cached_transcript(video_id, text, lang)

        # LLM‑Faktenprüfung (ohne Proxy)
        facts = openai_facts(text, lang_hint=lang or "de")

        rows = [{"claim": x["claim"], "verdict": x["verdict"], "sources": "\n".join(x["sources"])} for x in facts]
        return ("Faktenprüfung abgeschlossen.",
                f"Untertitel gefunden (Quelle: YouTube, Sprache: {lang}).",
                "",
                rows)

    finally:
        FACTCHECK_LOCK.release()

# Healthcheck
@server.route("/healthz")
def healthz():
    return "ok", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    # Debug AUS, um doppelte Aufrufe/Reloader zu vermeiden
    app.run(host="0.0.0.0", port=port, debug=False)
