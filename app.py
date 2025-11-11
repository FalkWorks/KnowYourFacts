import os
import json
import time
import random
import threading
from urllib.parse import urlparse, parse_qs

import httpx
import requests
from dash import Dash, html, dcc, dash_table, Input, Output, State
from flask import jsonify
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
)
from youtube_transcript_api._errors import IpBlocked, RequestBlocked, YouTubeRequestFailed
from youtube_transcript_api.proxies import WebshareProxyConfig  # nur hier für YT nutzen
class RateLimitError(RuntimeError):
    """Spezifische Ausnahme, wenn YouTube zu viele Requests ablehnt."""


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

# Rate-Limit-Parameter – konservativ wählen, um 429-Antworten zu vermeiden
YOUTUBE_MIN_FETCH_INTERVAL = float(os.getenv("YOUTUBE_MIN_FETCH_INTERVAL", "4.5"))
YOUTUBE_FAILURE_TTL = float(os.getenv("YOUTUBE_FAILURE_TTL", "900"))  # Sekunden
YOUTUBE_TRANSCRIPT_MISS_TTL = float(
    os.getenv("YOUTUBE_TRANSCRIPT_MISS_TTL", "1800")
)
YOUTUBE_RATE_LIMIT_LOCK = threading.Lock()
LAST_YT_REQUEST_AT = 0.0
FAILED_TRANSCRIPT_CACHE: dict[str, dict[str, float | str]] = {}

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

def mark_transcript_failure(video_id: str, message: str, ttl: float | None = None):
    expires_at = time.time() + (ttl if ttl is not None else YOUTUBE_FAILURE_TTL)
    FAILED_TRANSCRIPT_CACHE[video_id] = {"expires": expires_at, "message": message}
def clear_transcript_failure(video_id: str):
    FAILED_TRANSCRIPT_CACHE.pop(video_id, None)
def get_recent_transcript_failure(video_id: str) -> str | None:
    data = FAILED_TRANSCRIPT_CACHE.get(video_id)
    if not data:
        return None
    expires = float(data.get("expires", 0))
    if time.time() > expires:
        FAILED_TRANSCRIPT_CACHE.pop(video_id, None)
        return None
    return str(data.get("message", ""))

def build_ytt_api(video_id: str) -> YouTubeTranscriptApi:
    """
    Proxy NUR für YT nutzen. Sticky Session optional durch Username-Variation.
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": YT_USER_AGENT,
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    proxy_cfg = None
    if YTT_PROXY_USERNAME and YTT_PROXY_PASSWORD:
        # Session leicht variieren, aber stabil pro Video (minimiert Flagging)
        session_suffix = video_id[-6:] if video_id else "s1"
        proxy_username = YTT_PROXY_USERNAME
        if "-session-" not in proxy_username:
            proxy_username = f"{proxy_username}-session-{session_suffix}"
        proxy_cfg = WebshareProxyConfig(
            proxy_username=proxy_username,
            proxy_password=YTT_PROXY_PASSWORD,
        )
    return YouTubeTranscriptApi(proxy_config=proxy_cfg, http_client=session)

def wait_for_youtube_slot(attempt: int) -> None:
    """Wartet optional, um Mindestabstand & Jitter sicherzustellen."""
    with YOUTUBE_RATE_LIMIT_LOCK:
        now = time.monotonic()
        wait_for = LAST_YT_REQUEST_AT + YOUTUBE_MIN_FETCH_INTERVAL - now
    base_wait = max(0.0, wait_for)
    jitter_base = YOUTUBE_MIN_FETCH_INTERVAL * (0.15 if attempt == 1 else 0.6)
    jitter = random.uniform(jitter_base * 0.5, jitter_base * 1.1)
    sleep_for = base_wait + jitter
    if sleep_for > 0:
        time.sleep(sleep_for)
def mark_youtube_request() -> None:
    global LAST_YT_REQUEST_AT
    with YOUTUBE_RATE_LIMIT_LOCK:
        LAST_YT_REQUEST_AT = time.monotonic()

def fetch_captions_once(ytt: YouTubeTranscriptApi, video_id: str):
    """
    EIN Versuch, EIN Request: eine Sprachliste ['de','en'].
    """
    chunks = ytt.fetch(video_id, languages=['de', 'en'])
    text = " ".join(c.text for c in chunks if c.text).strip()
    return (text or None), None  # Sprache ist hier nicht sicher bestimmbar

def fetch_with_retry(video_id: str, max_tries: int = 3, base_delay: float = 2.0):
    """
    Streng seriell, kleine Retries mit Jitter und Rate-Limit-Handling.
    """
    ytt = build_ytt_api(video_id)
    last_err: Exception | None = None

    for attempt in range(1, max_tries + 1):
        wait_for_youtube_slot(attempt)
        try:
            text, lang = fetch_captions_once(ytt, video_id)
            if text:
                return text, lang, None
            # Keine Transkripte vorhanden -> kein weiterer Retry nötig
            return None, None, "Keine Untertitel wurden von YouTube geliefert."
        except (NoTranscriptFound, TranscriptsDisabled) as e:
            return None, None, f"{e.__class__.__name__}: {e}"
        except (IpBlocked, RequestBlocked) as e:
            last_err = e
            if attempt < max_tries:
                cooldown = base_delay * (2 ** (attempt - 1)) * random.uniform(1.5, 2.3)
                time.sleep(min(cooldown, 60.0))
                continue
            raise RateLimitError(
                "YouTube hat den Abruf stark gedrosselt. Bitte mindestens 15 Minuten warten und erneut versuchen."
            ) from e
        except YouTubeRequestFailed as e:
            last_err = e
            if attempt < max_tries:
                delay = base_delay * (2 ** (attempt - 1)) * random.uniform(0.9, 1.3)
                time.sleep(min(delay, 20.0))
                continue
        except Exception as e:
            last_err = e
            if attempt < max_tries:
                delay = base_delay * (2 ** (attempt - 1)) * random.uniform(0.9, 1.2)
                time.sleep(min(delay, 6.0))
                continue
        finally:
            mark_youtube_request()

    # nach max_tries gescheitert
    print(f"Transcript fetch failed after {max_tries} tries. Last error: {last_err}")
    raise RuntimeError(
        f"Transcript fetch failed after {max_tries} tries. Last error: {last_err}"
    )

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
        dcc.Loading(
            id="caption_loading",
            type="circle",
            color="#2563eb",
            delay_show=400,
            children=[
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
            clear_transcript_failure(video_id)
            facts = openai_facts(cached_text, lang_hint=cached_lang or "de")
            return ("Faktenprüfung abgeschlossen (aus Cache).",
                    f"Untertitel (Cache) – Sprache: {cached_lang or 'de/en'}.",
                    "",
                    [{"claim": x["claim"], "verdict": x["verdict"], "sources": "\n".join(x["sources"])} for x in facts])

        recent_failure = get_recent_transcript_failure(video_id)
        if recent_failure:
            return ("", "", recent_failure, [])
        
        # (4)+(3)+(1) Strenger, bot-sicherer Fetch der Untertitel (seriell, Retries, 1 Sprachliste, Proxy nur für YT)
        try:
            text, lang = fetch_with_retry(video_id)
        except RateLimitError as e:
            message = str(e) or "YouTube hat den Abruf stark gedrosselt."
            mark_transcript_failure(video_id, message, ttl=YOUTUBE_FAILURE_TTL)
            print(f"Rate limit while fetching transcript for {video_id}: {e}")
            return ("", "", message, [])
        except RuntimeError as e:
            message = "YouTube hat den Abruf gedrosselt. Bitte später erneut versuchen oder anderes Video testen."
            mark_transcript_failure(video_id, message, ttl=YOUTUBE_FAILURE_TTL / 2)
            print(f"Transcript fetch failed for {video_id}: {e}")
            return ("", "", message, [])

        if not text:
            # Keine Untertitel vorhanden oder deaktiviert
            warn = "Für dieses Video sind keine Untertitel verfügbar. Bitte anderes Video probieren."
            detail = yt_message or "Keine weiteren Details von YouTube."
            message = "Für dieses Video sind keine Untertitel verfügbar. Bitte anderes Video probieren."
            mark_transcript_failure(video_id, message, ttl=YOUTUBE_TRANSCRIPT_MISS_TTL)
            return ("", "", message, [])
            return ("", "", message + f"youtube returned the following: {detail}", [])

        # (2) Cache setzen
        set_cached_transcript(video_id, text, lang)
        clear_transcript_failure(video_id)

        # LLM‑Faktenprüfung (ohne Proxy)
        facts = openai_facts(text, lang_hint=lang or "de")

        rows = [{"claim": x["claim"], "verdict": x["verdict"], "sources": "\n".join(x["sources"])} for x in facts]
        return ("Faktenprüfung abgeschlossen.",
                f"Untertitel gefunden (Quelle: YouTube, Sprache: {lang or 'de/en'}).",
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
