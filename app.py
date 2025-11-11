import os
import glob
import json
import re
import time
import tempfile
import threading
from urllib.parse import urlparse, parse_qs

import httpx
from dash import Dash, html, dcc, dash_table, Input, Output, State
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

class TranscriptFetchError(RuntimeError):
    """Technischer Fehler beim Laden der Untertitel."""


class TranscriptUnavailableError(RuntimeError):
    """Video enthält keine nutzbaren Untertitel."""


# =========================
# Konfiguration / Globals
# =========================
OPENAI_KEY = os.getenv("OPENAI_API_KEY")  # <--- NIEMALS hardcoden
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

# Welche Sprachen sollen priorisiert werden?
SUBTITLE_LANG_PREF = [
    lang.strip()
    for lang in os.getenv("YOUTUBE_SUBTITLE_LANGS", "de,en").split(",")
    if lang.strip()
]
if not SUBTITLE_LANG_PREF:
    SUBTITLE_LANG_PREF = ["de", "en"]

# TTLs für Fehler-Caching
YOUTUBE_FAILURE_TTL = float(os.getenv("YOUTUBE_FAILURE_TTL", "900"))  # Sekunden
YOUTUBE_TRANSCRIPT_MISS_TTL = float(
    os.getenv("YOUTUBE_TRANSCRIPT_MISS_TTL", "1800")
)
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

def vtt_to_text(vtt: str) -> str:
    """
    Entfernt Meta-Infos, Zeitstempel und HTML-Tags und gibt Klartext zurück.
    """
    vtt = re.sub(r"WEBVTT.*\n", "", vtt, flags=re.IGNORECASE)
    vtt = re.sub(r"\d{2}:\d{2}:\d{2}\.\d{3} --> .*", "", vtt)
    vtt = re.sub(r"<[^>]+>", "", vtt)
    vtt = re.sub(r"\s*\n\s*\n+", "\n", vtt)
    return vtt.strip()

def _extract_lang_from_filename(path: str) -> str | None:
    base = os.path.basename(path)
    parts = base.rsplit(".", 2)
    if len(parts) >= 2:
        return parts[-2]
    return None

def _lang_rank(lang: str | None, preference: list[str]) -> int:
    if not lang:
        return len(preference) + 1
    try:
        return preference.index(lang)
    except ValueError:
        return len(preference)

def fetch_captions_with_ytdlp(video_id: str, languages: list[str] | None = None):
    """
    Lädt Untertitel via yt-dlp ohne Proxy und gibt bereinigten Text zurück.
    """
    languages = languages or SUBTITLE_LANG_PREF
    url = f"https://www.youtube.com/watch?v={video_id}"
    with tempfile.TemporaryDirectory() as tmp_dir:
        outtmpl = os.path.join(tmp_dir, "%(id)s.%(lang)s.%(ext)s")
        ydl_opts = {
            "skip_download": True,
            "writesubtitles": True,
            "writeautomaticsub": True,
            "subtitleslangs": languages,
            "subtitlesformat": "vtt",
            "outtmpl": outtmpl,
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "http_headers": {"User-Agent": YT_USER_AGENT},
        }

        try:
            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
        except DownloadError as e:
            raise TranscriptFetchError(
                "Untertitel konnten nicht von YouTube geladen werden."
            ) from e
        except Exception as e:
            raise TranscriptFetchError(
                "Unerwarteter Fehler beim Laden der Untertitel."
            ) from e

        pattern = os.path.join(tmp_dir, f"{video_id}*.vtt")
        files = sorted(glob.glob(pattern))
        if not files:
            raise TranscriptUnavailableError(
                "Für dieses Video sind keine Untertitel verfügbar."
            )

        files.sort(key=lambda p: (_lang_rank(_extract_lang_from_filename(p), languages), p))
        for file_path in files:
            try:
                with open(file_path, "r", encoding="utf-8") as fh:
                    raw_vtt = fh.read()
            except OSError as e:
                continue

            cleaned = vtt_to_text(raw_vtt)
            if cleaned:
                lang = _extract_lang_from_filename(file_path) or (languages[0] if languages else None)
                return cleaned, lang

        raise TranscriptUnavailableError(
            "Die geladenen Untertiteldateien enthielten keinen Text."
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
        html.Div(
            id="youtube_error_response",
            style={
                "fontSize": "12px",
                "color": "#4b5563",
                "marginTop": "4px",
                "minHeight": "18px",
            },
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
    Output("youtube_error_response", "children"),
    Output("facts_table", "data"),
    Input("analyze", "n_clicks"),
    Input("url", "n_submit"),
    State("url", "value"),
    prevent_initial_call=True,
)
def run_pipeline(n_clicks, n_submit, url):
    # (6) Sofortiger Doppel-Trigger-Schutz & Serialisierung
    if not FACTCHECK_LOCK.acquire(blocking=False):
        return ("Bitte warten – ein anderer Auftrag läuft bereits.", "", "", "", [])

    try:
        # Basic Validierungen
        if not url or not is_valid_youtube_url(url):
            return ("❌ Keine gültige YouTube‑URL.", "", "", "", [])

        if not OPENAI_KEY:
            return ("⚠️ OPENAI_API_KEY fehlt.", "", "", "", [])

        video_id = get_video_id(url)
        if not video_id:
            return ("❌ Konnte Video‑ID nicht ermitteln.", "", "", "", [])

        # (2) Cache prüfen
        cached_text, cached_lang = get_cached_transcript(video_id)
        if cached_text:
            clear_transcript_failure(video_id)
            facts = openai_facts(cached_text, lang_hint=cached_lang or "de")
            return ("Faktenprüfung abgeschlossen (aus Cache).",
                    f"Untertitel (Cache) – Sprache: {cached_lang or 'de/en'}.",
                    "",
                    "",
                    [{"claim": x["claim"], "verdict": x["verdict"], "sources": "\n".join(x["sources"])} for x in facts])

        recent_failure = get_recent_transcript_failure(video_id)
        if recent_failure:
            yt_detail = f"YouTube-Antwort: {recent_failure}"
            return ("", "", recent_failure, yt_detail, [])
        
        # Untertitel laden (ohne Proxy, yt-dlp mit automatischen Subs)
        try:
            text, lang = fetch_captions_with_ytdlp(video_id, SUBTITLE_LANG_PREF)
        except TranscriptUnavailableError as e:
            message = str(e) or "Für dieses Video sind keine Untertitel verfügbar. Bitte anderes Video probieren."
            mark_transcript_failure(video_id, message, ttl=YOUTUBE_TRANSCRIPT_MISS_TTL)
            detail = f"YouTube-Antwort: {e.__cause__ or 'Keine weiteren Details von YouTube.'}"
            return ("", "", message, detail, [])
        except TranscriptFetchError as e:
            message = str(e) or "Untertitel konnten nicht geladen werden."
            mark_transcript_failure(video_id, message, ttl=YOUTUBE_FAILURE_TTL / 2)
            print(f"Transcript fetch failed for {video_id}: {e}")
            detail = f"Fehlerdetails: {e.__cause__ or e}"
            return ("", "", message, detail, [])

        # (2) Cache setzen
        set_cached_transcript(video_id, text, lang)
        clear_transcript_failure(video_id)

        # LLM‑Faktenprüfung (ohne Proxy)
        facts = openai_facts(text, lang_hint=lang or "de")

        rows = [{"claim": x["claim"], "verdict": x["verdict"], "sources": "\n".join(x["sources"])} for x in facts]
        return ("Faktenprüfung abgeschlossen.",
                f"Untertitel gefunden (Quelle: YouTube, Sprache: {lang or 'de/en'}).",
                "",
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
