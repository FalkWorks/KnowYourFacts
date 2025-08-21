import os
from urllib.parse import urlparse, parse_qs

import sys
print(sys.version)

import json
import httpx
from dash import Dash, html, dcc, dash_table, Input, Output, State
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

OPENAI_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_KEY:
    print("WARN: OPENAI_API_KEY fehlt – die Faktenprüfung wird fehlschlagen.")

app = Dash(__name__)
server = app.server #render

# ---------- Utils ----------
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

def fetch_captions_simple(video_id: str):
    """
    Holt das YouTube-Transcript (ohne Audio), bevorzugt DE, dann EN.
    Rückgabe: (text, lang) oder (None, None)
    """
    yt = YouTubeTranscriptApi()
    try:
        for langs in (['de'], ['en'], ['de','en'], ['en','de']):
            try:
                chunks = yt.fetch(video_id, languages=langs)
                text = " ".join(c.text for c in chunks).strip()
                if text:
                    return text, (langs[0] if isinstance(langs, list) else langs)
            except NoTranscriptFound:
                continue
        return None, None
    except (TranscriptsDisabled, NoTranscriptFound):
        return None, None
    except Exception:
        return None, None

async def openai_facts(text: str, lang_hint: str = "de"):
    import json as _json
    system = (
        "Du bist ein Faktenprüf-Assistent. "
        "Extrahiere NUR objektiv überprüfbare Aussagen (Zahlen/Daten/Fakten). "
        "Bewerte jede Aussage als 'richtig' | 'falsch' | 'unklar'. "
        "Gib pro Eintrag 1–3 glaubwürdige Quellen-URLs an. "
        "Keine Erklärsätze außerhalb der verlangten JSON-Struktur."
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
                                # statt "format": "uri" -> Regex:
                                "pattern": "^https?://\\S+$"
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


    headers = {"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"}
    body = {
        "model": "gpt-4o-mini",   # Fallback: "gpt-4o" oder "gpt-4o-mini"
        "temperature": 0.1,
        "messages": messages,
        # -> Erzwinge wohlgeformtes JSON nach Schema:
        "response_format": {
            "type": "json_schema",
            "json_schema": schema
        },
        "max_tokens": 800
    }

    async with httpx.AsyncClient(timeout=120) as cx:
        r = await cx.post("https://api.openai.com/v1/chat/completions", headers=headers, json=body)
    if r.status_code >= 400:
        raise RuntimeError(f"OpenAI error {r.status_code}: {r.text}")

    data = r.json()
    content = data["choices"][0]["message"]["content"]

    # Jetzt sollte content garantiert ein JSON-Objekt mit 'items' sein:
    try:
        parsed = _json.loads(content)
    except Exception as e:
        # Wenn es trotz Schema schief geht, gib die Rohantwort zurück zur Diagnose
        raise RuntimeError(f"JSON-Parsing fehlgeschlagen: {e}\nAntwort: {content[:500]}")

    items = parsed.get("items", [])
    # Sicherheitsnetz: Liste von Dicts erzwingen
    norm = []
    for x in items:
        if isinstance(x, dict):
            claim = x.get("claim", "")
            verdict = x.get("verdict", "")
            sources = x.get("sources", [])
            if isinstance(sources, list):
                norm.append({"claim": claim, "verdict": verdict, "sources": sources})
        elif isinstance(x, str):
            norm.append({"claim": x, "verdict": "unklar", "sources": []})
    return norm


# ---------- UI ----------
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
                    style={"padding": "12px 16px", "border": "none", "borderRadius": "10px",
                           "background": "#2563eb", "color": "white", "fontWeight": 600, "cursor": "pointer"},
                ),
            ],
        ),
        html.Div(id="status", style={"marginTop": "10px", "color": "#555"}),
        html.Div(id="caption_info", style={"marginTop": "6px", "color": "#0a7f3f"}),
        html.Div(id="warning_info", style={"marginTop": "6px", "color": "#b06500"}),
        html.Hr(),
        html.Div(id="transcript_preview", style={"whiteSpace": "pre-wrap", "marginBottom": "16px", "display": "none"}),
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

# ---------- Callback ----------
@app.callback(
    Output("status", "children"),
    Output("caption_info", "children"),
    Output("warning_info", "children"),
    Output("transcript_preview", "children"),
    Output("transcript_preview", "style"),
    Output("facts_table", "data"),
    Input("analyze", "n_clicks"),
    State("url", "value"),
    prevent_initial_call=True,
)
def run_pipeline(n_clicks, url):
    if not url or not is_valid_youtube_url(url):
        return ("❌ Keine gültige YouTube‑URL.", "", "", "", {"display": "none"}, [])

    if not OPENAI_KEY:
        return ("⚠️ OPENAI_API_KEY fehlt.", "", "", "", {"display": "none"}, [])

    vid = get_video_id(url)
    if not vid:
        return ("❌ Konnte Video‑ID nicht ermitteln.", "", "", "", {"display": "none"}, [])

    # 1) Captions-only
    text, lang = fetch_captions_simple(vid)
    if not text:
        # MVP: kein Fallback auf Audio; klare Meldung:
        warn = "Für dieses Video sind keine Untertitel verfügbar. Bitte anderes Video probieren."
        return ("", "", warn, "", {"display": "none"}, [])

    # 2) LLM
    try:
        import asyncio
        results = asyncio.run(openai_facts(text, lang_hint=lang or "de"))
    except Exception as e:
        return (f"LLM-Fehler: {e}", "", "", "", {"display": "none"}, [])

    # 3) Tabelle
    rows = []
    for item in results:
        claim = item.get("claim", "")
        verdict = item.get("verdict", "")
        sources = item.get("sources", [])
        src_str = "\n".join(sources) if isinstance(sources, list) else str(sources)
        rows.append({"claim": claim, "verdict": verdict, "sources": src_str})


    # Optional: kurzes Preview vom Transkript (ausblenden im MVP)
    preview = text[:1000] + ("…" if len(text) > 1000 else "")
    cap_info = f"Untertitel gefunden (Quelle: YouTube, Sprache: {lang})."
    status = "Faktenprüfung abgeschlossen."
    return (status, cap_info, "", preview, {"display": "none"}, rows)

# Healthcheck (Render)
@server.route("/healthz")
def healthz():
    return "ok", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
