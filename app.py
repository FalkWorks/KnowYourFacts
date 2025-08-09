import re
from urllib.parse import urlparse, parse_qs

from dash import Dash, html, dcc, Input, Output, State, ctx

app = Dash(__name__)
server = app.server  # falls du auf Render mit gunicorn startest

def is_valid_youtube_url(url: str) -> bool:
    """
    Akzeptiert u.a.:
    - https://www.youtube.com/watch?v=...
    - https://youtube.com/watch?v=...
    - https://youtu.be/VIDEOID
    - https://www.youtube.com/shorts/VIDEOID
    - mit/ohne weitere Query-Parameter
    """
    if not url:
        return False
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False
        host = (p.netloc or "").lower()
        path = (p.path or "")
        if "youtube.com" in host:
            # /watch?v=..., /shorts/<id>, /live/<id> gelten
            qs = parse_qs(p.query or "")
            if path.startswith("/watch"):
                return "v" in qs and len(qs["v"][0]) > 5
            if path.startswith("/shorts/") and len(path.split("/")[2]) >= 5:
                return True
            if path.startswith("/live/") and len(path.split("/")[2]) >= 5:
                return True
            # ggf. weitere Pfade zulassen
            return False
        if "youtu.be" in host:
            # Kurzlink: /<videoid>
            parts = [seg for seg in path.split("/") if seg]
            return len(parts) == 1 and len(parts[0]) >= 5
        return False
    except Exception:
        return False

app.layout = html.Div(
    style={"maxWidth": 720, "margin": "40px auto", "fontFamily": "system-ui, Arial"},
    children=[
        html.H2("YouTube‑URL prüfen (MVP Schritt 1)"),
        html.Div(
            style={"display": "flex", "gap": "10px", "alignItems": "center"},
            children=[
                dcc.Input(
                    id="url",
                    type="url",
                    placeholder="https://www.youtube.com/watch?v=...",
                    style={
                        "flex": 1,
                        "padding": "12px",
                        "borderRadius": "10px",
                        "border": "1px solid #ccc",
                    },
                ),
                html.Button(
                    "Absenden",
                    id="submit",
                    n_clicks=0,
                    style={
                        "padding": "12px 16px",
                        "border": "none",
                        "borderRadius": "10px",
                        "background": "#2563eb",
                        "color": "white",
                        "fontWeight": 600,
                        "cursor": "pointer",
                    },
                ),
            ],
        ),
        html.Div(id="result", style={"marginTop": "16px", "fontSize": "16px"}),
    ],
)

@app.callback(
    Output("result", "children"),
    Output("result", "style"),
    Input("submit", "n_clicks"),
    Input("url", "n_submit"),   # Enter im Textfeld triggert das
    State("url", "value"),
    prevent_initial_call=True,
)
def validate_url(n_clicks, n_submit, value):
    # Trigger egal ob Button oder Enter
    _ = (n_clicks, n_submit)
    ok = is_valid_youtube_url(value or "")
    if ok:
        return (
            html.Span(["✅ ", html.Strong("Gültige YouTube‑URL." )]),
            {"color": "#0a7f3f"},
        )
    else:
        return (
            html.Span(["❌ ", html.Strong("Keine gültige YouTube‑URL.")]),
            {"color": "#b00020"},
        )

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
