from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from app.core.config import settings
from app.models.schemas import ChatResponse
from app.services.chat_service import answer_query
from app.services.feedback_service import add_chat_feedback


router = APIRouter(tags=["ui"])


def _require_ui_token(request: Request) -> None:
    expected = settings.chat_ui_token
    if not expected:
        raise HTTPException(status_code=503, detail="CHAT_UI_TOKEN not set")
    token = request.headers.get("X-Chat-Token") or request.query_params.get("token")
    if token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


class ChatRequest(BaseModel):
    query: str
    n_sims: int = 50000
    seed: int = 42
    bankroll: float = 1000.0
    session_id: str | None = None


class FeedbackRequest(BaseModel):
    query: str
    response: str
    label: str
    notes: str | None = None
    match_id: str | None = None
    meta: dict | None = None


@router.get("/ui", response_class=HTMLResponse)
def chat_ui(request: Request) -> HTMLResponse:
    _require_ui_token(request)
    return HTMLResponse(HTML_PAGE)


@router.post("/v1/chat-ui", response_model=ChatResponse)
def chat_ui_api(req: ChatRequest, request: Request) -> ChatResponse:
    _require_ui_token(request)
    return answer_query(
        query=req.query,
        n_sims=req.n_sims,
        seed=req.seed,
        bankroll=req.bankroll,
        session_id=req.session_id,
    )


@router.post("/v1/chat-feedback")
def chat_feedback(req: FeedbackRequest, request: Request) -> dict:
    _require_ui_token(request)
    feedback_id = add_chat_feedback(
        query=req.query,
        response=req.response,
        label=req.label,
        notes=req.notes,
        match_id=req.match_id,
        meta=req.meta,
    )
    return {"status": "OK", "feedback_id": feedback_id}


HTML_PAGE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Football Predictor - Private Chat</title>
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600&family=IBM+Plex+Mono:wght@400;500&display=swap');
      :root {
        --ink: #1b1f1a;
        --muted: #6b6f64;
        --card: #f8f4ea;
        --card-2: #fffdf7;
        --accent: #e07a3f;
        --accent-2: #2a7f6f;
        --line: #e2ddd0;
        --shadow: 0 12px 30px rgba(30, 30, 30, 0.08);
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: 'Space Grotesk', sans-serif;
        color: var(--ink);
        background: radial-gradient(1200px 700px at 10% 10%, #ffe9d6 0%, transparent 55%),
                    radial-gradient(1000px 600px at 90% 20%, #e5f2e7 0%, transparent 55%),
                    linear-gradient(180deg, #fdfaf2 0%, #f3efe4 100%);
        min-height: 100vh;
      }
      header {
        padding: 28px 20px 10px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 16px;
      }
      .title {
        font-size: 26px;
        font-weight: 600;
        letter-spacing: 0.5px;
      }
      .badge {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 12px;
        color: #1b3b35;
        background: #d7efe6;
        padding: 6px 10px;
        border-radius: 999px;
      }
      main {
        max-width: 980px;
        margin: 0 auto;
        padding: 10px 20px 40px;
        display: grid;
        grid-template-columns: 1.2fr 0.8fr;
        gap: 18px;
      }
      .panel {
        background: var(--card);
        border: 1px solid var(--line);
        border-radius: 18px;
        box-shadow: var(--shadow);
        padding: 16px;
      }
      .chat {
        display: flex;
        flex-direction: column;
        min-height: 520px;
      }
      .messages {
        flex: 1;
        overflow-y: auto;
        padding: 10px 6px 18px;
        display: flex;
        flex-direction: column;
        gap: 12px;
      }
      .msg {
        max-width: 80%;
        padding: 12px 14px;
        border-radius: 16px;
        border: 1px solid #e3e0d6;
        background: var(--card-2);
        box-shadow: 0 10px 22px rgba(30, 30, 30, 0.05);
        animation: rise 0.35s ease;
        white-space: pre-wrap;
      }
      .msg.user {
        align-self: flex-end;
        background: #fff1e3;
        border-color: #f2d5bf;
      }
      .msg.bot {
        align-self: flex-start;
        background: #f1f7f3;
        border-color: #d6e5dc;
      }
      .meta {
        color: var(--muted);
        font-size: 12px;
        margin-top: 6px;
      }
      .composer {
        border-top: 1px solid var(--line);
        padding-top: 12px;
        display: grid;
        gap: 10px;
      }
      textarea {
        width: 100%;
        min-height: 90px;
        border-radius: 14px;
        border: 1px solid #d8d1c3;
        padding: 12px;
        font-family: inherit;
        resize: vertical;
        background: #fffdf8;
      }
      button {
        border: none;
        border-radius: 12px;
        padding: 10px 14px;
        font-weight: 600;
        cursor: pointer;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
      }
      button:hover { transform: translateY(-1px); }
      .btn-primary {
        background: var(--accent);
        color: white;
        box-shadow: 0 10px 18px rgba(224, 122, 63, 0.25);
      }
      .btn-secondary {
        background: #e8efe9;
        color: #1c3c35;
      }
      .btn-ghost {
        background: transparent;
        border: 1px dashed #c7c0b2;
        color: var(--muted);
      }
      .field {
        display: grid;
        gap: 6px;
        font-size: 13px;
      }
      input, select {
        border-radius: 10px;
        border: 1px solid #d8d1c3;
        padding: 8px;
        font-family: 'IBM Plex Mono', monospace;
        background: #fffdf8;
      }
      .stack {
        display: grid;
        gap: 10px;
      }
      .row {
        display: flex;
        gap: 10px;
        align-items: center;
      }
      .row > * { flex: 1; }
      .hint {
        font-size: 12px;
        color: var(--muted);
        line-height: 1.4;
      }
      .status {
        padding: 8px 10px;
        background: #fef6ed;
        border: 1px solid #f1d4bd;
        border-radius: 12px;
        font-size: 12px;
        color: #7a3e18;
      }
      @keyframes rise {
        from { opacity: 0; transform: translateY(8px); }
        to { opacity: 1; transform: translateY(0); }
      }
      @media (max-width: 860px) {
        main { grid-template-columns: 1fr; }
      }
    </style>
  </head>
  <body>
    <header>
      <div class="title">Football Predictor</div>
      <div class="badge">Private Chat UI</div>
    </header>
    <main>
      <section class="panel chat">
        <div class="messages" id="messages"></div>
        <div class="composer">
          <textarea id="query" placeholder="Scrivi la tua richiesta..."></textarea>
        <div class="row">
            <button class="btn-primary" id="sendBtn">Send</button>
            <button class="btn-secondary" id="clearBtn">Clear</button>
            <button class="btn-ghost" id="newChatBtn">New chat</button>
          </div>
          <div class="hint">Tip: chiedi analisi partita, schedine, assenze o previsioni per oggi.</div>
        </div>
      </section>
      <aside class="panel stack">
        <div class="status" id="status">Ready</div>
        <div class="field">
          <label for="token">Access token</label>
          <input id="token" type="password" placeholder="Set token for this browser">
          <button class="btn-ghost" id="saveToken">Save token</button>
        </div>
        <div class="field">
          <label>Advanced settings</label>
          <div class="row">
            <input id="nSims" type="number" min="1000" step="1000" value="50000">
            <input id="seed" type="number" min="0" step="1" value="42">
          </div>
          <input id="bankroll" type="number" min="1" step="10" value="1000">
        </div>
        <div class="field">
          <label>Training feedback</label>
          <select id="label">
            <option value="good">good</option>
            <option value="bad">bad</option>
            <option value="note">note</option>
          </select>
          <input id="notes" type="text" placeholder="Optional notes">
          <button class="btn-secondary" id="saveFeedback">Save feedback</button>
          <div class="hint">Feedback is stored locally and can be exported later for training.</div>
        </div>
      </aside>
    </main>

    <script>
      const messages = document.getElementById('messages');
      const statusEl = document.getElementById('status');
      const queryEl = document.getElementById('query');
      const tokenEl = document.getElementById('token');
      const nSimsEl = document.getElementById('nSims');
      const seedEl = document.getElementById('seed');
      const bankrollEl = document.getElementById('bankroll');
      const labelEl = document.getElementById('label');
      const notesEl = document.getElementById('notes');

      let lastExchange = null;
      let sessionId = null;

      function setStatus(text, kind) {
        statusEl.textContent = text;
        statusEl.style.background = kind === 'error' ? '#ffe7e0' : '#fef6ed';
        statusEl.style.borderColor = kind === 'error' ? '#f3b5a2' : '#f1d4bd';
        statusEl.style.color = kind === 'error' ? '#7a2b20' : '#7a3e18';
      }

      function addMessage(role, text) {
        const msg = document.createElement('div');
        msg.className = 'msg ' + role;
        msg.textContent = text;
        messages.appendChild(msg);
        messages.scrollTop = messages.scrollHeight;
      }

      function getToken() {
        const urlParams = new URLSearchParams(window.location.search);
        const urlToken = urlParams.get('token');
        if (urlToken) {
          localStorage.setItem('chat_ui_token', urlToken);
          const cleanUrl = window.location.pathname;
          history.replaceState({}, '', cleanUrl);
        }
        return localStorage.getItem('chat_ui_token') || '';
      }

      function getSessionId() {
        let id = localStorage.getItem('chat_session_id');
        if (!id) {
          if (crypto.randomUUID) {
            id = crypto.randomUUID();
          } else {
            id = 'sess_' + Math.random().toString(36).slice(2);
          }
          localStorage.setItem('chat_session_id', id);
        }
        return id;
      }

      function resetSession() {
        sessionId = getSessionId();
      }

      tokenEl.value = getToken();
      sessionId = getSessionId();

      document.getElementById('saveToken').addEventListener('click', () => {
        localStorage.setItem('chat_ui_token', tokenEl.value.trim());
        setStatus('Token saved', 'ok');
      });

      document.getElementById('clearBtn').addEventListener('click', () => {
        messages.innerHTML = '';
        lastExchange = null;
        setStatus('Cleared', 'ok');
      });

      document.getElementById('newChatBtn').addEventListener('click', () => {
        localStorage.removeItem('chat_session_id');
        sessionId = getSessionId();
        messages.innerHTML = '';
        lastExchange = null;
        setStatus('New chat started', 'ok');
      });

      document.getElementById('sendBtn').addEventListener('click', async () => {
        const query = queryEl.value.trim();
        if (!query) return;
        const token = getToken();
        if (!token) {
          setStatus('Missing token', 'error');
          return;
        }
        addMessage('user', query);
        queryEl.value = '';
        setStatus('Running...', 'ok');

        const payload = {
          query: query,
          n_sims: parseInt(nSimsEl.value || '50000', 10),
          seed: parseInt(seedEl.value || '42', 10),
          bankroll: parseFloat(bankrollEl.value || '1000'),
          session_id: sessionId
        };

        try {
          const res = await fetch('/v1/chat-ui', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Chat-Token': token },
            body: JSON.stringify(payload)
          });
          if (!res.ok) {
            throw new Error('HTTP ' + res.status);
          }
          const data = await res.json();
          addMessage('bot', data.answer || 'No answer');
          if (data.warnings && data.warnings.length) {
            addMessage('bot', 'Warnings: ' + data.warnings.join(', '));
          }
          lastExchange = { query: query, response: data.answer || '', meta: payload };
          setStatus('Ready', 'ok');
        } catch (err) {
          addMessage('bot', 'Error: ' + err.message);
          setStatus('Request failed', 'error');
        }
      });

      document.getElementById('saveFeedback').addEventListener('click', async () => {
        if (!lastExchange) {
          setStatus('No response to label', 'error');
          return;
        }
        const token = getToken();
        if (!token) {
          setStatus('Missing token', 'error');
          return;
        }
        const payload = {
          query: lastExchange.query,
          response: lastExchange.response,
          label: labelEl.value,
          notes: notesEl.value.trim() || null,
          match_id: null,
          meta: lastExchange.meta
        };
        try {
          const res = await fetch('/v1/chat-feedback', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Chat-Token': token },
            body: JSON.stringify(payload)
          });
          if (!res.ok) {
            throw new Error('HTTP ' + res.status);
          }
          setStatus('Feedback saved', 'ok');
        } catch (err) {
          setStatus('Feedback failed: ' + err.message, 'error');
        }
      });
    </script>
  </body>
</html>
"""
