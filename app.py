#!/usr/bin/env python3
"""
Break Queue Bot v2
──────────────────
• Employees use /break slash command
• Auto-approve with queue (1 person on break at a time)
• Fixed 15-minute breaks
• "I'm Back" button when break ends
• 2-minute window to accept queued turn
• Daily reset at midnight
• Real-time detailed DMs to manager
• Manager commands: /setlimit N  /resetbreaks  /breakstatus
"""

import os, sqlite3, threading, time, schedule
from datetime import datetime
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# ── Config ────────────────────────────────────────────────────────────────────
BREAK_CHANNEL_ID    = os.environ["BREAK_CHANNEL_ID"]
MANAGER_USER_ID     = os.environ["MANAGER_USER_ID"]
BREAK_DURATION_SECS = 15 * 60
QUEUE_TIMEOUT_SECS  = 120
DB_PATH             = "breaks.db"

app = App(token=os.environ["SLACK_BOT_TOKEN"])
active_timers: dict = {}

# ── Database ──────────────────────────────────────────────────────────────────
def init_db():
    with sqlite3.connect(DB_PATH) as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS breaks (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id    TEXT    NOT NULL,
                status         TEXT    NOT NULL DEFAULT 'queued',
                started_at     REAL,
                ended_at       REAL,
                duration_sec   REAL,
                created_at     REAL    NOT NULL DEFAULT (unixepoch()),
                notified_at    REAL,
                channel_msg_ts TEXT
            );
            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT OR IGNORE INTO config VALUES ('daily_limit', '2');
        """)

def cfg(key):
    with sqlite3.connect(DB_PATH) as c:
        row = c.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
        return row[0] if row else ""

def set_cfg(key, value):
    with sqlite3.connect(DB_PATH) as c:
        c.execute("INSERT OR REPLACE INTO config VALUES (?,?)", (key, value))
        c.commit()

def q(sql, *args, one=False):
    with sqlite3.connect(DB_PATH) as c:
        c.row_factory = sqlite3.Row
        cur = c.execute(sql, args)
        return cur.fetchone() if one else cur.fetchall()

def run(sql, *args):
    with sqlite3.connect(DB_PATH) as c:
        c.execute(sql, args)
        c.commit()

def last_id():
    with sqlite3.connect(DB_PATH) as c:
        return c.execute("SELECT last_insert_rowid()").fetchone()[0]

# ── Helpers ───────────────────────────────────────────────────────────────────
def username(uid):
    try:
        r = app.client.users_info(user=uid)
        u = r["user"]
        return u.get("real_name") or u.get("display_name") or u["name"]
    except Exception:
        return f"<@{uid}>"

def fmt_dur(secs):
    secs = int(secs)
    m, s = divmod(secs, 60)
    return f"{m} min {s} sec" if m else f"{s} sec"

def now_str():
    return datetime.now().strftime("%I:%M %p")

def breaks_today(uid):
    midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    rows = q(
        "SELECT id FROM breaks WHERE employee_id=? AND created_at>=? "
        "AND status NOT IN ('forfeited','denied')",
        uid, midnight
    )
    return len(rows)

def active_break():
    return q("SELECT * FROM breaks WHERE status='on_break'", one=True)

def next_queued():
    return q(
        "SELECT * FROM breaks WHERE status='queued' ORDER BY id LIMIT 1",
        one=True
    )

def queue_count():
    return len(q(
        "SELECT id FROM breaks WHERE status IN ('queued','notified')"
    ))

# ── DM + Channel helpers ──────────────────────────────────────────────────────
def dm(text):
    try:
        app.client.chat_postMessage(
            channel=MANAGER_USER_ID, text=text, mrkdwn=True
        )
    except Exception as e:
        print(f"[DM error] {e}")

def post(text, blocks=None):
    kw = {"channel": BREAK_CHANNEL_ID, "text": text}
    if blocks:
        kw["blocks"] = blocks
    try:
        r = app.client.chat_postMessage(**kw)
        return r["ts"]
    except Exception as e:
        err = str(e)
        print(f"[post error] {err}")
        try:
            app.client.chat_postMessage(
                channel=MANAGER_USER_ID,
                text=f"⚠️ *Bot channel post failed:* `{err}`\nMake sure the bot is invited: `/invite @Break Bot` in the channel."
            )
        except Exception:
            pass
        return None

def update_msg(ts, text, blocks=None):
    kw = {"channel": BREAK_CHANNEL_ID, "ts": ts, "text": text}
    if blocks:
        kw["blocks"] = blocks
    try:
        app.client.chat_update(**kw)
    except Exception as e:
        print(f"[update error] {e}")

def ephemeral(user, text):
    app.client.chat_postEphemeral(
        channel=BREAK_CHANNEL_ID, user=user, text=text
    )

# ── Break lifecycle ───────────────────────────────────────────────────────────
def start_break(brk_id):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        return
    run("UPDATE breaks SET status='on_break', started_at=unixepoch() WHERE id=?", brk_id)
    name = username(brk["employee_id"])

    ts = post(
        f"🟡 {name} is on break!",
        blocks=[{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"🟡 *{name}* is now on a *15-minute break* _(started {now_str()})_\n"
                    f"⏳ They'll be notified when time is up."
                )
            }
        }]
    )
    if ts:
        run("UPDATE breaks SET channel_msg_ts=? WHERE id=?", ts, brk_id)

    used = breaks_today(brk["employee_id"])
    qc = queue_count()
    dm(
        f"🟡 *Break Started*\n"
        f"  • *Employee:* {name}\n"
        f"  • *Time:* {now_str()}\n"
        f"  • *Breaks used today:* {used}/{cfg('daily_limit')}\n"
        f"  • *Queue behind them:* {qc} person(s)"
    )

    t = threading.Timer(BREAK_DURATION_SECS, end_break, args=[brk_id])
    t.daemon = True
    t.start()
    active_timers[brk_id] = t


def end_break(brk_id):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk or brk["status"] != "on_break":
        return
    active_timers.pop(brk_id, None)
    name = username(brk["employee_id"])

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"⏰ *{name}*, your 15-minute break is over!\n"
                    f"Please click the button below when you're back. 👇"
                )
            }
        },
        {
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "✅ I'm Back!"},
                "style": "primary",
                "action_id": "im_back",
                "value": str(brk_id)
            }]
        }
    ]

    if brk["channel_msg_ts"]:
        update_msg(brk["channel_msg_ts"], f"⏰ {name}, your break is over!", blocks=blocks)
    else:
        post(f"⏰ {name}, break time's up!", blocks=blocks)

    dm(
        f"⏰ *Break Timer Ended*\n"
        f"  • *Employee:* {name}\n"
        f"  • *Waiting for them to click 'I'm Back'*\n"
        f"  • *Time:* {now_str()}"
    )


def complete_break(brk_id, clicked_by):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        return
    if brk["employee_id"] != clicked_by:
        ephemeral(clicked_by, "⚠️ Only the person on break can click this.")
        return
    if brk["status"] not in ("on_break",):
        ephemeral(clicked_by, "⚠️ This break is no longer active.")
        return

    ended = time.time()
    started = brk["started_at"] or ended
    duration = ended - started
    run(
        "UPDATE breaks SET status='completed', ended_at=?, duration_sec=? WHERE id=?",
        ended, duration, brk_id
    )

    name = username(brk["employee_id"])
    dur_str = fmt_dur(duration)

    if brk["channel_msg_ts"]:
        update_msg(
            brk["channel_msg_ts"],
            f"✅ {name} is back!",
            blocks=[{
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"✅ *{name}* is back after *{dur_str}*. 👋"}
            }]
        )

    used = breaks_today(brk["employee_id"])
    dm(
        f"✅ *Employee Returned*\n"
        f"  • *Employee:* {name}\n"
        f"  • *Actual break time:* {dur_str}\n"
        f"  • *Breaks used today:* {used}/{cfg('daily_limit')}\n"
        f"  • *Returned at:* {now_str()}"
    )

    promote_queue()


def notify_next(brk_id):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        return
    run(
        "UPDATE breaks SET status='notified', notified_at=unixepoch() WHERE id=?",
        brk_id
    )
    name = username(brk["employee_id"])

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"🟢 It's your turn, *{name}*!\n"
                    f"Click *Start My Break* within *2 minutes* or your spot will be given away. ⏳"
                )
            }
        },
        {
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "▶️ Start My Break"},
                "style": "primary",
                "action_id": "start_queued_break",
                "value": str(brk_id)
            }]
        }
    ]
    ts = post(f"🟢 {name}, it's your turn!", blocks=blocks)
    if ts:
        run("UPDATE breaks SET channel_msg_ts=? WHERE id=?", ts, brk_id)

    dm(
        f"🔔 *Queue: Next Person Notified*\n"
        f"  • *Employee:* {name}\n"
        f"  • *Has 2 minutes to accept*\n"
        f"  • *Time:* {now_str()}"
    )

    t = threading.Timer(QUEUE_TIMEOUT_SECS, forfeit_spot, args=[brk_id])
    t.daemon = True
    t.start()
    active_timers[brk_id] = t


def forfeit_spot(brk_id):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk or brk["status"] != "notified":
        return
    active_timers.pop(brk_id, None)
    run("UPDATE breaks SET status='forfeited' WHERE id=?", brk_id)
    name = username(brk["employee_id"])

    if brk["channel_msg_ts"]:
        update_msg(
            brk["channel_msg_ts"],
            f"❌ {name} didn't respond in time.",
            blocks=[{
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"❌ *{name}* didn't respond within 2 minutes and lost their queue spot."
                }
            }]
        )

    dm(
        f"❌ *Queue Spot Forfeited*\n"
        f"  • *Employee:* {name}\n"
        f"  • *Reason:* No response within 2 minutes\n"
        f"  • *Time:* {now_str()}"
    )
    promote_queue()


def promote_queue():
    if active_break():
        return
    nxt = next_queued()
    if nxt:
        notify_next(nxt["id"])
    else:
        post("✅ Break slot is open! Type `/break` to request one.")


# ── Slash Commands ────────────────────────────────────────────────────────────
@app.command("/break")
def handle_break(ack, body):
    ack()
    user = body["user_id"]

    if body.get("channel_id") != BREAK_CHANNEL_ID:
        app.client.chat_postEphemeral(
            channel=body["channel_id"],
            user=user,
            text=f"⚠️ Please use `/break` in <#{BREAK_CHANNEL_ID}>."
        )
        return

    limit = int(cfg("daily_limit"))
    used = breaks_today(user)
    if used >= limit:
        ephemeral(
            user,
            f"⛔ You've used all *{limit}* of your breaks for today. They reset at midnight! 🌙"
        )
        return

    existing = q(
        "SELECT id FROM breaks WHERE employee_id=? AND status IN ('queued','notified','on_break')",
        user, one=True
    )
    if existing:
        ephemeral(user, "⚠️ You already have an active break request or are in the queue!")
        return

    name = username(user)
    run("INSERT INTO breaks (employee_id, status) VALUES (?,?)", user, "queued")
    bid = last_id()

    active = active_break()
    qc = queue_count() - 1  # subtract self

    if active is None and qc == 0:
        dm(
            f"📥 *Break Requested*\n"
            f"  • *Employee:* {name}\n"
            f"  • *Slot:* Free — notifying now\n"
            f"  • *Breaks used today:* {used + 1}/{limit}\n"
            f"  • *Time:* {now_str()}"
        )
        notify_next(bid)
    else:
        pos = (1 if active else 0) + qc + 1
        ephemeral(
            user,
            f"⏳ Added to the queue! You're *#{pos}* in line. I'll ping you when it's your turn. 🎯"
        )
        dm(
            f"📥 *Break Requested (Queued)*\n"
            f"  • *Employee:* {name}\n"
            f"  • *Queue position:* #{pos}\n"
            f"  • *Breaks used today:* {used + 1}/{limit}\n"
            f"  • *Time:* {now_str()}"
        )


@app.command("/setlimit")
def handle_setlimit(ack, body):
    ack()
    user = body["user_id"]
    if user != MANAGER_USER_ID:
        app.client.chat_postEphemeral(
            channel=body["channel_id"], user=user,
            text="⛔ Only the manager can change the break limit."
        )
        return
    text = body.get("text", "").strip()
    if not text.isdigit() or int(text) < 1:
        app.client.chat_postEphemeral(
            channel=body["channel_id"], user=user,
            text="Usage: `/setlimit 3`  — sets the daily break limit per employee."
        )
        return
    set_cfg("daily_limit", text)
    post(f"📢 Daily break limit updated to *{text} break(s) per employee* per day.")
    dm(f"✏️ You updated the daily break limit to *{text}/person/day* at {now_str()}.")


@app.command("/resetbreaks")
def handle_reset(ack, body):
    ack()
    user = body["user_id"]
    if user != MANAGER_USER_ID:
        app.client.chat_postEphemeral(
            channel=body["channel_id"], user=user,
            text="⛔ Only the manager can reset breaks."
        )
        return
    midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    run(
        "DELETE FROM breaks WHERE status IN ('queued','forfeited','completed','denied') "
        "AND created_at >= ?", midnight
    )
    post("🔄 Break counts have been manually reset by the manager. Everyone starts fresh!")
    dm(f"🔄 You manually reset all break counts at {now_str()}.")


@app.command("/clearqueue")
def handle_clearqueue(ack, body):
    ack()
    user = body["user_id"]
    if user != MANAGER_USER_ID:
        app.client.chat_postEphemeral(
            channel=body["channel_id"], user=user,
            text="⛔ Only the manager can use this command."
        )
        return
    # Cancel all active timers
    for t in list(active_timers.values()):
        try:
            t.cancel()
        except Exception:
            pass
    active_timers.clear()
    # Clear all active/stuck breaks
    run(
        "UPDATE breaks SET status='cancelled' WHERE status IN ('queued','notified','on_break')"
    )
    app.client.chat_postEphemeral(
        channel=body["channel_id"], user=user,
        text="🧹 All active breaks and queue entries have been cleared!"
    )
    try:
        post("🧹 Break queue has been cleared by the manager. Use `/break` to start fresh!")
    except Exception as e:
        print(f"[clearqueue post error] {e}")
    dm(f"🧹 You cleared the entire break queue at {now_str()}.")


@app.command("/mybreakid")
def handle_mybreakid(ack, body):
    """Debug: shows the channel ID where command was typed."""
    ack()
    user = body["user_id"]
    channel = body["channel_id"]
    app.client.chat_postEphemeral(
        channel=channel, user=user,
        text=(
            f"🔍 *Debug Info*\n"
            f"  • This channel ID: `{channel}`\n"
            f"  • Configured BREAK_CHANNEL_ID: `{BREAK_CHANNEL_ID}`\n"
            f"  • Match: {'✅ Yes' if channel == BREAK_CHANNEL_ID else '❌ No — update your Railway variable!'}"
        )
    )


@app.command("/breakstatus")
def handle_status(ack, body):
    ack()
    user = body["user_id"]
    if user != MANAGER_USER_ID:
        app.client.chat_postEphemeral(
            channel=body["channel_id"], user=user,
            text="⛔ Only the manager can view the full break status."
        )
        return

    active = active_break()
    queued = q(
        "SELECT * FROM breaks WHERE status IN ('queued','notified') ORDER BY id"
    )
    midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    today_rows = q(
        "SELECT employee_id, COUNT(*) as cnt FROM breaks "
        "WHERE created_at>=? AND status NOT IN ('forfeited','denied') "
        "GROUP BY employee_id",
        midnight
    )
    limit = cfg("daily_limit")
    lines = [f"📊 *Break Dashboard — {datetime.now().strftime('%b %d, %Y %I:%M %p')}*\n"]

    if active:
        elapsed = int(time.time() - (active["started_at"] or time.time()))
        remaining = max(0, BREAK_DURATION_SECS - elapsed)
        lines.append(
            f"🟡 *On Break:* {username(active['employee_id'])} "
            f"— _{fmt_dur(remaining)} remaining_"
        )
    else:
        lines.append("🟢 *Break Slot:* Available")

    if queued:
        lines.append(f"\n👥 *Queue ({len(queued)}):*")
        for i, bq in enumerate(queued, 1):
            icon = "🔔" if bq["status"] == "notified" else "⏳"
            lines.append(f"  {icon} #{i}: {username(bq['employee_id'])}")
    else:
        lines.append("\n👥 *Queue:* Empty")

    if today_rows:
        lines.append(f"\n📅 *Today's Breaks (limit {limit}/person):*")
        for row in today_rows:
            bar = "🟩" * int(row["cnt"]) + "⬜" * max(0, int(limit) - int(row["cnt"]))
            lines.append(f"  {bar} {username(row['employee_id'])}: {row['cnt']}/{limit}")
    else:
        lines.append(f"\n📅 *Today's Breaks:* None yet (limit {limit}/person)")

    app.client.chat_postEphemeral(
        channel=body["channel_id"],
        user=user,
        text="\n".join(lines),
        mrkdwn=True
    )


# ── Button actions ────────────────────────────────────────────────────────────
@app.action("start_queued_break")
def handle_start_queued(ack, body, action):
    ack()
    user = body["user"]["id"]
    brk_id = int(action["value"])
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        ephemeral(user, "⚠️ Break not found.")
        return
    if brk["employee_id"] != user:
        ephemeral(user, "⚠️ This isn't your break notification.")
        return
    if brk["status"] != "notified":
        ephemeral(user, "⚠️ This break is no longer available.")
        return
    t = active_timers.pop(brk_id, None)
    if t:
        t.cancel()
    run("UPDATE breaks SET status='queued' WHERE id=?", brk_id)
    start_break(brk_id)


@app.action("im_back")
def handle_im_back(ack, body, action):
    ack()
    user = body["user"]["id"]
    brk_id = int(action["value"])
    complete_break(brk_id, user)


# ── Midnight scheduler ────────────────────────────────────────────────────────
def midnight_reset():
    try:
        post(
            f"🌅 Good morning! Break counts have reset.\n"
            f"Each employee gets *{cfg('daily_limit')} break(s)* today. Type `/break` to use one!"
        )
        dm(
            f"🌅 *Daily Reset*\n"
            f"  • Break counts reset at midnight\n"
            f"  • Current limit: *{cfg('daily_limit')}/person/day*"
        )
    except Exception as e:
        print(f"[midnight reset error] {e}")

def run_scheduler():
    schedule.every().day.at("00:00").do(midnight_reset)
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    init_db()
    threading.Thread(target=run_scheduler, daemon=True).start()
    print("🚀 Break Queue Bot v2 running...")
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
