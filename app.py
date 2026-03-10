#!/usr/bin/env python3
"""
Break Queue Bot v3
──────────────────
• Minutes-based daily allowance (e.g. 60 mins/day total)
• Employee types /break 15  (requests 15 mins)
• Channel posts are minimal — just tag the person + buttons, no duration shown
• Manager gets full details via DM
• Employee gets private ephemeral showing their remaining minutes
• "End Break Early" button on active break message
• "I'm Back" button when timer ends
• 2-min window to accept queued turn
• Daily reset at midnight
• Manager commands: /setminutes  /resetbreaks  /clearqueue  /breakstatus
"""

import os, sqlite3, threading, time, schedule
from datetime import datetime
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# ── Config ────────────────────────────────────────────────────────────────────
BREAK_CHANNEL_ID   = os.environ["BREAK_CHANNEL_ID"]
MANAGER_USER_ID    = os.environ["MANAGER_USER_ID"]
QUEUE_TIMEOUT_SECS = 120
DB_PATH            = "breaks.db"

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
                requested_mins INTEGER NOT NULL DEFAULT 15,
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
            INSERT OR IGNORE INTO config VALUES ('daily_minutes', '60');
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
        cur = c.execute(sql, args)
        c.commit()
        return cur.lastrowid

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

def minutes_used_today(uid):
    """Total completed + active break minutes used today."""
    midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    rows = q(
        "SELECT requested_mins, duration_sec, status FROM breaks "
        "WHERE employee_id=? AND created_at>=? AND status NOT IN ('forfeited','cancelled','denied')",
        uid, midnight
    )
    total = 0
    for r in rows:
        if r["status"] == "completed" and r["duration_sec"]:
            total += r["duration_sec"] / 60
        elif r["status"] in ("on_break", "queued", "notified"):
            total += r["requested_mins"]
    return total

def minutes_remaining_today(uid):
    limit = int(cfg("daily_minutes"))
    used = minutes_used_today(uid)
    return max(0, limit - used)

def active_break():
    return q("SELECT * FROM breaks WHERE status='on_break'", one=True)

def next_queued():
    return q("SELECT * FROM breaks WHERE status='queued' ORDER BY id LIMIT 1", one=True)

def queue_count():
    return len(q("SELECT id FROM breaks WHERE status IN ('queued','notified')"))

# ── Messaging helpers ─────────────────────────────────────────────────────────
def dm(text):
    try:
        app.client.chat_postMessage(channel=MANAGER_USER_ID, text=text, mrkdwn=True)
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
        print(f"[post error] {e}")
        dm(f"⚠️ *Bot failed to post to channel:* `{e}`\nMake sure bot is invited: `/invite @Break Bot`")
        return None

def update_msg(ts, text, blocks=None):
    kw = {"channel": BREAK_CHANNEL_ID, "ts": ts, "text": text}
    if blocks:
        kw["blocks"] = blocks
    try:
        app.client.chat_update(**kw)
    except Exception as e:
        print(f"[update error] {e}")

def ephemeral(user, text, mrkdwn=True):
    try:
        app.client.chat_postEphemeral(
            channel=BREAK_CHANNEL_ID, user=user, text=text, mrkdwn=mrkdwn
        )
    except Exception as e:
        print(f"[ephemeral error] {e}")

# ── Break lifecycle ───────────────────────────────────────────────────────────
def start_break(brk_id):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        return
    run("UPDATE breaks SET status='on_break', started_at=unixepoch() WHERE id=?", brk_id)
    uid  = brk["employee_id"]
    name = username(uid)
    mins = brk["requested_mins"]

    # ── Channel message: minimal, just tag + end early button ──
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"🟡 <@{uid}> is currently on a break."
            }
        },
        {
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "🔚 End Break Early"},
                "style": "danger",
                "action_id": "end_early",
                "value": str(brk_id)
            }]
        }
    ]
    ts = post(f"🟡 <@{uid}> is on a break.", blocks=blocks)
    if ts:
        run("UPDATE breaks SET channel_msg_ts=? WHERE id=?", ts, brk_id)

    # ── Private message to employee only ──
    remaining = minutes_remaining_today(uid)
    ephemeral(
        uid,
        f"✅ Your break has started!\n"
        f"  • *Duration:* {mins} min\n"
        f"  • *Minutes remaining after this:* {remaining - mins:.0f} min\n"
        f"You'll be tagged here when time is up. 🔔"
    )

    # ── DM manager with full details ──
    qc = queue_count()
    dm(
        f"🟡 *Break Started*\n"
        f"  • *Employee:* {name}\n"
        f"  • *Duration:* {mins} min\n"
        f"  • *Time:* {now_str()}\n"
        f"  • *Minutes used today:* {minutes_used_today(uid):.0f}/{cfg('daily_minutes')}\n"
        f"  • *Queue:* {qc} person(s) waiting"
    )

    t = threading.Timer(mins * 60, end_break, args=[brk_id])
    t.daemon = True
    t.start()
    active_timers[brk_id] = t


def end_break(brk_id, early=False):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk or brk["status"] != "on_break":
        return
    active_timers.pop(brk_id, None)
    uid  = brk["employee_id"]
    name = username(uid)

    # ── Update channel message: tag person + I'm Back button ──
    label = "ended their break early" if early else "break time is up"
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"⏰ <@{uid}> — time to head back! Click the button below. 👇"
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
        update_msg(brk["channel_msg_ts"], f"⏰ <@{uid}> break time is up!", blocks=blocks)
    else:
        post(f"⏰ <@{uid}> break time is up!", blocks=blocks)

    dm(
        f"⏰ *Break Timer Ended{'(early)' if early else ''}*\n"
        f"  • *Employee:* {name}\n"
        f"  • *Waiting for 'I'm Back' click*\n"
        f"  • *Time:* {now_str()}"
    )


def complete_break(brk_id, clicked_by):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        return
    if brk["employee_id"] != clicked_by:
        ephemeral(clicked_by, "⚠️ Only the person on break can click this.")
        return
    if brk["status"] != "on_break":
        ephemeral(clicked_by, "⚠️ This break is no longer active.")
        return

    ended    = time.time()
    started  = brk["started_at"] or ended
    duration = ended - started
    run(
        "UPDATE breaks SET status='completed', ended_at=?, duration_sec=? WHERE id=?",
        ended, duration, brk_id
    )

    uid      = brk["employee_id"]
    name     = username(uid)
    dur_str  = fmt_dur(duration)
    remaining = minutes_remaining_today(uid)

    # ── Update channel message: clean minimal ──
    if brk["channel_msg_ts"]:
        update_msg(
            brk["channel_msg_ts"],
            f"✅ <@{uid}> is back!",
            blocks=[{
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"✅ <@{uid}> is back. 👋"}
            }]
        )

    # ── Private message to employee ──
    ephemeral(
        uid,
        f"👋 Welcome back!\n"
        f"  • *Break duration:* {dur_str}\n"
        f"  • *Minutes remaining today:* {remaining:.0f} min"
    )

    # ── DM manager ──
    dm(
        f"✅ *Employee Returned*\n"
        f"  • *Employee:* {name}\n"
        f"  • *Actual break time:* {dur_str}\n"
        f"  • *Minutes used today:* {minutes_used_today(uid):.0f}/{cfg('daily_minutes')}\n"
        f"  • *Minutes remaining today:* {remaining:.0f} min\n"
        f"  • *Returned at:* {now_str()}"
    )

    promote_queue()


def notify_next(brk_id):
    print(f"[notify_next] called for break_id={brk_id}")
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        print(f"[notify_next] break not found!")
        return
    run("UPDATE breaks SET status='notified', notified_at=unixepoch() WHERE id=?", brk_id)
    uid  = brk["employee_id"]
    name = username(uid)
    print(f"[notify_next] posting to channel for {name}")

    # ── Channel: tag them + Start button ──
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"🟢 <@{uid}> — it's your turn! Click below within *2 minutes* or your spot will be skipped. ⏳"
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
    ts = post(f"🟢 <@{uid}> it's your turn!", blocks=blocks)
    print(f"[notify_next] post returned ts={ts}")
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
    uid  = brk["employee_id"]
    name = username(uid)

    if brk["channel_msg_ts"]:
        update_msg(
            brk["channel_msg_ts"],
            f"❌ <@{uid}> didn't respond in time.",
            blocks=[{
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"❌ <@{uid}> didn't respond within 2 minutes — spot skipped."}
            }]
        )

    dm(
        f"❌ *Queue Spot Forfeited*\n"
        f"  • *Employee:* {name}\n"
        f"  • *Reason:* No response in 2 minutes\n"
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
        post("✅ Break slot is open! Type `/break [minutes]` to request one.")


# ── Slash command: /break [minutes] ──────────────────────────────────────────
@app.command("/break")
def handle_break(ack, body):
    ack()
    user    = body["user_id"]
    channel = body["channel_id"]

    if channel != BREAK_CHANNEL_ID:
        app.client.chat_postEphemeral(
            channel=channel, user=user,
            text=f"⚠️ Please use `/break` in <#{BREAK_CHANNEL_ID}>."
        )
        return

    # Parse minutes from command text, default to 15
    text = body.get("text", "").strip()
    if text.isdigit() and int(text) > 0:
        mins = int(text)
    elif text == "":
        mins = 15
    else:
        ephemeral(user, "Usage: `/break 15` — request a 15-minute break. Number must be a positive whole number.")
        return

    # Check remaining minutes
    remaining = minutes_remaining_today(user)
    if mins > remaining:
        ephemeral(
            user,
            f"⛔ You only have *{remaining:.0f} minutes* left today but requested *{mins} min*.\n"
            f"Try `/break {int(remaining)}` or less. Minutes reset at midnight! 🌙"
        )
        return

    # Check if already in queue
    existing = q(
        "SELECT id FROM breaks WHERE employee_id=? AND status IN ('queued','notified','on_break')",
        user, one=True
    )
    if existing:
        ephemeral(user, "⚠️ You already have an active break or are in the queue!")
        return

    name = username(user)
    print(f"[/break] user={user} name={name} mins={mins} active={active_break()} qc={queue_count()}")
    bid = run("INSERT INTO breaks (employee_id, status, requested_mins) VALUES (?,?,?)", user, "queued", mins)

    active = active_break()
    qc     = queue_count() - 1  # subtract self just inserted

    if active is None and qc == 0:
        # Slot is free — start immediately, no button needed
        run("UPDATE breaks SET status='queued' WHERE id=?", bid)
        dm(
            f"📥 *Break Started Immediately*\n"
            f"  • *Employee:* {name}\n"
            f"  • *Duration:* {mins} min\n"
            f"  • *Minutes used today:* {int(minutes_used_today(user))}/{cfg('daily_minutes')}\n"
            f"  • *Time:* {now_str()}"
        )
        start_break(bid)
    else:
        pos = (1 if active else 0) + qc + 1
        ephemeral(
            user,
            f"⏳ You're *#{pos}* in the queue for a *{mins}-min* break.\n"
            f"I'll tag you here when it's your turn! 🎯"
        )
        dm(
            f"📥 *Break Requested (Queued)*\n"
            f"  • *Employee:* {name}\n"
            f"  • *Duration:* {mins} min\n"
            f"  • *Queue position:* #{pos}\n"
            f"  • *Minutes used today:* {minutes_used_today(user):.0f}/{cfg('daily_minutes')}\n"
            f"  • *Time:* {now_str()}"
        )


# ── Slash command: /setminutes N ─────────────────────────────────────────────
@app.command("/setminutes")
def handle_setminutes(ack, body):
    ack()
    user = body["user_id"]
    if user != MANAGER_USER_ID:
        app.client.chat_postEphemeral(
            channel=body["channel_id"], user=user,
            text="⛔ Only the manager can change the break allowance."
        )
        return
    text = body.get("text", "").strip()
    if not text.isdigit() or int(text) < 1:
        app.client.chat_postEphemeral(
            channel=body["channel_id"], user=user,
            text="Usage: `/setminutes 60` — sets the daily break allowance to 60 minutes per employee."
        )
        return
    set_cfg("daily_minutes", text)
    post(f"📢 Daily break allowance updated to *{text} minutes per employee* per day.")
    dm(f"✏️ You updated the daily break allowance to *{text} min/person/day* at {now_str()}.")


# ── Slash command: /resetbreaks ───────────────────────────────────────────────
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
        "DELETE FROM breaks WHERE status NOT IN ('on_break') AND created_at >= ?", midnight
    )
    post("🔄 Break minutes have been manually reset by the manager. Everyone starts fresh!")
    dm(f"🔄 You manually reset all break minutes at {now_str()}.")


# ── Slash command: /clearqueue ────────────────────────────────────────────────
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
    for t in list(active_timers.values()):
        try:
            t.cancel()
        except Exception:
            pass
    active_timers.clear()
    run("UPDATE breaks SET status='cancelled' WHERE status IN ('queued','notified','on_break')")
    app.client.chat_postEphemeral(
        channel=body["channel_id"], user=user,
        text="🧹 All active breaks and queue entries cleared!"
    )
    post("🧹 Break queue cleared by manager. Use `/break [minutes]` to start fresh!")
    dm(f"🧹 You cleared the entire break queue at {now_str()}.")


# ── Slash command: /breakstatus ───────────────────────────────────────────────
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

    active  = active_break()
    queued  = q("SELECT * FROM breaks WHERE status IN ('queued','notified') ORDER BY id")
    midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    today_rows = q(
        "SELECT DISTINCT employee_id FROM breaks WHERE created_at>=? "
        "AND status NOT IN ('forfeited','cancelled','denied')",
        midnight
    )
    limit = cfg("daily_minutes")
    lines = [f"📊 *Break Dashboard — {datetime.now().strftime('%b %d, %Y %I:%M %p')}*\n"]

    if active:
        elapsed   = int(time.time() - (active["started_at"] or time.time()))
        remaining = max(0, active["requested_mins"] * 60 - elapsed)
        lines.append(f"🟡 *On Break:* <@{active['employee_id']}> — _{fmt_dur(remaining)} remaining_")
    else:
        lines.append("🟢 *Break Slot:* Available")

    if queued:
        lines.append(f"\n👥 *Queue ({len(queued)}):*")
        for i, bq in enumerate(queued, 1):
            icon = "🔔" if bq["status"] == "notified" else "⏳"
            lines.append(f"  {icon} #{i}: {username(bq['employee_id'])} ({bq['requested_mins']} min)")
    else:
        lines.append("\n👥 *Queue:* Empty")

    if today_rows:
        lines.append(f"\n📅 *Today's Usage (limit: {limit} min/person):*")
        for row in today_rows:
            used = minutes_used_today(row["employee_id"])
            rem  = max(0, int(limit) - used)
            pct  = min(int(used / int(limit) * 10), 10)
            bar  = "🟩" * pct + "⬜" * (10 - pct)
            lines.append(f"  {bar} {username(row['employee_id'])}: {used:.0f}/{limit} min ({rem:.0f} left)")
    else:
        lines.append(f"\n📅 *Today's Usage:* None yet (limit {limit} min/person)")

    app.client.chat_postEphemeral(
        channel=body["channel_id"], user=user,
        text="\n".join(lines), mrkdwn=True
    )


# ── Slash command: /mybreakid (debug) ────────────────────────────────────────
@app.command("/breakhelp")
def handle_help(ack, body):
    ack()
    user = body["user_id"]
    is_manager = user == MANAGER_USER_ID

    employee_cmds = (
        "*👤 Employee Commands:*\n"
        "  • `/break 15` — Start a 15-min break immediately (or join queue if someone's out)\n"
        "  • `/break 10` — Same but 10 mins (any number works)\n"
        "  • `/break` — Defaults to 15 min if no number given\n"
        "\n"
        "*🔘 Buttons (appear in channel):*\n"
        "  • *▶️ Start My Break* — Claim your queued turn (2-min window)\n"
        "  • *🔚 End Break Early* — End your break instantly, logs actual time\n"
        "  • *✅ I'm Back!* — Confirm you're back after timer ends\n"
    )

    manager_cmds = (
        "\n*🔐 Manager-Only Commands:*\n"
        "  • `/breakstatus` — Dashboard: who's out, queue, everyone's minutes today\n"
        "  • `/setminutes 60` — Set daily break allowance (in minutes) for all employees\n"
        "  • `/resetbreaks` — Wipe today's break minutes, everyone starts fresh\n"
        "  • `/clearqueue` — Force-clear all active breaks and queue entries\n"
        "  • `/breakhelp` — Show this help message\n"
        "\n"
        "*🗑 Commands you can delete in Slack API settings (not used):*\n"
        "  • `/mybreakid` — Was a debug tool, no longer needed\n"
        "  • `/setlimit` — Old count-based limit, replaced by `/setminutes`\n"
    )

    text = employee_cmds + (manager_cmds if is_manager else "")
    app.client.chat_postEphemeral(
        channel=body["channel_id"],
        user=user,
        text=text,
        mrkdwn=True
    )
    ack()
    user    = body["user_id"]
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


# ── Button: Start My Break (from queue) ──────────────────────────────────────
@app.action("start_queued_break")
def handle_start_queued(ack, body, action):
    ack()
    user   = body["user"]["id"]
    brk_id = int(action["value"])
    brk    = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
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


# ── Button: End Break Early ───────────────────────────────────────────────────
@app.action("end_early")
def handle_end_early(ack, body, action):
    ack()
    user   = body["user"]["id"]
    brk_id = int(action["value"])
    brk    = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        ephemeral(user, "⚠️ Break not found.")
        return
    if brk["employee_id"] != user:
        ephemeral(user, "⚠️ Only the person on break can end it early.")
        return
    if brk["status"] != "on_break":
        ephemeral(user, "⚠️ This break is no longer active.")
        return
    # Cancel the timer
    t = active_timers.pop(brk_id, None)
    if t:
        t.cancel()
    # Log it immediately — no I'm Back step needed
    ended    = time.time()
    started  = brk["started_at"] or ended
    duration = ended - started
    run(
        "UPDATE breaks SET status='completed', ended_at=?, duration_sec=? WHERE id=?",
        ended, duration, brk_id
    )
    uid      = brk["employee_id"]
    name     = username(uid)
    dur_str  = fmt_dur(duration)
    remaining = minutes_remaining_today(uid)

    if brk["channel_msg_ts"]:
        update_msg(
            brk["channel_msg_ts"],
            f"✅ <@{uid}> ended their break early and is back.",
            blocks=[{
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"✅ <@{uid}> is back early. 👋"}
            }]
        )
    ephemeral(uid,
        f"👋 Break ended early!\n"
        f"  • *Time taken:* {dur_str}\n"
        f"  • *Minutes remaining today:* {remaining:.0f} min"
    )
    dm(
        f"🔚 *Break Ended Early*\n"
        f"  • *Employee:* {name}\n"
        f"  • *Time taken:* {dur_str}\n"
        f"  • *Minutes used today:* {minutes_used_today(uid):.0f}/{cfg('daily_minutes')}\n"
        f"  • *Minutes remaining today:* {remaining:.0f} min\n"
        f"  • *Time:* {now_str()}"
    )
    promote_queue()


# ── Button: I'm Back ─────────────────────────────────────────────────────────
@app.action("im_back")
def handle_im_back(ack, body, action):
    ack()
    user   = body["user"]["id"]
    brk_id = int(action["value"])
    complete_break(brk_id, user)


# ── Midnight scheduler ────────────────────────────────────────────────────────
def midnight_reset():
    try:
        mins = cfg("daily_minutes")
        post(
            f"🌅 Good morning! Break minutes have reset.\n"
            f"Each employee has *{mins} minutes* of breaks available today. "
            f"Type `/break 15` to use some!"
        )
        dm(
            f"🌅 *Daily Reset*\n"
            f"  • Break minutes reset at midnight\n"
            f"  • Current allowance: *{mins} min/person/day*"
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
    print("🚀 Break Queue Bot v3 running...")
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
