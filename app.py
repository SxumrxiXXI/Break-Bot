#!/usr/bin/env python3
"""
Break Queue Bot v7
Changes:
- Multi-channel support: Uses BREAK_CHANNEL_IDS
- Database tracks 'channel_id' so the bot replies in the correct channel
- DMs only send to the PRIMARY manager
"""

import os, sqlite3, threading, time, schedule
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

LOCAL_TZ = ZoneInfo("Africa/Cairo")

# ── Config ────────────────────────────────────────────────────────────────────
# Parse comma-separated lists of IDs
BREAK_CHANNEL_IDS  = [cid.strip() for cid in os.environ.get("BREAK_CHANNEL_IDS", "").split(",") if cid.strip()]
MANAGER_USER_IDS   = [uid.strip() for uid in os.environ.get("MANAGER_USER_IDS", "").split(",") if uid.strip()]

QUEUE_TIMEOUT_SECS = 120
MAX_BREAK_MINS     = 15
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
                channel_msg_ts TEXT,
                return_status  TEXT,
                channel_id     TEXT
            );
            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT OR IGNORE INTO config VALUES ('daily_minutes', '60');
        """)
        # DB Upgrades
        try:
            c.execute("ALTER TABLE breaks ADD COLUMN return_status TEXT")
            c.commit()
        except Exception:
            pass
        try:
            c.execute("ALTER TABLE breaks ADD COLUMN channel_id TEXT")
            c.commit()
        except Exception:
            pass

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
    return datetime.now(LOCAL_TZ).strftime("%I:%M %p")

def return_time_str(mins):
    return (datetime.now(LOCAL_TZ) + timedelta(minutes=mins)).strftime("%I:%M %p")

def ordinal(n):
    return f"{n}{'th' if 11<=n<=13 else {1:'st',2:'nd',3:'rd'}.get(n%10,'th')}"

def minutes_used_today(uid, exclude_id=None):
    midnight = datetime.now(LOCAL_TZ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    rows = q(
        "SELECT id, requested_mins, duration_sec, status FROM breaks "
        "WHERE employee_id=? AND created_at>=? AND status NOT IN ('forfeited','cancelled','denied')",
        uid, midnight
    )
    total = 0.0
    for r in rows:
        if exclude_id and r["id"] == exclude_id:
            continue
        if r["status"] == "completed" and r["duration_sec"]:
            total += r["duration_sec"] / 60
        elif r["status"] in ("on_break", "queued", "notified"):
            total += r["requested_mins"]
    return total

def minutes_remaining_today(uid, exclude_id=None):
    limit = int(cfg("daily_minutes"))
    used  = minutes_used_today(uid, exclude_id=exclude_id)
    return max(0.0, limit - used)

def active_break():
    return q("SELECT * FROM breaks WHERE status='on_break'", one=True)

def next_queued():
    return q("SELECT * FROM breaks WHERE status='queued' ORDER BY id LIMIT 1", one=True)

def queue_count():
    return len(q("SELECT id FROM breaks WHERE status IN ('queued','notified')"))

def get_channel_for_break(brk):
    """Safely fetch channel_id from break row, falling back to the first allowed channel."""
    if brk and "channel_id" in brk.keys() and brk["channel_id"]:
        return brk["channel_id"]
    return BREAK_CHANNEL_IDS[0] if BREAK_CHANNEL_IDS else ""

# ── Messaging helpers ─────────────────────────────────────────────────────────
def dm(text):
    if not MANAGER_USER_IDS:
        return
    primary_mgr = MANAGER_USER_IDS[0]
    try:
        app.client.chat_postMessage(channel=primary_mgr, text=text, mrkdwn=True)
    except Exception as e:
        print(f"[DM error for primary manager {primary_mgr}] {e}")

def post(text, channel, blocks=None):
    kw = {"channel": channel, "text": text}
    if blocks:
        kw["blocks"] = blocks
    try:
        r = app.client.chat_postMessage(**kw)
        return r["ts"]
    except Exception as e:
        print(f"[post error in {channel}] {e}")
        dm(f"⚠️ *Bot failed to post to channel <#{channel}>:* `{e}`")
        return None

def update_msg(ts, text, channel, blocks=None):
    kw = {"channel": channel, "ts": ts, "text": text}
    if blocks:
        kw["blocks"] = blocks
    try:
        app.client.chat_update(**kw)
    except Exception as e:
        print(f"[update error in {channel}] {e}")

def ephemeral(user, text, channel):
    try:
        app.client.chat_postEphemeral(
            channel=channel, user=user, text=text, mrkdwn=True
        )
    except Exception as e:
        print(f"[ephemeral error to {user} in {channel}] {e}")

# ── Break lifecycle ───────────────────────────────────────────────────────────
def start_break(brk_id):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        return
    run("UPDATE breaks SET status='on_break', started_at=unixepoch() WHERE id=?", brk_id)
    uid  = brk["employee_id"]
    name = username(uid)
    mins = brk["requested_mins"]
    eta  = return_time_str(mins)
    chan = get_channel_for_break(brk)

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"🟡 <@{uid}> is on a break — estimated return *{eta}*"
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
    ts = post(f"🟡 <@{uid}> is on a break. Est. return: {eta}", channel=chan, blocks=blocks)
    if ts:
        run("UPDATE breaks SET channel_msg_ts=? WHERE id=?", ts, brk_id)

    remaining_after = minutes_remaining_today(uid, exclude_id=brk_id) - mins
    remaining_after = max(0, remaining_after)
    ephemeral(
        user=uid,
        text=(
            f"✅ *Your break has started!*\n"
            f"  • *Duration:* {mins} min\n"
            f"  • *Estimated return:* {eta}\n"
            f"  • *Minutes left for today after this break:* {remaining_after:.0f} min\n"
            f"You'll be tagged here when time is up. 🔔"
        ),
        channel=chan
    )

    qc = queue_count()
    dm(
        f"🟡 *Break Started*\n"
        f"  • *Employee:* {name}\n"
        f"  • *Duration:* {mins} min\n"
        f"  • *Est. return:* {eta}\n"
        f"  • *Time:* {now_str()}\n"
        f"  • *Minutes used today (incl. this):* {minutes_used_today(uid):.0f}/{cfg('daily_minutes')}\n"
        f"  • *Queue behind them:* {qc} person(s)"
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
    chan = get_channel_for_break(brk)

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"⏰ <@{uid}> — break time is up! Click below to confirm you're back. 👇"
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
        update_msg(brk["channel_msg_ts"], f"⏰ <@{uid}> break time is up!", channel=chan, blocks=blocks)
    else:
        post(f"⏰ <@{uid}> break time is up!", channel=chan, blocks=blocks)

    dm(
        f"⏰ *Break Timer Ended*\n"
        f"  • *Employee:* {username(uid)}\n"
        f"  • *Waiting for 'I'm Back' click*\n"
        f"  • *Time:* {now_str()}"
    )

    t = threading.Timer(60, remind_employee_loop, args=[brk_id, 1])
    t.daemon = True
    t.start()
    active_timers[f"remind_{brk_id}"] = t


def remind_employee_loop(brk_id, attempt):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk or brk["status"] != "on_break":
        active_timers.pop(f"remind_{brk_id}", None)
        return
    active_timers.pop(f"remind_{brk_id}", None)

    uid      = brk["employee_id"]
    overdue  = attempt
    chan     = get_channel_for_break(brk)

    try:
        app.client.chat_postMessage(
            channel=uid,
            text=(
                f"👋 You're *{overdue} minute{'s' if overdue > 1 else ''} overdue* on your break!\n\n"
                f"Please head to <#{chan}> and click *✅ I'm Back!* to let the team know you're back. 🙏"
            ),
            mrkdwn=True
        )
    except Exception as e:
        print(f"[remind_employee_loop DM error] {e}")

    if attempt == 1 or attempt % 5 == 0:
        dm(
            f"⚠️ *Employee Still Not Back*\n"
            f"  • *Employee:* {username(uid)}\n"
            f"  • *Overdue by:* {overdue} minute{'s' if overdue > 1 else ''}\n"
            f"  • *Time:* {now_str()}"
        )

    t = threading.Timer(60, remind_employee_loop, args=[brk_id, attempt + 1])
    t.daemon = True
    t.start()
    active_timers[f"remind_{brk_id}"] = t


def _finish_break(brk_id, uid, early=False):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk or brk["status"] != "on_break":
        return False

    remind_timer = active_timers.pop(f"remind_{brk_id}", None)
    if remind_timer:
        remind_timer.cancel()

    ended    = time.time()
    started  = brk["started_at"] or ended
    duration = ended - started
    scheduled_secs = brk["requested_mins"] * 60

    grace = 30
    if early or duration < scheduled_secs - grace:
        return_status = "early"
    elif duration > scheduled_secs + grace:
        return_status = "late"
    else:
        return_status = "on_time"

    run(
        "UPDATE breaks SET status='completed', ended_at=?, duration_sec=?, return_status=? WHERE id=?",
        ended, duration, return_status, brk_id
    )

    name      = username(uid)
    dur_str   = fmt_dur(duration)
    remaining = minutes_remaining_today(uid)
    chan      = get_channel_for_break(brk)

    status_icon = {"early": "🟢 Early", "late": "🔴 Late", "on_time": "✅ On time"}[return_status]

    if brk["channel_msg_ts"]:
        update_msg(
            brk["channel_msg_ts"],
            f"✅ <@{uid}> is back.",
            channel=chan,
            blocks=[{
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"✅ <@{uid}> is back. 👋"}
            }]
        )

    ephemeral(
        user=uid,
        text=(
            f"👋 *Welcome back!*\n"
            f"  • *Break time:* {dur_str} ({status_icon})\n"
            f"  • *Minutes remaining today:* {remaining:.0f} min"
        ),
        channel=chan
    )

    dm(
        f"{'🔚' if early else '✅'} *Break Completed {'(Early)' if early else ''}*\n"
        f"  • *Employee:* {name}\n"
        f"  • *Return status:* {status_icon}\n"
        f"  • *Scheduled:* {brk['requested_mins']} min\n"
        f"  • *Actual time:* {dur_str}\n"
        f"  • *Minutes used today:* {minutes_used_today(uid):.0f}/{cfg('daily_minutes')}\n"
        f"  • *Minutes remaining today:* {remaining:.0f} min\n"
        f"  • *Returned at:* {now_str()}"
    )
    return True


def complete_break(brk_id, clicked_by, channel_id):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        return
    if brk["employee_id"] != clicked_by:
        ephemeral(clicked_by, "⚠️ Only the person on break can click this.", channel_id)
        return
    if brk["status"] != "on_break":
        ephemeral(clicked_by, "⚠️ This break is no longer active.", channel_id)
        return
    if _finish_break(brk_id, clicked_by, early=False):
        promote_queue()


def notify_next(brk_id):
    brk = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        return
    run("UPDATE breaks SET status='notified', notified_at=unixepoch() WHERE id=?", brk_id)
    uid  = brk["employee_id"]
    name = username(uid)
    chan = get_channel_for_break(brk)

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"🟢 <@{uid}> — it's your turn! "
                    f"Click *Start My Break* within *2 minutes* or your spot will be skipped. ⏳"
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
    ts = post(f"🟢 <@{uid}> it's your turn!", channel=chan, blocks=blocks)
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
    chan = get_channel_for_break(brk)

    if brk["channel_msg_ts"]:
        update_msg(
            brk["channel_msg_ts"],
            f"❌ <@{uid}> didn't respond in time — spot skipped.",
            channel=chan,
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


def estimated_turn_time(pos):
    wait_mins = 0.0

    active = active_break()
    if active:
        elapsed   = time.time() - (active["started_at"] or time.time())
        remaining = max(0, active["requested_mins"] * 60 - elapsed)
        wait_mins += remaining / 60

    ahead = q(
        "SELECT requested_mins FROM breaks WHERE status IN ('queued','notified') "
        "ORDER BY id LIMIT ?", pos - 1
    )
    for r in ahead:
        wait_mins += r["requested_mins"]

    eta = datetime.now(LOCAL_TZ) + timedelta(minutes=wait_mins)
    return int(wait_mins), eta.strftime("%I:%M %p")


def promote_queue():
    if active_break():
        return
    nxt = next_queued()
    if nxt:
        notify_next(nxt["id"])
    else:
        # Announce queue is open in all allowed channels
        for c in BREAK_CHANNEL_IDS:
            post("✅ Break slot is open! Type `/break [minutes]` to request one.", channel=c)


# ── /break ────────────────────────────────────────────────────────────────────
@app.command("/break")
def handle_break(ack, body):
    ack()
    user    = body["user_id"]
    channel = body["channel_id"]

    if channel not in BREAK_CHANNEL_IDS:
        chans_formatted = " or ".join([f"<#{c}>" for c in BREAK_CHANNEL_IDS])
        ephemeral(user, f"⚠️ Please use `/break` in allowed channels: {chans_formatted}", channel)
        return

    text = body.get("text", "").strip()
    if text == "":
        mins = 15
    elif text.isdigit() and int(text) > 0:
        mins = int(text)
    else:
        ephemeral(user, "Usage: `/break 15` — number must be a positive whole number.", channel)
        return

    if mins > MAX_BREAK_MINS:
        ephemeral(user, f"⛔ Maximum break duration is *{MAX_BREAK_MINS} minutes*. Try `/break {MAX_BREAK_MINS}` or less.", channel)
        return

    remaining = minutes_remaining_today(user)
    if mins > remaining:
        ephemeral(
            user,
            f"⛔ You only have *{remaining:.0f} min* left today but requested *{mins} min*.\n"
            f"Try `/break {int(remaining)}` or less. Minutes reset at midnight! 🌙",
            channel
        )
        return

    existing = q(
        "SELECT id FROM breaks WHERE employee_id=? AND status IN ('queued','notified','on_break')",
        user, one=True
    )
    if existing:
        ephemeral(user, "⚠️ You already have an active break or are in the queue!", channel)
        return

    name   = username(user)
    active = active_break()
    
    # Save the channel_id so the bot knows where to reply later
    bid = run(
        "INSERT INTO breaks (employee_id, status, requested_mins, channel_id) VALUES (?,?,?,?)", 
        user, "queued", mins, channel
    )
    qc = queue_count() - 1

    if active is None and qc == 0:
        start_break(bid)
    else:
        pos = (1 if active else 0) + qc + 1
        wait_mins, eta = estimated_turn_time(pos)
        post(f"⏳ <@{user}> is *{ordinal(pos)}* in the break queue.", channel=channel)
        ephemeral(
            user,
            f"⏳ You're *{ordinal(pos)}* in the queue for a *{mins}-min* break.\n"
            f"  • *Estimated wait:* ~{wait_mins} min\n"
            f"  • *Estimated turn:* {eta}\n"
            f"I'll tag you here when it's your turn! 🎯",
            channel
        )
        dm(
            f"📥 *Break Requested (Queued)*\n"
            f"  • *Employee:* {name}\n"
            f"  • *Duration:* {mins} min\n"
            f"  • *Queue position:* {ordinal(pos)}\n"
            f"  • *Est. turn time:* {eta} (~{wait_mins} min wait)\n"
            f"  • *Minutes used today:* {int(minutes_used_today(user))}/{cfg('daily_minutes')}\n"
            f"  • *Time:* {now_str()}"
        )


# ── Manager Commands ──────────────────────────────────────────────────────────
@app.command("/setminutes")
def handle_setminutes(ack, body):
    ack()
    user    = body["user_id"]
    channel = body["channel_id"]
    if user not in MANAGER_USER_IDS:
        ephemeral(user, "⛔ Only managers can change the break allowance.", channel)
        return
    text = body.get("text", "").strip()
    if not text.isdigit() or int(text) < 1:
        ephemeral(user, "Usage: `/setminutes 60` — sets daily allowance to 60 min per employee.", channel)
        return
    set_cfg("daily_minutes", text)
    for c in BREAK_CHANNEL_IDS:
        post(f"📢 Daily break allowance updated to *{text} minutes per employee* per day.", channel=c)
    dm(f"✏️ You updated the daily break allowance to *{text} min/person/day* at {now_str()}.")


@app.command("/resetbreaks")
def handle_reset(ack, body):
    ack()
    user    = body["user_id"]
    channel = body["channel_id"]
    if user not in MANAGER_USER_IDS:
        ephemeral(user, "⛔ Only managers can reset breaks.", channel)
        return
    midnight = datetime.now(LOCAL_TZ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    run("DELETE FROM breaks WHERE status NOT IN ('on_break') AND created_at >= ?", midnight)
    for c in BREAK_CHANNEL_IDS:
        post("🔄 Break minutes reset by a manager. Everyone starts fresh!", channel=c)
    dm(f"🔄 You reset all break minutes at {now_str()}.")


@app.command("/resetperson")
def handle_resetperson(ack, body):
    ack()
    user    = body["user_id"]
    channel = body["channel_id"]
    if user not in MANAGER_USER_IDS:
        ephemeral(user, "⛔ Only managers can use this command.", channel)
        return
    text = body.get("text", "").strip()
    import re
    match = re.search(r"U[A-Z0-9]+", text)
    if not match:
        ephemeral(user, "Usage: `/resetperson @employee` — resets that person's break minutes for today.", channel)
        return
    target_uid = match.group(0)
    midnight   = datetime.now(LOCAL_TZ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    run(
        "DELETE FROM breaks WHERE employee_id=? AND status NOT IN ('on_break') AND created_at >= ?",
        target_uid, midnight
    )
    name = username(target_uid)
    ephemeral(user, f"✅ Reset break minutes for *{name}* today.", channel)
    dm(f"🔄 You reset break minutes for *{name}* at {now_str()}.")


@app.command("/clearqueue")
def handle_clearqueue(ack, body):
    ack()
    user    = body["user_id"]
    channel = body["channel_id"]
    if user not in MANAGER_USER_IDS:
        ephemeral(user, "⛔ Only managers can use this command.", channel)
        return
    for t in list(active_timers.values()):
        try:
            t.cancel()
        except Exception:
            pass
    active_timers.clear()
    run("UPDATE breaks SET status='cancelled' WHERE status IN ('queued','notified','on_break')")
    ephemeral(user, "🧹 All active breaks and queue entries cleared!", channel)
    for c in BREAK_CHANNEL_IDS:
        post("🧹 Break queue cleared by a manager. Use `/break [minutes]` to start fresh!", channel=c)
    dm(f"🧹 You cleared the entire break queue at {now_str()}.")


@app.command("/breakstatus")
def handle_status(ack, body):
    ack()
    user    = body["user_id"]
    channel = body["channel_id"]
    if user not in MANAGER_USER_IDS:
        ephemeral(user, "⛔ Only managers can view the full break status.", channel)
        return

    active   = active_break()
    queued   = q("SELECT * FROM breaks WHERE status IN ('queued','notified') ORDER BY id")
    midnight = datetime.now(LOCAL_TZ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    today_uids = q(
        "SELECT DISTINCT employee_id FROM breaks WHERE created_at>=? "
        "AND status NOT IN ('forfeited','cancelled','denied')", midnight
    )
    limit = cfg("daily_minutes")
    lines = [f"📊 *Break Dashboard — {datetime.now(LOCAL_TZ).strftime('%b %d, %Y %I:%M %p')}*\n"]

    if active:
        elapsed   = int(time.time() - (active["started_at"] or time.time()))
        remaining = max(0, active["requested_mins"] * 60 - elapsed)
        eta       = (datetime.now(LOCAL_TZ) + timedelta(seconds=remaining)).strftime("%I:%M %p")
        lines.append(f"🟡 *On Break:* <@{active['employee_id']}> — _{fmt_dur(remaining)} left, est. return {eta}_")
    else:
        lines.append("🟢 *Break Slot:* Available")

    if queued:
        lines.append(f"\n👥 *Queue ({len(queued)}):*")
        for i, bq in enumerate(queued, 1):
            icon = "🔔" if bq["status"] == "notified" else "⏳"
            lines.append(f"  {icon} {ordinal(i)}: {username(bq['employee_id'])} ({bq['requested_mins']} min)")
    else:
        lines.append("\n👥 *Queue:* Empty")

    if today_uids:
        lines.append(f"\n📅 *Today's Usage (limit: {limit} min/person):*")
        for row in today_uids:
            used = minutes_used_today(row["employee_id"])
            rem  = max(0, int(limit) - used)
            pct  = min(int(used / int(limit) * 10), 10)
            bar  = "🟩" * pct + "⬜" * (10 - pct)
            lines.append(f"  {bar} {username(row['employee_id'])}: {used:.0f}/{limit} min ({rem:.0f} left)")
    else:
        lines.append(f"\n📅 *Today's Usage:* None yet (limit {limit} min/person)")

    ephemeral(user, "\n".join(lines), channel)


@app.command("/breakreport")
def handle_report(ack, body):
    ack()
    user    = body["user_id"]
    channel = body["channel_id"]
    if user not in MANAGER_USER_IDS:
        ephemeral(user, "⛔ Only managers can view break reports.", channel)
        return

    text = body.get("text", "").strip().lower()
    if text == "week":
        since = (datetime.now(LOCAL_TZ) - timedelta(days=7)).timestamp()
        period = "Last 7 Days"
    else:
        since = datetime.now(LOCAL_TZ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        period = "Today"

    rows = q(
        "SELECT * FROM breaks WHERE created_at>=? AND status='completed' ORDER BY created_at DESC",
        since
    )

    if not rows:
        ephemeral(user, f"📋 No completed breaks found for *{period}*.", channel)
        return

    summary: dict = {}
    for r in rows:
        uid = r["employee_id"]
        if uid not in summary:
            summary[uid] = {"on_time": 0, "early": 0, "late": 0, "total_min": 0.0, "breaks": []}
        rs = r["return_status"] or "on_time"
        summary[uid][rs] += 1
        summary[uid]["total_min"] += (r["duration_sec"] or 0) / 60
        started = datetime.fromtimestamp(r["started_at"], tz=LOCAL_TZ).strftime("%I:%M %p") if r["started_at"] else "?"
        ended   = datetime.fromtimestamp(r["ended_at"], tz=LOCAL_TZ).strftime("%I:%M %p") if r["ended_at"] else "?"
        rs_icon = {"early": "🟢", "late": "🔴", "on_time": "✅"}.get(rs, "✅")
        summary[uid]["breaks"].append(
            f"    {rs_icon} {started}→{ended} ({fmt_dur(r['duration_sec'] or 0)}, {r['requested_mins']} min scheduled)"
        )

    lines = [f"📋 *Break Report — {period}*\n"]
    for uid, s in summary.items():
        name = username(uid)
        lines.append(
            f"*{name}*\n"
            f"  ✅ On time: {s['on_time']}  🟢 Early: {s['early']}  🔴 Late: {s['late']}  "
            f"| Total: {s['total_min']:.0f} min\n"
        )
        for b in s["breaks"]:
            lines.append(b)
        lines.append("")

    ephemeral(user, "\n".join(lines), channel)


@app.command("/breakhelp")
def handle_help(ack, body):
    ack()
    user       = body["user_id"]
    channel    = body["channel_id"]
    is_manager = user in MANAGER_USER_IDS

    employee_cmds = (
        "*👤 Employee Commands:*\n"
        "  • `/break` — Start a 15-min break (default)\n"
        "  • `/break 10` — Start a 10-min break (max 15 min)\n"
        "\n"
        "*🔘 Buttons (appear in channel):*\n"
        "  • *▶️ Start My Break* — Claim your queued turn (2-min window)\n"
        "  • *🔚 End Break Early* — End your break instantly, logs actual time\n"
        "  • *✅ I'm Back!* — Confirm you're back after timer ends\n"
    )

    manager_cmds = (
        "\n*🔐 Manager-Only Commands:*\n"
        "  • `/breakstatus` — Live dashboard: who's out, queue, everyone's minutes today\n"
        "  • `/breakreport` — Today's full break log (on-time/early/late per person)\n"
        "  • `/breakreport week` — Same but for the last 7 days\n"
        "  • `/setminutes 60` — Set daily break allowance in minutes for everyone\n"
        "  • `/resetbreaks` — Wipe today's break minutes, everyone starts fresh\n"
        "  • `/resetperson @employee` — Reset only one person's break minutes today\n"
        "  • `/clearqueue` — Force-clear all active breaks and queue entries\n"
        "  • `/breakhelp` — Show this message\n"
    )

    text = employee_cmds + (manager_cmds if is_manager else "")
    ephemeral(user, text, channel)


# ── Button: Start My Break ────────────────────────────────────────────────────
@app.action("start_queued_break")
def handle_start_queued(ack, body, action):
    ack()
    user    = body["user"]["id"]
    channel = body["channel"]["id"]
    brk_id  = int(action["value"])
    brk     = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        ephemeral(user, "⚠️ Break not found.", channel)
        return
    if brk["employee_id"] != user:
        ephemeral(user, "⚠️ This isn't your break notification.", channel)
        return
    if brk["status"] != "notified":
        ephemeral(user, "⚠️ This break is no longer available.", channel)
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
    user    = body["user"]["id"]
    channel = body["channel"]["id"]
    brk_id  = int(action["value"])
    brk     = q("SELECT * FROM breaks WHERE id=?", brk_id, one=True)
    if not brk:
        ephemeral(user, "⚠️ Break not found.", channel)
        return
    if brk["employee_id"] != user:
        ephemeral(user, "⚠️ Only the person on break can end it early.", channel)
        return
    if brk["status"] != "on_break":
        ephemeral(user, "⚠️ This break is no longer active.", channel)
        return
    t = active_timers.pop(brk_id, None)
    if t:
        t.cancel()
    remind_t = active_timers.pop(f"remind_{brk_id}", None)
    if remind_t:
        remind_t.cancel()
    if _finish_break(brk_id, user, early=True):
        promote_queue()


# ── Button: I'm Back ─────────────────────────────────────────────────────────
@app.action("im_back")
def handle_im_back(ack, body, action):
    ack()
    user    = body["user"]["id"]
    channel = body["channel"]["id"]
    brk_id  = int(action["value"])
    complete_break(brk_id, user, channel)


# ── Midnight reset ────────────────────────────────────────────────────────────
def midnight_reset():
    try:
        mins = cfg("daily_minutes")
        for c in BREAK_CHANNEL_IDS:
            post(
                f"🌅 Good morning! Break minutes have reset. "
                f"Each employee has *{mins} minutes* today. Type `/break` to use some!",
                channel=c
            )
        dm(
            f"🌅 *Daily Reset*\n"
            f"  • Break minutes reset at midnight\n"
            f"  • Current allowance: *{mins} min/person/day*"
        )
    except Exception as e:
        print(f"[midnight reset error] {e}")

def run_scheduler():
    schedule.every().day.at("22:00").do(midnight_reset)  # 22:00 UTC = 00:00 Cairo (UTC+2)
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    init_db()
    threading.Thread(target=run_scheduler, daemon=True).start()
    print(f"🚀 Break Queue Bot v7 running...")
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
