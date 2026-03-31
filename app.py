"""
SnapLoad Backend - Flask + yt-dlp
চালাতে হলে:
  pip install flask flask-cors yt-dlp
  python app.py
"""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import yt_dlp
import os, threading, uuid, glob, time, re, sqlite3, validators, logging
from dotenv import load_dotenv

load_dotenv()

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# --- Configuration ---
DB_FILE = os.path.join(os.path.dirname(__file__), "tasks.db")
DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", 5))
CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", 3600))
FILE_LIFETIME = int(os.getenv("FILE_LIFETIME", 43200))
PORT = int(os.getenv("PORT", 5000))

# Semaphore to control concurrency
download_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_DOWNLOADS)

# Rate Limiter
limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://",
)

# Performance optimization: Metadata cache
INFO_CACHE = {} # {url: {"info": data, "expiry": timestamp}}
CACHE_TTL = 300 # 5 minutes

# --- Database Helpers ---
def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                status TEXT,
                progress REAL,
                filename TEXT,
                error TEXT,
                cancelled INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

init_db()

def create_task(task_id):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO tasks (task_id, status, progress) VALUES (?, ?, ?)",
            (task_id, "শুরু হচ্ছে...", 0)
        )
        conn.commit()

def update_task(task_id, status=None, progress=None, filename=None, error=None, cancelled=None):
    with get_db() as conn:
        updates = []
        params = []
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if progress is not None:
            updates.append("progress = ?")
            params.append(progress)
        if filename is not None:
            updates.append("filename = ?")
            params.append(filename)
        if error is not None:
            updates.append("error = ?")
            params.append(error)
        if cancelled is not None:
            updates.append("cancelled = ?")
            params.append(1 if cancelled else 0)
        
        if updates:
            params.append(task_id)
            conn.execute(f"UPDATE tasks SET {', '.join(updates)} WHERE task_id = ?", params)
            conn.commit()

# Throttling helper for progress updates
last_update_state = {} # {task_id: {"time": stamp, "progress": value}}

def should_update_db(task_id, progress):
    now = time.time()
    state = last_update_state.get(task_id)
    if not state:
        last_update_state[task_id] = {"time": now, "progress": progress}
        return True
    
    # Update if progress > 1% change OR > 1 second passed
    if abs(progress - state["progress"]) >= 1 or (now - state["time"]) >= 1:
        last_update_state[task_id] = {"time": now, "progress": progress}
        return True
    return False

def get_task(task_id):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        return dict(row) if row else None

def cleanup_task():
    """পুরনো ফাইল এবং ডাটাবেস এন্ট্রি ডিলিট করার ব্যাকগ্রাউন্ড টাস্ক"""
    while True:
        try:
            # ১২ ঘণ্টার বেশি পুরনো টাস্ক ডিলিট
            with get_db() as conn:
                old_tasks = conn.execute(
                    "SELECT task_id, filename FROM tasks WHERE datetime(created_at, 'localtime') < datetime('now', 'localtime', '-12 hours')"
                ).fetchall()
                
                for task in old_tasks:
                    tid = task["task_id"]
                    fname = task["filename"]
                    
                    # ফাইল ডিলিট
                    if fname and os.path.exists(fname):
                        try:
                            os.remove(fname)
                            logger.info(f"Cleanup: Deleted old file {os.path.basename(fname)}")
                        except Exception as e:
                            logger.error(f"Cleanup file error: {e}")
                    else:
                        # যদি ডাটাবেসে নাম না থাকে, তাও ডাউনলোড ফোল্ডার চেক করা ভালো
                        for f in glob.glob(os.path.join(DOWNLOAD_DIR, f"{tid}.*")):
                            try:
                                os.remove(f)
                                logger.info(f"Cleanup: Deleted orphan file {os.path.basename(f)}")
                            except: pass
                    
                    # ডাটাবেস এন্ট্রি ডিলিট
                    conn.execute("DELETE FROM tasks WHERE task_id = ?", (tid,))
                
                conn.commit()

        except Exception as e:
            logger.error(f"Cleanup Error: {e}")
        
        time.sleep(CLEANUP_INTERVAL)

# ক্লিনার থ্রেড চালু করা
logger.info("Starting cleanup thread...")
threading.Thread(target=cleanup_task, daemon=True).start()


# ── Ping ──────────────────────────────────
@app.route("/api/ping", methods=["GET", "POST", "OPTIONS"])
def ping():
    return jsonify({"ok": True})


# ── Video info ────────────────────────────
@app.route("/api/info", methods=["POST"])
@limiter.limit("50 per minute")
def get_info():
    data = request.get_json(silent=True) or {}
    url  = data.get("url", "").strip()

    if not url or not validators.url(url):
        return jsonify({"error": "সঠিক URL দিন"}), 400

    # 1. Check Cache
    cached = INFO_CACHE.get(url)
    if cached and time.time() < cached["expiry"]:
        logger.info(f"Cache hit for info: {url}")
        return jsonify(cached["info"])

    try:
        # 2. Extract with optimizations
        opts = {
            "quiet": True, "no_warnings": True, "skip_download": True,
            "lazy_playlist": True, "no_check_certificate": True,
            "extract_flat": "in_playlist"
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)

        seen_h = {}
        for f in (info.get("formats") or []):
            h      = f.get("height")
            vcodec = f.get("vcodec", "none") or "none"
            if not h or vcodec == "none":
                continue
            tbr  = f.get("tbr") or f.get("vbr") or 0
            size = f.get("filesize") or f.get("filesize_approx") or 0
            prev = seen_h.get(h)
            if prev is None or tbr > prev["tbr"]:
                seen_h[h] = {
                    "format_id": f.get("format_id", ""),
                    "height": h,
                    "vcodec": vcodec.split(".")[0],
                    "acodec": (f.get("acodec") or "").split(".")[0],
                    "fps":    f.get("fps") or 0,
                    "tbr":    round(tbr),
                    "size":   size,
                }

        video_formats = []
        for h, v in sorted(seen_h.items(), reverse=True):
            fmt_str = f"{v['format_id']}+bestaudio/best[height<={h}][ext=mp4]/best[height<={h}]"
            video_formats.append({
                "id": fmt_str, "label": f"{h}p", "type": "video",
                "ext": "mp4", "size": v["size"], "height": h,
                "vcodec": v["vcodec"], "acodec": v["acodec"],
                "fps": v["fps"], "tbr": v["tbr"],
            })

        duration = info.get("duration", 0)
        # 2.4 Best Audio formats (Added for mobile-side conversion)
        audio_formats = []
        seen_acodec = {}
        for f in (info.get("formats") or []):
            acodec = f.get("acodec", "none") or "none"
            vcodec = f.get("vcodec", "none") or "none"
            if acodec == "none" or vcodec != "none":
                continue
            
            abr  = f.get("abr") or f.get("tbr") or 0
            size = f.get("filesize") or f.get("filesize_approx") or 0
            ext  = f.get("ext", "m4a")
            
            # Keep the best of each extension (m4a, webm)
            prev = seen_acodec.get(ext)
            if prev is None or abr > prev["abr"]:
                seen_acodec[ext] = {
                    "format_id": f.get("format_id", ""),
                    "ext": ext,
                    "abr": round(abr),
                    "size": size,
                    "acodec": acodec.split(".")[0],
                }

        for ext, v in seen_acodec.items():
            label = f"Audio ({ext.upper()}) {v['abr']}kbps"
            audio_formats.append({
                "id": v["format_id"], "label": label, "type": "audio",
                "ext": ext, "size": v["size"], "height": 0,
                "vcodec": "", "acodec": v["acodec"], "fps": 0, "tbr": v["abr"]
            })

        # Keep hardcoded MP3 for browser compatibility if needed, but prioritize raw
        audio_formats.sort(key=lambda x: x["tbr"], reverse=True)

        result = {
            "title":      info.get("title", "শিরোনাম পাওয়া যায়নি"),
            "thumbnail":  info.get("thumbnail", ""),
            "duration":   info.get("duration", 0),
            "uploader":   info.get("uploader", ""),
            "view_count": info.get("view_count", 0),
            "platform":   info.get("extractor_key", "Unknown"),
            "formats":    video_formats + audio_formats,
            "url":        url,
        }
        
        # 3. Save to Cache
        INFO_CACHE[url] = {"info": result, "expiry": time.time() + CACHE_TTL}
        return jsonify(result)

    except yt_dlp.utils.DownloadError as e:
        return jsonify({"error": f"ভিডিও পাওয়া যায়নি: {str(e)[:200]}"}), 400
    except Exception as e:
        return jsonify({"error": f"সমস্যা: {str(e)[:200]}"}), 500


# ── Start download ────────────────────────
@app.route("/api/download", methods=["POST"])
@limiter.limit("10 per minute")
def start_download():
    data     = request.get_json(silent=True) or {}
    url      = data.get("url", "").strip()
    fmt_id   = data.get("format_id", "bestvideo+bestaudio/best")
    fmt_type = data.get("type", "video")
    task_id  = str(uuid.uuid4())

    if not url or not validators.url(url):
        return jsonify({"error": "সঠিক URL দিন"}), 400

    # format_id validation (basic regex)
    if not re.match(r'^[\w\+\-\[\]\=<>/\s]+$', fmt_id):
        return jsonify({"error": "অবৈধ ফরম্যাট আইডি"}), 400

    create_task(task_id)

    def run():
        # Acquire semaphore
        logger.info(f"Task {task_id}: Waiting for semaphore...")
        with download_semaphore:
            logger.info(f"Task {task_id}: Download started for {url}")
            out_tpl  = os.path.join(DOWNLOAD_DIR, f"{task_id}.%(ext)s")
            
            # ... (rest of progress_hook and postprocessor_hook remain same)
            
            try:
                # Re-defining methods inside run for closure usage
                def progress_hook(d):
                    st = get_task(task_id)
                    if not st: return
                    if st.get("cancelled"): raise Exception("Cancelled by user")
                    if d["status"] == "downloading":
                        p, t = d.get("downloaded_bytes", 0), d.get("total_bytes") or d.get("total_bytes_estimate")
                        if t:
                            progress = round((p / t) * 100, 1)
                            if should_update_db(task_id, progress):
                                speed = re.sub(r'\x1b\[[0-9;?]*[a-zA-Z]', '', d.get("_speed_str", "").strip())
                                update_task(task_id, status=f"ডাউনলোড হচ্ছে ({speed})" if speed else "ডাউনলোড হচ্ছে", progress=progress)
                        else:
                            update_task(task_id, status="ডাউনলোড হচ্ছে...", progress=((st.get("progress") or 0) + 0.1) % 100)

                def postprocessor_hook(d):
                    if d["status"] == "started": update_task(task_id, status="কনভার্ট হচ্ছে...", progress=95)
                    elif d["status"] == "finished":
                        fname = d.get("info_dict", {}).get("filepath") or d.get("info_dict", {}).get("filename") or ""
                        if fname and os.path.exists(fname): update_task(task_id, filename=fname)

                if fmt_type == "audio":
                    # skip FFmpegExtractAudio to allow mobile-side conversion
                    opts = {
                        "format": fmt_id if fmt_id not in ["320", "128"] else "bestaudio/best",
                        "outtmpl": out_tpl,
                        "quiet": True,
                        "progress_hooks": [progress_hook],
                        "postprocessor_hooks": [postprocessor_hook]
                    }
                    # Only convert to mp3 if user specifically requested 320/128 (for old app version compatibility)
                    if fmt_id in ["320", "128"]:
                        opts["postprocessors"] = [{
                            "key": "FFmpegExtractAudio",
                            "preferredcodec": "mp3",
                            "preferredquality": fmt_id
                        }]
                else:
                    opts = {"format": fmt_id, "outtmpl": out_tpl, "quiet": True, "progress_hooks": [progress_hook], "postprocessor_hooks": [postprocessor_hook], "merge_output_format": "mp4"}

                with yt_dlp.YoutubeDL(opts) as ydl:
                    logger.info(f"Task {task_id}: Executing ydl.download")
                    ydl.download([url])
                    logger.info(f"Task {task_id}: ydl.download finished execution")

                st = get_task(task_id)
                if not st:
                    logger.error(f"Task {task_id}: Record disappeared from DB")
                    return
                fname = st.get("filename")
                if not fname or not os.path.exists(fname):
                    pattern = os.path.join(DOWNLOAD_DIR, f"{task_id}.*")
                    found = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
                    # Priority 1: Exact matches for expected extensions
                    ext = ".mp3" if fmt_type == "audio" else ".mp4"
                    matches = [f for f in found if f.endswith(ext)]
                    if matches:
                        fname = matches[0]
                    elif found:
                        # Fallback: Just take the most recent file for this task
                        fname = found[0]
                    
                    if fname:
                        update_task(task_id, filename=fname)
                        logger.info(f"Task {task_id}: Found file via glob: {fname}")

                if fname and os.path.exists(fname):
                    update_task(task_id, status="প্রস্তুত", progress=100)
                    logger.info(f"Task {task_id}: Completed successfully")
                else:
                    update_task(task_id, status="ব্যর্থ", error="ফাইল তৈরি হয়নি।")
                    logger.error(f"Task {task_id}: File not found after download")

            except Exception as e:
                update_task(task_id, status="ব্যর্থ", error=str(e)[:400])
                logger.error(f"Task {task_id}: Failed with error: {e}")

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"task_id": task_id})


# ── Status ────────────────────────────────
@app.route("/api/status/<task_id>")
@limiter.exempt
def get_status(task_id):
    s = get_task(task_id)
    if not s:
        return jsonify({"error": "Task নেই"}), 404
    return jsonify(s)


# ── Cancel download ───────────────────────
@app.route("/api/cancel/<task_id>", methods=["POST"])
def cancel_download(task_id):
    s = get_task(task_id)
    if s:
        update_task(task_id, cancelled=True, status="বাতিল করা হয়েছে")
        return jsonify({"ok": True})
    return jsonify({"error": "Task পাওয়া যায়নি"}), 404


# ── Serve file ────────────────────────────
@app.route("/api/file/<task_id>")
def serve_file(task_id):
    s = get_task(task_id)
    if not s:
        return jsonify({"error": "Task নেই"}), 404

    if s.get("status") != "প্রস্তুত":
        return jsonify({"error": "ফাইল এখনো প্রস্তুত নয়"}), 400

    filepath = s.get("filename", "")

    # Path traversal prevention: only look for files starting with task_id in DOWNLOAD_DIR
    if not filepath or not os.path.exists(filepath):
        pattern  = os.path.join(DOWNLOAD_DIR, f"{task_id}.*")
        found    = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
        if found:
            filepath = found[0]

    if not filepath or not os.path.exists(filepath) or not os.path.basename(filepath).startswith(task_id):
        return jsonify({"error": "ফাইল পাওয়া যায়নি"}), 404

    ext      = os.path.splitext(filepath)[1].lower()
    mimetype = {
        ".mp4":  "video/mp4",
        ".mp3":  "audio/mpeg",
        ".m4a":  "audio/mp4",
        ".webm": "video/webm",
        ".mkv":  "video/x-matroska",
    }.get(ext, "application/octet-stream")

    return send_file(
        filepath,
        mimetype=mimetype,
        as_attachment=True,
        download_name=os.path.basename(filepath),
    )


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)
