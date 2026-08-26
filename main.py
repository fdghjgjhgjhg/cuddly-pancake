import sqlite3
import json
import threading
import time
import asyncio
import aiohttp
from flask import Flask, request, render_template_string, redirect, url_for, session, jsonify

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-in-production'  # در محیط واقعی تغییر دهید

DB_PATH = 'db.db'

# ====== مدیریت دیتابیس ======
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        # جدول کاربران
        conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE,
                password TEXT
            )
        ''')
        # جدول تنظیمات
        conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        # جدول نتایج تست
        conn.execute('''
            CREATE TABLE IF NOT EXISTS test_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time REAL,
                end_time REAL,
                ok_count INTEGER,
                error_count INTEGER,
                total_bytes INTEGER,
                throughput REAL,
                details TEXT
            )
        ''')
        # اضافه کردن کاربر ادمین اگر موجود نیست
        cur = conn.execute("SELECT * FROM users WHERE username = 'Ziroxishere'")
        if not cur.fetchone():
            conn.execute("INSERT INTO users (username, password) VALUES (?, ?)",
                         ('Ziroxishere', 'Kt@115115'))
        # تنظیمات پیش‌فرض اگر وجود ندارند
        default_settings = {
            'MODE': 'duration',
            'TOTAL_DATA_GB': '5',
            'DURATION_SECONDS': '60',
            'DATA_PER_REQUEST': '1048576',
            'CONCURRENT_LIMIT': '200',
            'TIMEOUT': '30',
            'USE_POST': 'False',
            'URL': 'http://192.168.1.1'
        }
        for key, val in default_settings.items():
            cur = conn.execute("SELECT * FROM settings WHERE key = ?", (key,))
            if not cur.fetchone():
                conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (key, val))
        conn.commit()

def get_setting(key):
    with get_db() as conn:
        cur = conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cur.fetchone()
        return row['value'] if row else None

def set_setting(key, value):
    with get_db() as conn:
        conn.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        conn.commit()

def get_all_settings():
    with get_db() as conn:
        cur = conn.execute("SELECT key, value FROM settings")
        return {row['key']: row['value'] for row in cur.fetchall()}

def save_test_result(start_time, end_time, ok_count, error_count, total_bytes, throughput, details=''):
    with get_db() as conn:
        conn.execute('''
            INSERT INTO test_results (start_time, end_time, ok_count, error_count, total_bytes, throughput, details)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (start_time, end_time, ok_count, error_count, total_bytes, throughput, details))
        conn.commit()

def get_latest_result():
    with get_db() as conn:
        cur = conn.execute('''
            SELECT * FROM test_results ORDER BY id DESC LIMIT 1
        ''')
        return cur.fetchone()

# ====== کد تست بار (همان اسکریپت قبلی با تغییرات جزئی) ======
async def send_request(session, semaphore, req_id, url, data_per_request, timeout, use_post):
    async with semaphore:
        start_time = time.time()
        try:
            if use_post:
                payload = b"0" * data_per_request
                async with session.post(url, data=payload, timeout=timeout) as resp:
                    await resp.read()
                    status = resp.status
            else:
                async with session.get(url, timeout=timeout) as resp:
                    bytes_read = 0
                    async for chunk in resp.content.iter_chunked(8192):
                        bytes_read += len(chunk)
                    if bytes_read < data_per_request:
                        return f"ERROR: received {bytes_read} bytes, expected {data_per_request}"
                    status = resp.status
            elapsed = time.time() - start_time
            return f"OK {status} {elapsed:.3f}s"
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            return f"ERROR {e}"

async def run_test_async(settings):
    mode = settings.get('MODE', 'duration')
    url = settings['URL']
    data_per_request = int(settings['DATA_PER_REQUEST'])
    concurrent_limit = int(settings['CONCURRENT_LIMIT'])
    timeout = int(settings['TIMEOUT'])
    use_post = settings['USE_POST'].lower() == 'true'

    if mode == 'volume':
        total_data_gb = float(settings['TOTAL_DATA_GB'])
        total_bytes = int(total_data_gb * 1024**3)
        total_requests = (total_bytes + data_per_request - 1) // data_per_request
    else:  # duration
        duration_seconds = int(settings['DURATION_SECONDS'])

    semaphore = asyncio.Semaphore(concurrent_limit)
    connector = aiohttp.TCPConnector(
        limit=concurrent_limit,
        limit_per_host=concurrent_limit,
        force_close=False,
        enable_cleanup_closed=True
    )

    start_total = time.time()
    ok_count = 0
    error_count = 0
    total_bytes_transferred = 0
    results = []

    async with aiohttp.ClientSession(connector=connector) as session:
        if mode == 'volume':
            tasks = [send_request(session, semaphore, i, url, data_per_request, timeout, use_post)
                     for i in range(total_requests)]
            results = await asyncio.gather(*tasks, return_exceptions=False)
        else:
            end_time = time.time() + duration_seconds
            pending = set()
            req_id = 0
            for _ in range(concurrent_limit):
                task = asyncio.create_task(send_request(session, semaphore, req_id, url, data_per_request, timeout, use_post))
                pending.add(task)
                req_id += 1

            while pending and time.time() < end_time:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED, timeout=0.5)
                for task in done:
                    results.append(task.result())
                if time.time() < end_time:
                    for _ in range(len(done)):
                        task = asyncio.create_task(send_request(session, semaphore, req_id, url, data_per_request, timeout, use_post))
                        pending.add(task)
                        req_id += 1
            if pending:
                done, _ = await asyncio.wait(pending)
                for task in done:
                    results.append(task.result())

    elapsed_total = time.time() - start_total
    for res in results:
        if res.startswith("OK"):
            ok_count += 1
            total_bytes_transferred += data_per_request
        else:
            error_count += 1

    throughput = total_bytes_transferred / elapsed_total / (1024**3) if elapsed_total > 0 else 0

    save_test_result(start_total, time.time(), ok_count, error_count, total_bytes_transferred, throughput,
                     details=f"تعداد کل درخواست‌ها: {len(results)}")
    return {
        'ok': ok_count,
        'error': error_count,
        'total_bytes': total_bytes_transferred,
        'throughput': throughput,
        'elapsed': elapsed_total,
        'total_requests': len(results)
    }

# متغیرهای سراسری برای وضعیت تست
test_running = False
test_result = None
test_thread = None

def run_test_in_thread():
    global test_running, test_result
    try:
        settings = get_all_settings()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(run_test_async(settings))
        test_result = result
    except Exception as e:
        test_result = {'error': str(e)}
    finally:
        test_running = False

# ====== قالب‌های HTML (جاسازی‌شده در کد) ======
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>پنل تست بار</title>
    <style>
        body { font-family: sans-serif; direction: rtl; text-align: right; padding: 20px; }
        .container { max-width: 800px; margin: auto; }
        .form-group { margin-bottom: 10px; }
        label { display: inline-block; width: 150px; }
        input, select { padding: 5px; width: 200px; }
        button { padding: 10px 20px; background: #4CAF50; color: white; border: none; cursor: pointer; }
        .error { color: red; }
        .success { color: green; }
        .result-box { border: 1px solid #ccc; padding: 10px; margin-top: 20px; }
        .nav { margin-bottom: 20px; }
        .nav a { margin-left: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="nav">
            <a href="{{ url_for('settings') }}">تنظیمات</a>
            <a href="{{ url_for('logout') }}">خروج</a>
        </div>
        <h1>پنل تست بار</h1>
        {% if test_running %}
            <p>تست در حال اجراست...</p>
            <button onclick="checkStatus()">بررسی وضعیت</button>
            <div id="status"></div>
            <script>
                function checkStatus() {
                    fetch('/status')
                        .then(res => res.json())
                        .then(data => {
                            if (data.running) {
                                document.getElementById('status').innerHTML = 'در حال اجرا...';
                            } else {
                                window.location.reload();
                            }
                        });
                }
                setTimeout(checkStatus, 3000);
            </script>
        {% else %}
            {% if result %}
                <div class="result-box">
                    <h3>نتیجه آخرین تست</h3>
                    <p>موفق: {{ result.ok }}</p>
                    <p>خطا: {{ result.error }}</p>
                    <p>حجم کل منتقل شده: {{ "%.3f"|format(result.total_bytes / (1024**3)) }} GB</p>
                    <p>نرخ انتقال: {{ "%.3f"|format(result.throughput) }} GB/s</p>
                    <p>زمان: {{ "%.2f"|format(result.elapsed) }} ثانیه</p>
                    <p>تعداد کل درخواست‌ها: {{ result.total_requests }}</p>
                </div>
            {% endif %}
            <form action="/run_test" method="post">
                <button type="submit">شروع تست جدید</button>
            </form>
        {% endif %}
    </div>
</body>
</html>
'''

SETTINGS_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>تنظیمات</title>
    <style>
        body { font-family: sans-serif; direction: rtl; text-align: right; padding: 20px; }
        .container { max-width: 600px; margin: auto; }
        .form-group { margin-bottom: 10px; }
        label { display: inline-block; width: 150px; }
        input, select { padding: 5px; width: 200px; }
        button { padding: 10px 20px; background: #4CAF50; color: white; border: none; cursor: pointer; }
        .nav { margin-bottom: 20px; }
        .nav a { margin-left: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="nav">
            <a href="{{ url_for('index') }}">خانه</a>
            <a href="{{ url_for('logout') }}">خروج</a>
        </div>
        <h1>تنظیمات</h1>
        <form method="post">
            {% for key, value in settings.items() %}
            <div class="form-group">
                <label for="{{ key }}">{{ key }}</label>
                {% if key == 'USE_POST' %}
                    <select name="{{ key }}" id="{{ key }}">
                        <option value="True" {% if value == 'True' %}selected{% endif %}>True</option>
                        <option value="False" {% if value == 'False' %}selected{% endif %}>False</option>
                    </select>
                {% elif key == 'MODE' %}
                    <select name="{{ key }}" id="{{ key }}">
                        <option value="volume" {% if value == 'volume' %}selected{% endif %}>حجمی</option>
                        <option value="duration" {% if value == 'duration' %}selected{% endif %}>زمانی</option>
                    </select>
                {% else %}
                    <input type="text" name="{{ key }}" id="{{ key }}" value="{{ value }}">
                {% endif %}
            </div>
            {% endfor %}
            <button type="submit">ذخیره تنظیمات</button>
        </form>
        {% if message %}
            <p class="success">{{ message }}</p>
        {% endif %}
    </div>
</body>
</html>
'''

LOGIN_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>ورود</title>
    <style>
        body { font-family: sans-serif; direction: rtl; text-align: center; padding: 50px; }
        .login-box { max-width: 300px; margin: auto; border: 1px solid #ccc; padding: 20px; }
        input { display: block; width: 100%; padding: 8px; margin: 10px 0; }
        button { padding: 10px 20px; background: #4CAF50; color: white; border: none; }
        .error { color: red; }
    </style>
</head>
<body>
    <div class="login-box">
        <h2>ورود</h2>
        {% if error %}
            <p class="error">{{ error }}</p>
        {% endif %}
        <form method="post" action="/login">
            <input type="text" name="username" placeholder="نام کاربری" required>
            <input type="password" name="password" placeholder="رمز عبور" required>
            <button type="submit">ورود</button>
        </form>
    </div>
</body>
</html>
'''

# ====== مسیرهای Flask ======
@app.route('/')
def index():
    if 'username' not in session:
        return redirect(url_for('login'))
    global test_running, test_result
    return render_template_string(HTML_TEMPLATE, test_running=test_running, result=test_result)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        with get_db() as conn:
            cur = conn.execute("SELECT * FROM users WHERE username = ? AND password = ?", (username, password))
            user = cur.fetchone()
        if user:
            session['username'] = username
            return redirect(url_for('index'))
        else:
            return render_template_string(LOGIN_TEMPLATE, error='نام کاربری یا رمز عبور اشتباه است')
    return render_template_string(LOGIN_TEMPLATE)

@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if 'username' not in session:
        return redirect(url_for('login'))
    if request.method == 'POST':
        for key in ['MODE', 'TOTAL_DATA_GB', 'DURATION_SECONDS', 'DATA_PER_REQUEST',
                    'CONCURRENT_LIMIT', 'TIMEOUT', 'USE_POST', 'URL']:
            value = request.form.get(key)
            if value is not None:
                set_setting(key, value)
        return render_template_string(SETTINGS_TEMPLATE, settings=get_all_settings(), message='تنظیمات ذخیره شد')
    return render_template_string(SETTINGS_TEMPLATE, settings=get_all_settings(), message=None)

@app.route('/run_test', methods=['POST'])
def run_test():
    if 'username' not in session:
        return redirect(url_for('login'))
    global test_running, test_thread, test_result
    if test_running:
        return 'تست در حال اجراست', 400
    test_running = True
    test_result = None
    test_thread = threading.Thread(target=run_test_in_thread)
    test_thread.start()
    return redirect(url_for('index'))

@app.route('/status')
def status():
    global test_running, test_result
    return jsonify(running=test_running, result=test_result)

if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='0.0.0.0', port=5000)
