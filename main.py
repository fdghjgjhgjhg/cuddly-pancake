import json
import threading
import time
import asyncio
import aiohttp
import os
from flask import Flask, request, render_template_string, redirect, url_for, session, jsonify

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-in-production'

# ====== نام فایل‌های JSON ======
SETTINGS_FILE = 'settings.json'
USERS_FILE = 'users.json'
RESULTS_FILE = 'results.json'
file_lock = threading.Lock()

# ====== تنظیمات پیش‌فرض ======
DEFAULT_SETTINGS = {
    'MODE': 'duration',
    'TOTAL_DATA_GB': '5',
    'DURATION_SECONDS': '60',
    'DATA_PER_REQUEST': '1048576',
    'CONCURRENT_LIMIT': '200',
    'TIMEOUT': '30',
    'USE_POST': 'False',
    'URL': 'http://192.168.1.1'
}

# ====== توابع مدیریت فایل‌ها ======
def load_settings():
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(DEFAULT_SETTINGS, f, indent=4, ensure_ascii=False)
        return DEFAULT_SETTINGS.copy()

def save_settings(settings):
    with file_lock:
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(settings, f, indent=4, ensure_ascii=False)

def load_users():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        default = [{"username": "Ziroxishere", "password": "Kt@115115"}]
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(default, f, indent=4, ensure_ascii=False)
        return default

def save_users(users):
    with file_lock:
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users, f, indent=4, ensure_ascii=False)

def find_user(username, password):
    for user in load_users():
        if user['username'] == username and user['password'] == password:
            return user
    return None

def load_results():
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        return []

def save_results(results):
    with file_lock:
        with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

def save_test_result(start_time, end_time, ok_count, error_count, total_bytes, throughput, details=''):
    results = load_results()
    results.append({
        'start_time': start_time,
        'end_time': end_time,
        'ok_count': ok_count,
        'error_count': error_count,
        'total_bytes': total_bytes,
        'throughput': throughput,
        'details': details
    })
    save_results(results)

def get_latest_result():
    results = load_results()
    return results[-1] if results else None

# ====== کد تست بار ======
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
    else:
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

# ====== متغیرهای وضعیت تست ======
test_running = False
test_result = None
test_thread = None

def run_test_in_thread():
    global test_running, test_result
    try:
        settings = load_settings()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(run_test_async(settings))
        test_result = result
    except Exception as e:
        test_result = {'error': str(e)}
    finally:
        test_running = False

# ====== قالب HTML یکپارچه (داشبورد) ======
DASHBOARD_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>داشبورد تست بار</title>
    <style>
        body { font-family: sans-serif; direction: rtl; text-align: right; padding: 20px; background: #f5f5f5; }
        .container { max-width: 900px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
        .nav { margin-bottom: 20px; }
        .nav a { margin-left: 15px; color: #2196F3; text-decoration: none; }
        h1 { color: #333; }
        .section { border: 1px solid #ddd; padding: 15px; margin-bottom: 20px; border-radius: 5px; background: #fafafa; }
        .section h2 { margin-top: 0; }
        .form-group { margin-bottom: 12px; display: flex; align-items: center; }
        .form-group label { width: 180px; font-weight: bold; }
        .form-group input, .form-group select { flex: 1; padding: 6px 10px; border: 1px solid #ccc; border-radius: 4px; }
        .form-group input[type="text"] { direction: ltr; text-align: left; }
        .btn { padding: 10px 20px; background: #4CAF50; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
        .btn:hover { background: #45a049; }
        .btn-danger { background: #f44336; }
        .btn-danger:hover { background: #da190b; }
        .result-box { background: #e8f5e9; padding: 15px; border-radius: 5px; border-right: 4px solid #4CAF50; }
        .result-box p { margin: 5px 0; }
        .error { color: red; }
        .success { color: green; }
        .status { padding: 5px 10px; background: #ffeb3b; display: inline-block; border-radius: 4px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="nav">
            <a href="{{ url_for('logout') }}">خروج</a>
        </div>
        <h1>📊 داشبورد تست بار</h1>

        <!-- بخش تنظیمات -->
        <div class="section">
            <h2>⚙️ تنظیمات</h2>
            <form method="post" action="/save_settings">
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
                <button type="submit" class="btn">💾 ذخیره تنظیمات</button>
                {% if save_message %}
                    <span class="success">{{ save_message }}</span>
                {% endif %}
            </form>
        </div>

        <!-- بخش کنترل تست -->
        <div class="section">
            <h2>🚀 اجرای تست</h2>
            {% if test_running %}
                <p><span class="status">⏳ تست در حال اجراست...</span></p>
                <button onclick="checkStatus()" class="btn">بررسی وضعیت</button>
                <div id="status"></div>
                <script>
                    function checkStatus() {
                        fetch('/status')
                            .then(res => res.json())
                            .then(data => {
                                if (data.running) {
                                    document.getElementById('status').innerHTML = 'در حال اجرا...';
                                    setTimeout(checkStatus, 2000);
                                } else {
                                    window.location.reload();
                                }
                            });
                    }
                    setTimeout(checkStatus, 2000);
                </script>
            {% else %}
                <form method="post" action="/run_test">
                    <button type="submit" class="btn">▶️ شروع تست جدید</button>
                </form>
            {% endif %}
        </div>

        <!-- بخش نتیجه آخرین تست -->
        {% if result %}
        <div class="section result-box">
            <h2>📈 نتیجه آخرین تست</h2>
            <p><strong>موفق:</strong> {{ result.ok }}</p>
            <p><strong>خطا:</strong> {{ result.error }}</p>
            <p><strong>حجم کل منتقل شده:</strong> {{ "%.3f"|format(result.total_bytes / (1024**3)) }} GB</p>
            <p><strong>نرخ انتقال:</strong> {{ "%.3f"|format(result.throughput) }} GB/s</p>
            <p><strong>زمان:</strong> {{ "%.2f"|format(result.elapsed) }} ثانیه</p>
            <p><strong>تعداد کل درخواست‌ها:</strong> {{ result.total_requests }}</p>
        </div>
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
        .login-box { max-width: 300px; margin: auto; border: 1px solid #ccc; padding: 20px; border-radius: 8px; background: white; }
        input { display: block; width: 100%; padding: 8px; margin: 10px 0; box-sizing: border-box; }
        button { padding: 10px 20px; background: #4CAF50; color: white; border: none; border-radius: 4px; cursor: pointer; }
        .error { color: red; }
    </style>
</head>
<body>
    <div class="login-box">
        <h2>ورود به داشبورد</h2>
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
    settings = load_settings()
    result = get_latest_result()
    global test_running
    return render_template_string(DASHBOARD_TEMPLATE, settings=settings, result=result,
                                   test_running=test_running, save_message=None)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if find_user(username, password):
            session['username'] = username
            return redirect(url_for('index'))
        else:
            return render_template_string(LOGIN_TEMPLATE, error='نام کاربری یا رمز عبور اشتباه است')
    return render_template_string(LOGIN_TEMPLATE)

@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))

@app.route('/save_settings', methods=['POST'])
def save_settings_route():  # نام تغییر یافته
    if 'username' not in session:
        return redirect(url_for('login'))
    settings = load_settings()
    for key in settings.keys():
        if key in request.form:
            settings[key] = request.form[key]
    save_settings(settings)  # تابع ذخیره‌سازی فایل JSON
    result = get_latest_result()
    global test_running
    return render_template_string(DASHBOARD_TEMPLATE, settings=settings, result=result,
                                   test_running=test_running, save_message='تنظیمات با موفقیت ذخیره شد!')

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
    load_settings()
    load_users()
    load_results()
    app.run(debug=True, host='0.0.0.0', port=5000)
