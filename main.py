import json
import threading
import time
import asyncio
import aiohttp
import os
import requests  # <-- اضافه شد
from flask import Flask, request, render_template_string, redirect, url_for, session, jsonify

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-in-production'

# ====== JSON File Names ======
SETTINGS_FILE = 'settings.json'
USERS_FILE = 'users.json'
RESULTS_FILE = 'results.json'
file_lock = threading.Lock()

# ====== Default Settings ======
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

# ====== File Management Functions ======
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
    if results:
        latest = results[-1]
        # Ensure all keys exist (for backward compatibility)
        if 'ok' not in latest and 'ok_count' in latest:
            latest['ok'] = latest['ok_count']
        if 'error' not in latest and 'error_count' in latest:
            latest['error'] = latest['error_count']
        if 'elapsed' not in latest and 'start_time' in latest and 'end_time' in latest:
            latest['elapsed'] = latest['end_time'] - latest['start_time']
        if 'total_requests' not in latest and 'details' in latest:
            try:
                import re
                match = re.search(r'Total requests: (\d+)', latest['details'])
                if match:
                    latest['total_requests'] = int(match.group(1))
                else:
                    latest['total_requests'] = latest.get('ok', 0) + latest.get('error', 0)
            except:
                latest['total_requests'] = latest.get('ok', 0) + latest.get('error', 0)
        latest.setdefault('ok', 0)
        latest.setdefault('error', 0)
        latest.setdefault('total_bytes', 0)
        latest.setdefault('throughput', 0)
        latest.setdefault('elapsed', 0)
        latest.setdefault('total_requests', 0)
        return latest
    return None

# ====== Function to fetch target IP ======
def fetch_target_ip():
    """دریافت IP هدف از فایل راه دور"""
    try:
        response = requests.get('https://getdown.xo.je/ip.txt', timeout=10)
        if response.status_code == 200:
            ip = response.text.strip()
            # اعتبارسنجی ساده (حداقل شامل نقطه باشد)
            if ip and '.' in ip:
                return ip
        return None
    except Exception as e:
        print(f"Error fetching IP: {e}")
        return None

# ====== Load Test Core ======
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

    result_data = {
        'ok': ok_count,
        'error': error_count,
        'total_bytes': total_bytes_transferred,
        'throughput': throughput,
        'elapsed': elapsed_total,
        'total_requests': len(results)
    }
    save_test_result(start_total, time.time(), ok_count, error_count, total_bytes_transferred, throughput,
                     details=f"Total requests: {len(results)}")
    return result_data

# ====== Test Status Variables ======
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

# ====== Pelican-Style HTML Template (modified) ======
DASHBOARD_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Load Test Dashboard</title>
    <style>
        /* === Reset & Base === */
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f8f9fa;
            color: #2d3436;
            line-height: 1.6;
            padding: 40px 20px;
        }
        .container {
            max-width: 1000px;
            margin: 0 auto;
            background: #ffffff;
            border-radius: 12px;
            box-shadow: 0 4px 24px rgba(0,0,0,0.06);
            padding: 40px 45px;
        }

        /* === Header === */
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 2px solid #f1f2f6;
            padding-bottom: 20px;
            margin-bottom: 30px;
        }
        .header h1 {
            font-size: 26px;
            font-weight: 600;
            color: #2d3436;
            letter-spacing: -0.3px;
        }
        .header h1 span {
            color: #0984e3;
        }
        .header .user {
            display: flex;
            align-items: center;
            gap: 12px;
        }
        .header .user .avatar {
            width: 36px;
            height: 36px;
            border-radius: 50%;
            background: #0984e3;
            color: white;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 14px;
        }
        .header .user a {
            color: #636e72;
            text-decoration: none;
            font-size: 14px;
            padding: 6px 14px;
            border-radius: 20px;
            background: #f1f2f6;
            transition: background 0.2s;
        }
        .header .user a:hover {
            background: #dfe6e9;
        }

        /* === Cards / Sections === */
        .card {
            background: #ffffff;
            border-radius: 10px;
            border: 1px solid #ecedef;
            padding: 24px 28px;
            margin-bottom: 28px;
            transition: box-shadow 0.2s;
        }
        .card:hover {
            box-shadow: 0 2px 12px rgba(0,0,0,0.04);
        }
        .card-title {
            font-size: 17px;
            font-weight: 600;
            color: #2d3436;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .card-title .icon {
            font-size: 20px;
        }

        /* === Form === */
        .form-row {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            margin-bottom: 14px;
        }
        .form-row label {
            width: 180px;
            font-weight: 500;
            font-size: 14px;
            color: #2d3436;
        }
        .form-row input,
        .form-row select {
            flex: 1;
            min-width: 200px;
            padding: 8px 14px;
            border: 1px solid #dcdde1;
            border-radius: 6px;
            font-size: 14px;
            background: #fafafa;
            transition: border-color 0.2s;
        }
        .form-row input:focus,
        .form-row select:focus {
            outline: none;
            border-color: #0984e3;
            background: #ffffff;
        }
        .form-row input[type="text"] {
            direction: ltr;
            text-align: left;
        }
        .form-actions {
            margin-top: 10px;
            display: flex;
            align-items: center;
            gap: 16px;
        }

        /* === Buttons === */
        .btn {
            display: inline-block;
            padding: 10px 24px;
            font-size: 15px;
            font-weight: 500;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s;
            text-decoration: none;
            background: #f1f2f6;
            color: #2d3436;
        }
        .btn-primary {
            background: #0984e3;
            color: #fff;
        }
        .btn-primary:hover {
            background: #0873c7;
            transform: translateY(-1px);
        }
        .btn-success {
            background: #00b894;
            color: #fff;
        }
        .btn-success:hover {
            background: #00a381;
            transform: translateY(-1px);
        }
        .btn-danger {
            background: #e17055;
            color: #fff;
        }
        .btn-danger:hover {
            background: #d63031;
        }
        .btn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
            transform: none;
        }
        .btn .spinner {
            display: inline-block;
            width: 16px;
            height: 16px;
            border: 2px solid rgba(255,255,255,0.3);
            border-top: 2px solid #fff;
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
            margin-right: 8px;
            vertical-align: middle;
        }
        @keyframes spin {
            to { transform: rotate(360deg); }
        }

        /* === Messages === */
        .message {
            padding: 10px 16px;
            border-radius: 6px;
            font-size: 14px;
            margin-top: 10px;
        }
        .message-success {
            background: #e8f8f5;
            color: #00b894;
            border: 1px solid #b2dfdb;
        }
        .message-error {
            background: #fde8e8;
            color: #e17055;
            border: 1px solid #f5c6cb;
        }

        /* === Result Box === */
        .result-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 16px;
            margin-top: 6px;
        }
        .result-item {
            background: #f8f9fa;
            padding: 14px 18px;
            border-radius: 8px;
            text-align: center;
        }
        .result-item .value {
            font-size: 26px;
            font-weight: 700;
            color: #2d3436;
            line-height: 1.2;
        }
        .result-item .label {
            font-size: 13px;
            color: #636e72;
            margin-top: 2px;
        }
        .result-item .value.green { color: #00b894; }
        .result-item .value.red { color: #e17055; }
        .result-item .value.blue { color: #0984e3; }
        .result-item .value.orange { color: #e17055; }

        /* === Status Badge === */
        .badge {
            display: inline-block;
            padding: 4px 14px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 500;
        }
        .badge-running {
            background: #ffeaa7;
            color: #b7950b;
        }
        .badge-idle {
            background: #dfe6e9;
            color: #636e72;
        }

        /* === Login === */
        .login-box {
            max-width: 360px;
            margin: 60px auto;
            background: #ffffff;
            padding: 40px 35px;
            border-radius: 12px;
            box-shadow: 0 4px 24px rgba(0,0,0,0.06);
        }
        .login-box h2 {
            font-weight: 600;
            margin-bottom: 24px;
            text-align: center;
            color: #2d3436;
        }
        .login-box input {
            width: 100%;
            padding: 10px 14px;
            margin-bottom: 14px;
            border: 1px solid #dcdde1;
            border-radius: 6px;
            font-size: 14px;
        }
        .login-box input:focus {
            outline: none;
            border-color: #0984e3;
        }
        .login-box .btn {
            width: 100%;
            text-align: center;
        }
        .login-box .error {
            color: #e17055;
            font-size: 14px;
            margin-bottom: 12px;
            text-align: center;
        }

        /* === Responsive === */
        @media (max-width: 640px) {
            .container { padding: 20px; }
            .form-row { flex-direction: column; align-items: stretch; }
            .form-row label { width: auto; margin-bottom: 4px; }
            .form-row input, .form-row select { width: 100%; }
            .header { flex-direction: column; gap: 12px; }
            .result-grid { grid-template-columns: 1fr 1fr; }
        }
    </style>
</head>
<body>

<div class="container">
    <!-- Header -->
    <div class="header">
        <h1>⚡ <span>Load</span>Test</h1>
        <div class="user">
            <div class="avatar">{{ session['username'][0]|upper }}</div>
            <span style="font-weight:500; font-size:15px;">{{ session['username'] }}</span>
            <a href="{{ url_for('logout') }}">Logout</a>
        </div>
    </div>

    <!-- Settings -->
    <div class="card">
        <div class="card-title"><span class="icon">⚙️</span> Settings</div>
        <form method="post" action="/save_settings">
            {% for key, value in settings.items() %}
            <div class="form-row">
                <label for="{{ key }}">{{ key }}</label>
                {% if key == 'USE_POST' %}
                    <select name="{{ key }}" id="{{ key }}">
                        <option value="True" {% if value == 'True' %}selected{% endif %}>True</option>
                        <option value="False" {% if value == 'False' %}selected{% endif %}>False</option>
                    </select>
                {% elif key == 'MODE' %}
                    <select name="{{ key }}" id="{{ key }}">
                        <option value="volume" {% if value == 'volume' %}selected{% endif %}>Volume</option>
                        <option value="duration" {% if value == 'duration' %}selected{% endif %}>Duration</option>
                    </select>
                {% else %}
                    <input type="text" name="{{ key }}" id="{{ key }}" value="{{ value }}">
                {% endif %}
            </div>
            {% endfor %}
            <div class="form-actions">
                <button type="submit" class="btn btn-primary">💾 Save Settings</button>
                {% if save_message %}
                    <span class="message message-success">{{ save_message }}</span>
                {% endif %}
            </div>
        </form>
    </div>

    <!-- Control -->
    <div class="card">
        <div class="card-title"><span class="icon">🚀</span> Control</div>
        {% if test_running %}
            <p>
                <span class="badge badge-running">● Running</span>
                <span style="margin-left:12px; color:#636e72;">Test is currently in progress...</span>
            </p>
            <div style="margin-top:14px;">
                <button onclick="checkStatus()" class="btn btn-primary">Refresh Status</button>
                <span id="status-text" style="margin-left:12px; font-size:14px; color:#0984e3;"></span>
            </div>
            <script>
                function checkStatus() {
                    fetch('/status')
                        .then(res => res.json())
                        .then(data => {
                            if (data.running) {
                                document.getElementById('status-text').textContent = '⏳ Still running...';
                                setTimeout(checkStatus, 2000);
                            } else {
                                window.location.reload();
                            }
                        })
                        .catch(() => {
                            document.getElementById('status-text').textContent = '⚠️ Error checking status';
                        });
                }
                setTimeout(checkStatus, 1500);
            </script>
        {% else %}
            <div style="display: flex; flex-wrap: wrap; gap: 12px; align-items: center;">
                <form method="post" action="/run_test">
                    <button type="submit" class="btn btn-success">▶️ Start Test (Auto IP)</button>
                </form>
                <form method="post" action="/run_test_custom" style="display: flex; gap: 8px; align-items: center; flex-wrap: wrap;">
                    <input type="text" name="custom_ip" placeholder="Or enter IP manually" style="padding: 8px 14px; border: 1px solid #dcdde1; border-radius: 6px; font-size: 14px; min-width: 180px;">
                    <button type="submit" class="btn btn-primary">🔧 Test Custom IP</button>
                </form>
            </div>
        {% endif %}
    </div>

    <!-- Latest Result -->
    {% if result %}
    <div class="card" style="border-color: #b2dfdb; background: #fafffe;">
        <div class="card-title"><span class="icon">📈</span> Latest Test Result</div>
        <div class="result-grid">
            <div class="result-item">
                <div class="value green">{{ result.get('ok', 0) }}</div>
                <div class="label">✅ Successful</div>
            </div>
            <div class="result-item">
                <div class="value red">{{ result.get('error', 0) }}</div>
                <div class="label">❌ Errors</div>
            </div>
            <div class="result-item">
                <div class="value blue">{{ "%.3f"|format(result.get('total_bytes', 0) / (1024**3)) }}</div>
                <div class="label">📦 Data Transferred (GB)</div>
            </div>
            <div class="result-item">
                <div class="value orange">{{ "%.3f"|format(result.get('throughput', 0)) }}</div>
                <div class="label">⚡ Throughput (GB/s)</div>
            </div>
            <div class="result-item">
                <div class="value">{{ "%.2f"|format(result.get('elapsed', 0)) }}</div>
                <div class="label">⏱️ Duration (s)</div>
            </div>
            <div class="result-item">
                <div class="value">{{ result.get('total_requests', 0) }}</div>
                <div class="label">📨 Total Requests</div>
            </div>
        </div>
    </div>
    {% endif %}

</div>

</body>
</html>
'''

LOGIN_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login - LoadTest</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f8f9fa;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            margin: 0;
            padding: 20px;
        }
        .login-box {
            background: #ffffff;
            padding: 40px 35px;
            border-radius: 12px;
            box-shadow: 0 4px 24px rgba(0,0,0,0.06);
            width: 100%;
            max-width: 360px;
        }
        .login-box h2 {
            font-weight: 600;
            margin-bottom: 8px;
            text-align: center;
            color: #2d3436;
            font-size: 24px;
        }
        .login-box .sub {
            text-align: center;
            color: #636e72;
            font-size: 14px;
            margin-bottom: 28px;
        }
        .login-box input {
            width: 100%;
            padding: 10px 14px;
            margin-bottom: 14px;
            border: 1px solid #dcdde1;
            border-radius: 6px;
            font-size: 14px;
            box-sizing: border-box;
            background: #fafafa;
        }
        .login-box input:focus {
            outline: none;
            border-color: #0984e3;
            background: #fff;
        }
        .login-box .btn {
            width: 100%;
            padding: 10px;
            background: #0984e3;
            color: #fff;
            border: none;
            border-radius: 6px;
            font-size: 16px;
            font-weight: 500;
            cursor: pointer;
            transition: background 0.2s;
        }
        .login-box .btn:hover {
            background: #0873c7;
        }
        .login-box .error {
            color: #e17055;
            font-size: 14px;
            margin-bottom: 14px;
            text-align: center;
        }
        .login-box .footer {
            text-align: center;
            margin-top: 18px;
            font-size: 13px;
            color: #b2bec3;
        }
    </style>
</head>
<body>
    <div class="login-box">
        <h2>⚡ LoadTest</h2>
        <div class="sub">Sign in to your account</div>
        {% if error %}
            <div class="error">{{ error }}</div>
        {% endif %}
        <form method="post" action="/login">
            <input type="text" name="username" placeholder="Username" required autofocus>
            <input type="password" name="password" placeholder="Password" required>
            <button type="submit" class="btn">Sign In</button>
        </form>
        <div class="footer">Default: Ziroxishere / Kt@115115</div>
    </div>
</body>
</html>
'''

# ====== Flask Routes ======
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
            return render_template_string(LOGIN_TEMPLATE, error='Invalid username or password')
    return render_template_string(LOGIN_TEMPLATE)

@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))

@app.route('/save_settings', methods=['POST'])
def save_settings_route():
    if 'username' not in session:
        return redirect(url_for('login'))
    settings = load_settings()
    for key in settings.keys():
        if key in request.form:
            settings[key] = request.form[key]
    save_settings(settings)
    result = get_latest_result()
    global test_running
    return render_template_string(DASHBOARD_TEMPLATE, settings=settings, result=result,
                                   test_running=test_running, save_message='Settings saved successfully!')

@app.route('/run_test', methods=['POST'])
def run_test():
    if 'username' not in session:
        return redirect(url_for('login'))
    global test_running, test_thread, test_result

    if test_running:
        return 'Test is already running', 400

    # دریافت IP از فایل راه دور
    target_ip = fetch_target_ip()
    if not target_ip:
        return '❌ Unable to fetch target IP from https://getdown.xo.je/ip.txt', 400

    # به‌روزرسانی تنظیمات با IP جدید
    settings = load_settings()
    settings['URL'] = f'http://{target_ip}'
    save_settings(settings)

    test_running = True
    test_result = None
    test_thread = threading.Thread(target=run_test_in_thread)
    test_thread.start()
    return redirect(url_for('index'))

@app.route('/run_test_custom', methods=['POST'])
def run_test_custom():
    if 'username' not in session:
        return redirect(url_for('login'))
    global test_running, test_thread, test_result

    if test_running:
        return 'Test is already running', 400

    custom_ip = request.form.get('custom_ip', '').strip()
    if not custom_ip:
        return 'Please enter a valid IP', 400

    settings = load_settings()
    settings['URL'] = f'http://{custom_ip}'
    save_settings(settings)

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
