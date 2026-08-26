import asyncio
import aiohttp
import time
import sys

# ====== خواندن تنظیمات از فایل ======
def load_settings(file_path="settings.txt"):
    settings = {}
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # حذف کامنت بعد از #
            if "#" in line:
                line = line.split("#")[0].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            # تبدیل نوع داده
            if key in ["TOTAL_DATA_GB", "DURATION_SECONDS", "DATA_PER_REQUEST",
                       "CONCURRENT_LIMIT", "TIMEOUT"]:
                settings[key] = int(value)
            elif key in ["USE_POST"]:
                settings[key] = value.lower() in ("true", "1", "yes")
            elif key in ["MODE"]:
                settings[key] = value.lower()
            elif key in ["URL"]:
                settings[key] = value
    return settings

SETTINGS = load_settings()
MODE = SETTINGS.get("MODE", "volume")  # volume یا duration

URL = SETTINGS["URL"]
DATA_PER_REQUEST = SETTINGS["DATA_PER_REQUEST"]
CONCURRENT_LIMIT = SETTINGS["CONCURRENT_LIMIT"]
TIMEOUT = SETTINGS["TIMEOUT"]
USE_POST = SETTINGS["USE_POST"]

if MODE == "volume":
    TOTAL_DATA_GB = SETTINGS["TOTAL_DATA_GB"]
    TOTAL_BYTES = int(TOTAL_DATA_GB * 1024**3)
    TOTAL_REQUESTS = (TOTAL_BYTES + DATA_PER_REQUEST - 1) // DATA_PER_REQUEST
else:  # duration
    DURATION_SECONDS = SETTINGS["DURATION_SECONDS"]

PAYLOAD = b"0" * DATA_PER_REQUEST

async def send_request(session, semaphore, req_id):
    async with semaphore:
        start_time = time.time()
        try:
            if USE_POST:
                async with session.post(URL, data=PAYLOAD, timeout=TIMEOUT) as resp:
                    await resp.read()
                    status = resp.status
            else:
                async with session.get(URL, timeout=TIMEOUT) as resp:
                    bytes_read = 0
                    async for chunk in resp.content.iter_chunked(8192):
                        bytes_read += len(chunk)
                    if bytes_read < DATA_PER_REQUEST:
                        return f"ERROR: received {bytes_read} bytes, expected {DATA_PER_REQUEST}"
                    status = resp.status
            elapsed = time.time() - start_time
            return f"OK {status} {elapsed:.3f}s"
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            return f"ERROR {e}"

async def run_volume_mode(session, semaphore):
    print(f"هدف: انتقال {TOTAL_DATA_GB} GB داده")
    print(f"تعداد درخواست‌ها: {TOTAL_REQUESTS} (هر کدام {DATA_PER_REQUEST//1024} کیلوبایت)")
    tasks = [send_request(session, semaphore, i) for i in range(TOTAL_REQUESTS)]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    return results

async def run_duration_mode(session, semaphore):
    print(f"مدت زمان اجرا: {DURATION_SECONDS} ثانیه")
    end_time = time.time() + DURATION_SECONDS
    pending = set()
    req_id = 0
    # پر کردن اولیه
    for _ in range(CONCURRENT_LIMIT):
        task = asyncio.create_task(send_request(session, semaphore, req_id))
        pending.add(task)
        req_id += 1

    results = []
    while pending and time.time() < end_time:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED, timeout=0.5)
        for task in done:
            results.append(task.result())
        if time.time() < end_time:
            for _ in range(len(done)):
                task = asyncio.create_task(send_request(session, semaphore, req_id))
                pending.add(task)
                req_id += 1
    # تسک‌های باقی‌مانده
    if pending:
        done, _ = await asyncio.wait(pending)
        for task in done:
            results.append(task.result())
    return results

async def main():
    print("= تست بار =")
    print(f"آدرس: {URL}")
    print(f"هر درخواست: {DATA_PER_REQUEST//1024} کیلوبایت")
    print(f"هم‌روندی: {CONCURRENT_LIMIT}")
    print(f"متد: {'POST' if USE_POST else 'GET'}")
    print("شروع...")

    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
    connector = aiohttp.TCPConnector(
        limit=CONCURRENT_LIMIT,
        limit_per_host=CONCURRENT_LIMIT,
        force_close=False,
        enable_cleanup_closed=True
    )

    start_total = time.time()

    async with aiohttp.ClientSession(connector=connector) as session:
        if MODE == "volume":
            results = await run_volume_mode(session, semaphore)
        else:
            results = await run_duration_mode(session, semaphore)

    elapsed_total = time.time() - start_total

    ok_count = sum(1 for r in results if r.startswith("OK"))
    error_count = len(results) - ok_count
    total_bytes = ok_count * DATA_PER_REQUEST
    throughput = total_bytes / elapsed_total / (1024**3)  # GB/s

    print(f"\nپایان در {elapsed_total:.2f} ثانیه.")
    print(f"تعداد کل درخواست‌ها: {len(results)}")
    print(f"موفق: {ok_count}, خطا: {error_count}")
    print(f"حجم کل منتقل‌شده: {total_bytes / (1024**3):.3f} GB")
    print(f"نرخ انتقال: {throughput:.3f} GB/s")

if __name__ == "__main__":
    asyncio.run(main())
