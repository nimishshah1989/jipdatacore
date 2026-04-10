"""Scrape Goldilocks media pages (con-call, video, sound bytes) to find audio/video URLs.

Run on EC2: python3 scripts/ingest/scrape_media_pages.py
"""
import json
import os
import time

import requests
from bs4 import BeautifulSoup

BASE = "https://www.goldilocksresearch.com"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def load_env():
    env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    env_path = os.path.abspath(env_path)
    env = {}
    if os.path.exists(env_path):
        for line in open(env_path).read().splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def authenticate(email, password):
    from playwright.sync_api import sync_playwright

    print(f"Authenticating as {email[:20]}...")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=UA)
        page = context.new_page()

        page.goto(f"{BASE}/cus_signin.php", wait_until="domcontentloaded", timeout=30000)
        time.sleep(2)
        page.fill('input[name="Email"]', email)
        page.fill('input[name="Password"]', password)
        page.click('button[type="submit"]')
        time.sleep(5)

        page.goto(f"{BASE}/cus_dashboard.php", wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)
        content = page.content()
        if "window.location.href" in content[:200]:
            browser.close()
            raise RuntimeError("Login failed")

        cookies = context.cookies()
        browser.close()

    sess = requests.Session()
    sess.headers.update({"User-Agent": UA})
    for c in cookies:
        sess.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))
    print(f"Authenticated with {len(cookies)} cookies")
    return sess


def scrape_page(sess, label, url):
    print(f"\n{'=' * 60}")
    print(f"{label}: {url}")
    print("=" * 60)

    resp = sess.get(url, timeout=15)
    if "window.location.href" in resp.text[:200]:
        print("REDIRECTED - session expired!")
        return

    soup = BeautifulSoup(resp.text, "html.parser")

    # Save HTML
    html_path = f"/tmp/goldilocks_{label.lower()}.html"
    with open(html_path, "w") as f:
        f.write(resp.text)

    # Audio elements
    audio_tags = []
    for el in soup.find_all("audio"):
        src = el.get("src", "")
        sources = [s.get("src", "") for s in el.find_all("source")]
        all_srcs = [src] + sources if src else sources
        for s in all_srcs:
            if s:
                audio_tags.append(s)
                print(f"  AUDIO TAG: {s}")

    # Iframes (YouTube, Vimeo embeds)
    iframes = []
    for el in soup.find_all("iframe"):
        src = el.get("src", "")
        if src and "googletagmanager" not in src:
            iframes.append(src)
            print(f"  IFRAME: {src}")

    # Direct media links
    media_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(ext in href.lower() for ext in [".mp3", ".mp4", ".wav", ".m4a"]):
            text = a.get_text(strip=True)[:80]
            media_links.append({"url": href, "text": text})
            print(f"  MEDIA: {href}")
            print(f"    text: {text}")

    # YouTube links
    yt_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "youtube.com/watch" in href or "youtu.be" in href:
            text = a.get_text(strip=True)[:80]
            yt_links.append({"url": href, "text": text})
            print(f"  YOUTUBE: {href} | {text}")

    # Summary
    print(f"\n  Summary: {len(audio_tags)} audio, {len(media_links)} media links, "
          f"{len(iframes)} iframes, {len(yt_links)} youtube")
    print(f"  Page: {len(resp.text)} chars, saved to {html_path}")

    return {
        "label": label,
        "audio_tags": audio_tags,
        "iframes": iframes,
        "media_links": media_links,
        "yt_links": yt_links,
    }


def main():
    env = load_env()
    email = env.get("GOLDILOCKS_EMAIL", "")
    password = env.get("GOLDILOCKS_PASSWORD", "")
    if not email or not password:
        print("ERROR: GOLDILOCKS_EMAIL/PASSWORD not in .env")
        return

    sess = authenticate(email, password)
    time.sleep(2)

    results = {}
    for label, url in [
        ("monthly_concall", f"{BASE}/monthly_con_call.php"),
        ("video_updates", f"{BASE}/video_update.php"),
        ("sound_bytes", f"{BASE}/sound_byte.php"),
        ("qa_gautam", f"{BASE}/q_a_gautam.php"),
        ("market_snippets", f"{BASE}/market_snippets.php"),
    ]:
        result = scrape_page(sess, label, url)
        if result:
            results[label] = result
        time.sleep(3)

    # Save results
    out_path = "/tmp/goldilocks_media_inventory.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nInventory saved to {out_path}")


if __name__ == "__main__":
    main()
