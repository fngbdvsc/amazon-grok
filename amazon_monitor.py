"""
Amazon CA monitor for Discord. A separate bot from the Walmart one (own repo, own Railway service).

- Every request to Amazon is spaced 8-15 seconds apart (random).
- Alerts only post when the PRODUCT PAGE confirms: "Ships from and sold by Amazon.ca" + in stock + real price.
  Resellers on Amazon list Pokemon at 2-10x, so third-party sellers never alert (this is a hard rule, not a setting).
- It always tells you WHY (in alerts, status messages, and with !why).
- You control it from Discord with commands (type !help in your alerts channel).
- If DASHBOARD_URL and BOT_API_KEY are set (Railway Variables), the DASHBOARD becomes the boss:
  it supplies the ASIN list (entries whose store starts with "amazon") and the pause state, and gets a copy of
  every alert, error and status message (each one starts with "[Amazon]" so you can tell it from Walmart).

Setup:    python3 -m pip install -r requirements.txt
Test:     python3 amazon_monitor.py test
Check one product:  python3 amazon_monitor.py debug <ASIN>
Check a saved page:  python3 amazon_monitor.py parse page.html
Run:      python3 amazon_monitor.py

IMPORTANT for commands: in the Discord Developer Portal, open your app -> Bot ->
turn ON "Message Content Intent", then save. Without it the bot can't read commands.
"""
import os
import re
import sys
import json
import time
import random
import asyncio
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus, unquote, urlsplit

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

# ====== PUT YOUR SETTINGS HERE (keep the quotes) ======
DISCORD_TOKEN = ""     # leave empty: set DISCORD_TOKEN in Railway Variables
CHANNEL_ID = ""        # leave empty: set CHANNEL_ID in Railway Variables
ROLE_ID = ""           # role to ping (leave "" for no ping)
PROXY = ""             # optional: http://user:pass@host:port (leave "" for none)
MAINTENANCE_CHANNEL_ID = 1569533434546757703   # errors, blocks and startup info (MAINTENANCE_CHANNEL_ID variable overrides)
ALLOWED_USER_IDS = []  # optional: [123456789012345678]. Empty = anyone with "Manage Server" can use commands
# =======================================================

STORE = "Amazon"
LOG_TAG = "[Amazon] "          # put in front of every dashboard line, so it's clear which bot sent it

# Seconds between ANY two requests to Amazon, random between these two numbers.
# Change them with the GAP_MIN / GAP_MAX Railway Variables (default 8 and 15). Never below 3 seconds.
GAP_MIN = max(3, int(os.environ.get("GAP_MIN", "8")))
GAP_MAX = max(GAP_MIN, int(os.environ.get("GAP_MAX", "15")))

# Each ASIN is checked at most once per this many seconds. With a short list the bot IDLES between rounds
# instead of re-loading the same few product pages over and over (that pattern gets you blocked).
MIN_SECONDS_PER_ROUND = int(os.environ.get("MIN_SECONDS_PER_ROUND", "90"))

# HARD RULE (same as the Walmart bot): alert only when the product page says the item ships from AND is sold by
# Amazon.ca. Not a Railway variable on purpose. Do not make this switchable.
ONLY_AMAZON_CA_SOLD = True
PING_ON_NEW_ITEM = True        # ping the role for New Item alerts too, not just restocks

# Starting watchlist when there's no dashboard (after that, use !watch / !remove in Discord - changes are saved)
WATCH_ASINS = []

# Search scan: ON = also scan Pokemon search results for new listings and stock changes, on top of your ASINs.
# Default OFF for Amazon: search results are full of resellers, and every page is one more chance to be blocked.
# Set SEARCH_SCAN=1 in Railway Variables to turn it on.
SEARCH_SCAN = os.environ.get("SEARCH_SCAN", "0") == "1"
SEARCH_TERMS = ["pokemon tcg", "pokemon cards"]

# Browse like a person: each round first opens a search page (results ignored, no new-listing alerts),
# then product pages with that search page as the referrer.
BROWSE_FIRST = os.environ.get("BROWSE_FIRST", "1") != "0"

# After a block: start a brand-new browser (1, default) or keep the same one and its cookies (0).
# On Walmart, keeping the flagged cookies left the bot blocked hundreds of times in a row.
FRESH_BROWSER_ON_BLOCK = os.environ.get("FRESH_BROWSER_ON_BLOCK", "1") != "0"
SEARCH_PAGES = 1
SEARCH_EVERY_N_ROUNDS = 3      # scan search results every 3rd round (watchlist is checked EVERY round)

MAX_VERIFY_PER_ROUND = 4       # product-page checks triggered by search results, per round
MAX_VERIFY_TRIES = 3
RECHECK_SECONDS = 120
# If Amazon shows its robot-check page, pause this many seconds, doubling each time up to BLOCK_RETRY_MAX.
BACKOFF_START = int(os.environ.get("BLOCK_RETRY_SECONDS", "300"))
BACKOFF_MAX = int(os.environ.get("BLOCK_RETRY_MAX", "1800"))
BLOCK_UPDATE_SECONDS = 600     # while a block lasts, post a "still blocked" update this often
WATCHDOG_SECONDS = 600         # if the check loop doesn't move for this long, the bot restarts itself

EXCLUDE_WORDS = ["t-shirt", "shirt", " tee", "backpack", "slipper", "pencil", "balloon",
                 "jibbitz", "swim", "shoe", "costume", "bottle", "snack", "sock", "pajama",
                 "hoodie", "blanket", "bedding", "coloring", "plush", "mug", "lunch",
                 "sleeves", "binder", "deck box", "playmat", "case for", "lot of", "custom"]

# On Railway, files vanish on every redeploy unless you attach a Volume (mount it at /data and set DATA_DIR=/data)
DATA_DIR = os.environ.get("DATA_DIR", ".")
STATE_FILE = os.path.join(DATA_DIR, "amazon_state.json")
CONFIG_FILE = os.path.join(DATA_DIR, "amazon_config.json")
BASE = "https://www.amazon.ca"
AMAZON_CA_MERCHANT_ID = "A3DWYIK6Y9EEQB"   # Amazon.ca's own seller ID (used only as a backup check)

# "browser" = load Amazon pages in a real Chrome. "curl" = lightweight method without a browser.
FETCHER = os.environ.get("FETCHER", "browser").strip().lower()
# On Railway, Chrome runs as a normal window on a virtual screen (HEADLESS=0 + DISPLAY from the start command).
# Without a screen (e.g. on a Mac test), it falls back to hidden.
HEADLESS = os.environ.get("HEADLESS", "1") != "0" or not os.environ.get("DISPLAY")
# With the dashboard connected, status messages go only to the dashboard and Discord gets only
# Restock / New Item alerts. Set STATUS_IN_DISCORD=1 to also post them in Discord.
STATUS_IN_DISCORD = os.environ.get("STATUS_IN_DISCORD", "0") == "1"
CONFIG_EVERY_SECONDS = (50, 70)   # how often the bot asks the dashboard for its settings (random in this range)
ERROR_REPEAT_SECONDS = 600        # don't send the same error to the dashboard more often than this

LOCAL_TZ = ZoneInfo(os.environ.get("TIMEZONE", "America/Toronto"))   # Quebec time; Railway's clock is UTC
BLOCK_REASON = "Amazon's robot-check page appeared, so this check was paused."


def local_now():
    return datetime.now(LOCAL_TZ)


def page_name(job):
    if not job:
        return "unknown page"
    if job[0] == "home":
        return "the homepage"
    if job[0] in ("browse", "search"):
        return f"the search page for '{job[1]}'"
    return f"the product page for {job[1]}"


ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")
ASIN_IN_URL = re.compile(r"/(?:dp|gp/product|gp/aw/d|product|d)/([A-Za-z0-9]{10})(?:[/?#]|$)")


class Blocked(Exception):
    """Amazon returned its robot-check page instead of the real page."""


# ------------------------------------------------------------ pure helpers
def page_title(html):
    m = re.search(r"<title[^>]*>(.*?)</title>", (html or "")[:6000], re.S | re.I)
    return (m.group(1).strip() if m else "")[:80]


BLOCK_MARKERS = ("/errors/validatecaptcha", "enter the characters you see below",
                 "type the characters you see in this image", "make sure you're not a robot",
                 "make sure you&#39;re not a robot", "to discuss automated access to amazon data")


def is_not_found(html):
    """Amazon's "Page Not Found" page. For some dead listings Amazon sends it with HTTP 503 instead of 404,
    which used to look like a robot check and paused the whole bot."""
    low = (html or "").lower()
    if 'id="producttitle"' in low:
        return False
    return "page not found" in page_title(html).lower()


def is_blocked(html, status=None):
    """Amazon's robot check: the captcha page / 'Continue shopping' page (both post to /errors/validateCaptcha),
    or the 503 'Sorry! Something went wrong' dog page it shows when it throttles you."""
    if status == 404 or is_not_found(html):
        return False           # "page not found" (e.g. a mistyped ASIN or a dead listing) is not a robot check
    low = (html or "").lower()
    real_page = 'id="producttitle"' in low or 's-search-result' in low
    if not real_page and any(m in low for m in BLOCK_MARKERS):
        return True
    if "robot check" in page_title(html).lower():
        return True
    return status == 503 and not real_page


def get_proxy():
    """Return a usable proxy URL. Accepts both 'user:pass@host:port' and 'http://user:pass@host:port'."""
    raw = (os.environ.get("PROXY") or PROXY or "").strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = "http://" + raw
    return raw


def proxies_for(proxy):
    return {"http": proxy, "https": proxy} if proxy else None


def search_url(term, page=1):
    url = f"{BASE}/s?k={quote_plus(term)}"
    return url + (f"&page={page}" if page > 1 else "")


def product_url(asin):
    return f"{BASE}/dp/{asin}"


def excluded(name):
    n = (name or "").lower()
    return any(w in n for w in EXCLUDE_WORDS)


def parse_asin(s):
    """Accepts an ASIN or a full Amazon link. Returns a clean ASIN or None."""
    s = (s or "").strip().strip("`<>")
    m = ASIN_IN_URL.search(s)
    if m:
        s = m.group(1)
    elif "/" in s:
        s = s.split("?")[0].rstrip("/").split("/")[-1]
    s = s.upper()
    return s if ASIN_RE.match(s) else None


def decide(prev, ok, explicit, first_run):
    """'Restock' / 'New Item' if an alert should fire, else None.
    prev: last known 'Amazon.ca in stock' (True/False/None). ok: it is now."""
    if not ok or prev is True:
        return None
    if first_run and not explicit:
        return None            # silent baseline on first start
    return "Restock" if (prev is False or explicit) else "New Item"


def money(v):
    return f"${v['price']:.2f}" if v.get("price") is not None else "an unknown price"


def seller_text(v):
    sold = v.get("seller") or "a third-party seller"
    ships = v.get("ships_from")
    if ships and ships.lower() != sold.lower():
        return f"{sold} (ships from {ships})"
    return sold


def is_ok(v):
    """The one rule that decides an alert."""
    if not v or not v.get("in_stock") or v.get("price") is None:
        return False
    return bool(v.get("internal")) or not ONLY_AMAZON_CA_SOLD


def asin_hint(asin):
    """Most common typo: the letter O instead of the number 0 (real ASINs start with B0)."""
    if asin and asin.startswith("BO"):
        return f" Check the ASIN: real ones start with B0 (zero), so it's probably B0{asin[2:]}."
    return " Check the ASIN, or paste the full amazon.ca link instead."


def reason_text(v, ok, status=None):
    if v is None and status == "HTTP 404":
        return "Amazon says this product page doesn't exist (HTTP 404), so I won't alert."
    if v is None:
        return (f"Couldn't read the product page ({status}), so I can't confirm the "
                f"price and seller and won't alert.")
    if ok:
        return f"Ships from and sold by Amazon.ca, in stock at {money(v)}."
    if not v.get("in_stock"):
        said = (v.get("availability") or "").split(". ")[0].rstrip(".")[:100]
        return ("Not in stock on the product page" +
                (f' (Amazon says: "{said}").' if said else " (no Add to Cart button)."))
    if v.get("price") is None:
        return "In stock, but I couldn't read the price on the product page, so I won't alert."
    return (f"In stock, but sold by {seller_text(v)} at {money(v)}, not Amazon.ca. "
            f"Waiting for Amazon.ca to sell it.")


# ------------------------------------------------------------ Amazon page parsing
def _text(el):
    return " ".join(el.get_text(" ", strip=True).split()) if el is not None else ""


def parse_money(s):
    s = (s or "").replace("\xa0", " ").strip()
    m = re.search(r"\d[\d,]*(?:\.\d{1,2})?", s)
    if not m:
        return None
    try:
        return float(m.group().replace(",", ""))
    except ValueError:
        return None


def _price_from(el):
    """Price from an Amazon .a-price block: the hidden .a-offscreen text, or whole + fraction."""
    if el is None:
        return None
    off = el.select_one(".a-offscreen")
    p = parse_money(_text(off)) if off is not None else None
    if p is not None:
        return p
    whole = el.select_one(".a-price-whole")
    if whole is not None:
        frac = el.select_one(".a-price-fraction")
        return parse_money(_text(whole).rstrip(".") + "." + (_text(frac) or "00"))
    return parse_money(_text(el))


def _buybox_price(soup):
    # The buy-box price. ".a-text-price" is the crossed-out list price, so it's skipped.
    for box in ("#corePrice_feature_div", "#corePriceDisplay_desktop_feature_div", "#apex_desktop",
                "#corePrice_desktop"):
        root = soup.select_one(box)
        if root is None:
            continue
        for el in root.select(".priceToPay, .apexPriceToPay, .a-price"):
            if "a-text-price" in (el.get("class") or []):
                continue
            p = _price_from(el)
            if p is not None:
                return p
    for sel in ("#price_inside_buybox", "#newBuyBoxPrice", "#priceblock_ourprice", "#priceblock_dealprice",
                "#priceblock_saleprice"):
        p = parse_money(_text(soup.select_one(sel)))
        if p is not None:
            return p
    inp = soup.select_one("input#twister-plus-price-data-price")
    if inp is not None:
        return parse_money(inp.get("value"))
    return None


def _seller_info(soup):
    """(sold_by, ships_from) from the buy box. Amazon has used three layouts over the years."""
    sold = ships = None
    # 1) current layout: "offer display" features
    el = soup.select_one('[offer-display-feature-name="desktop-merchant-info"] .offer-display-feature-text-message')
    if el is not None:
        sold = _text(el)
    el = soup.select_one('[offer-display-feature-name="desktop-fulfiller-info"] .offer-display-feature-text-message')
    if el is not None:
        ships = _text(el)
    # 2) "tabular buybox"
    if sold is None:
        el = soup.select_one('#tabular-buybox [tabular-attribute-name="Sold by"] .tabular-buybox-text-message')
        if el is not None:
            sold = _text(el)
    if ships is None:
        el = soup.select_one('#tabular-buybox [tabular-attribute-name="Ships from"] .tabular-buybox-text-message')
        if el is not None:
            ships = _text(el)
    # 3) older layout: one line of text
    if sold is None:
        line = _text(soup.select_one("#merchant-info"))
        m = re.search(r"ships from and sold by\s+(.+?)\.?$", line, re.I)
        if m:
            sold = ships = m.group(1).strip()
        else:
            m = re.search(r"sold by\s+(.+?)(?:\s+and\s+(?:fulfilled|ships)\s+(?:by|from)\s+(.+?))?\.?$", line, re.I)
            if m:
                sold = m.group(1).strip()
                ships = (m.group(2) or "").strip() or ships
    if sold is None:
        el = soup.select_one("#sellerProfileTriggerId")
        if el is not None:
            sold = _text(el)
    return sold or None, ships or None


def _is_amazon_ca(name):
    return (name or "").strip().rstrip(".").lower() == "amazon.ca"


OUT_WORDS = ("currently unavailable", "out of stock", "unavailable", "no featured offers",
             "not available")
IN_WORDS = ("in stock", "left in stock", "available to ship", "usually ships", "pre-order",
            "will be released", "ships within", "available on")


def parse_product(html):
    """Read an Amazon.ca product page. None if it isn't a readable product page."""
    soup = BeautifulSoup(html or "", "html.parser")
    name = _text(soup.select_one("#productTitle"))
    if not name:
        return None
    price = _buybox_price(soup)
    sold, ships = _seller_info(soup)
    mid = soup.select_one("input#merchantID, input[name='merchantID']")
    merchant_id = (mid.get("value") or "").strip() if mid is not None else ""

    availability = _text(soup.select_one("#availability")) or _text(soup.select_one("#outOfStock"))
    low = availability.lower()
    cart = soup.select_one("#add-to-cart-button, #buy-now-button, #preorder-button, "
                           "input[name='submit.add-to-cart'], input[name='submit.preorder']")
    if any(w in low for w in OUT_WORDS) and not any(w in low for w in IN_WORDS[:2]):
        in_stock = False
    elif soup.select_one("#outOfStock") is not None and cart is None:
        in_stock = False
    else:
        in_stock = cart is not None

    # Amazon.ca must be named as the seller (and as the shipper, when a shipper is shown).
    # The merchant ID is only a backup for when no seller text is on the page at all.
    if sold is not None:
        internal = _is_amazon_ca(sold) and (ships is None or _is_amazon_ca(ships))
    else:
        internal = merchant_id == AMAZON_CA_MERCHANT_ID
        if internal:
            sold = "Amazon.ca"

    qty = [int(o.get("value")) for o in soup.select("select#quantity option")
           if (o.get("value") or "").isdigit()]
    offer = soup.select_one("input#offerListingID, input[name='offerListingID'], input[name='items[0.base][offerListingId]']")
    img_el = soup.select_one("#landingImage, #imgBlkFront, #main-image")
    img = None
    if img_el is not None:
        img = img_el.get("data-old-hires") or img_el.get("src")
        if img and img.startswith("data:"):
            img = None
    return {
        "price": price,
        "name": name,
        "seller": sold,
        "ships_from": ships,
        "internal": internal,
        "merchant_id": merchant_id or None,
        "offer_id": ((offer.get("value") or "")[:60] or None) if offer is not None else None,
        "cart_limit": max(qty) if qty else None,
        "in_stock": in_stock,
        "availability": availability or None,
        "image": img,
    }


def parse_items(html):
    """Search result tiles. None if the page has no results grid at all."""
    soup = BeautifulSoup(html or "", "html.parser")
    tiles = soup.select('[data-component-type="s-search-result"][data-asin]')
    if not tiles and "s-search-result" not in (html or ""):
        return None
    items = []
    for t in tiles:
        asin = (t.get("data-asin") or "").strip().upper()
        if not ASIN_RE.match(asin):
            continue
        name = _text(t.select_one("h2"))
        price = None
        for el in t.select(".a-price"):
            if "a-text-price" not in (el.get("class") or []):
                price = _price_from(el)
                if price is not None:
                    break
        txt = _text(t).lower()
        img = t.select_one("img.s-image")
        items.append({
            "sku": asin,
            "name": name,
            "tile_price": price,
            "in_stock": price is not None and "currently unavailable" not in txt,
            "tile_seller": None,          # Amazon doesn't show the seller on search tiles
            "image": img.get("src") if img is not None else None,
            "url": product_url(asin),
        })
    return items


def referer_headers(referer):
    return {"Referer": referer} if referer else None


def page_excerpt(html, n=160):
    """The first visible words of a page, so a block message shows what Amazon actually displayed."""
    try:
        soup = BeautifulSoup(html or "", "html.parser")
        for t in soup(["script", "style", "noscript"]):
            t.decompose()
        return " ".join(soup.get_text(" ", strip=True).split())[:n]
    except Exception:
        return ""


def _check(r):
    if is_blocked(r.text, r.status_code):
        raise Blocked(f'HTTP {r.status_code}, page title "{page_title(r.text)}", '
                      f'page says: "{page_excerpt(r.text)}"')


async def fetch_home(session, proxy=None):
    r = await session.get(f"{BASE}/", timeout=20, proxies=proxies_for(proxy))
    _check(r)


async def fetch_items(session, term, proxy=None, page=1, referer=None):
    r = await session.get(search_url(term, page), timeout=20, proxies=proxies_for(proxy),
                          headers=referer_headers(referer))
    _check(r)
    items = parse_items(r.text)
    if items is None:
        print(f"[{term}] no search results (status {r.status_code}). "
              f"Page title: {page_title(r.text)!r}")
    return items


async def get_verified(session, asin, proxy, referer=None):
    r = await session.get(product_url(asin), timeout=20, proxies=proxies_for(proxy),
                          headers=referer_headers(referer))
    _check(r)
    status = 404 if is_not_found(r.text) else r.status_code   # dead listing, even when Amazon says 503
    return parse_product(r.text), status, r.text


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def save_json(path, obj):
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as f:
            json.dump(obj, f)
    except Exception as e:
        print(f"[warning] couldn't save {path}: {e}")


# ------------------------------------------------------------ real browser (same as the Walmart bot)
class PageResult:
    def __init__(self, text, status_code):
        self.text = text
        self.status_code = status_code


def browser_proxy(proxy):
    if not proxy:
        return None
    u = urlsplit(proxy)
    out = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        out["username"] = unquote(u.username)
        out["password"] = unquote(u.password or "")
    return out


class BrowserSession:
    """Chrome with the same get() shape as the curl session. One tab, reused, so cookies carry over
    between pages like in a normal browser."""

    RECYCLE_PAGES = 30     # restart Chrome (keeping cookies) this often so its memory doesn't keep growing

    def __init__(self, proxy=None):
        self.proxy = proxy
        self._pw = self._browser = self._ctx = self._page = None
        self._crashed = False
        self._closing = False
        self._pages = 0
        self._lock = asyncio.Lock()

    async def _cookies(self):
        try:
            if self._ctx is not None and self._browser.is_connected():
                return await asyncio.wait_for(self._ctx.storage_state(), 15)
        except Exception:
            pass
        return None

    async def _restart(self, why):
        state = await self._cookies()
        print(f"[browser] restarting Chrome ({why}){', keeping cookies' if state else ''}")
        await self.close()
        await self._start(state)

    def _alive(self):
        return (self._page is not None and not self._crashed and not self._page.is_closed()
                and self._browser.is_connected())

    def _on_crash(self, _page):
        self._crashed = True
        print("[browser] the Amazon tab crashed (usually means Chrome ran out of memory)")

    async def _start(self, storage_state=None):
        try:
            await asyncio.wait_for(self._start_inner(storage_state), 60)
        except asyncio.TimeoutError:
            await self.close()
            raise TimeoutError("Chrome didn't start within 60 seconds")

    async def _start_inner(self, storage_state=None):
        from playwright.async_api import async_playwright
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(
            channel="chromium", headless=HEADLESS, proxy=browser_proxy(self.proxy),
            # Images are switched off with a flag, not request blocking: blocking requests in Playwright
            # turns off the browser cache, so the site's scripts would be re-downloaded on every page.
            args=["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage",
                  "--disable-extensions", "--blink-settings=imagesEnabled=false",
                  "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
                  "--webrtc-ip-handling-policy=disable_non_proxied_udp",
                  "--renderer-process-limit=1", "--disable-site-isolation-trials",
                  "--disable-features=site-per-process,IsolateOrigins,Translate,BackForwardCache,"
                  "MediaRouter,OptimizationHints",
                  "--disable-background-networking", "--disable-component-update", "--no-first-run"])
        self._closing = False
        self._browser.on("disconnected", lambda _b: None if self._closing else
                         print("[browser] Chrome closed unexpectedly (often: Railway ran out of memory)"))
        major = self._browser.version.split(".")[0]
        ua = (f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
              f"Chrome/{major}.0.0.0 Safari/537.36")
        self._ctx = await self._browser.new_context(
            user_agent=ua, locale="en-CA", timezone_id="America/Toronto",
            viewport={"width": 1366, "height": 768}, storage_state=storage_state)
        self._page = await self._ctx.new_page()
        self._page.on("crash", self._on_crash)
        self._crashed = False
        self._pages = 0

    async def _dismiss_continue_shopping(self, wanted_url):
        """Amazon's soft check: 'Click the button below to continue shopping'.
        There is no image to solve — the button is already filled in. Click it once.
        An image captcha (type the characters) is left alone so the existing block path still handles it.
        After a successful click Amazon often lands on the homepage, so go back to the page we wanted."""
        for _ in range(2):
            html = await self._page.content()
            low = html.lower()
            if "captchacharacters" in low or "type the characters you see" in low:
                return html
            soft = (
                "click the button below to continue shopping" in low
                or (
                    "continue shopping" in low
                    and ("validatecaptcha" in low or 'id="checkpointform"' in low or "/checkpoint/validate" in low)
                )
            )
            if not soft:
                return html
            btn = self._page.locator(
                "form[action*='validateCaptcha'] button[type='submit'], "
                "form[action*='validate'] button[type='submit'], "
                "button.a-button-text:has-text('Continue shopping')"
            ).first
            try:
                if await btn.count() == 0:
                    return html
                print("[browser] Amazon continue-shopping check — clicking the button")
                await btn.click(timeout=8000)
                try:
                    await self._page.wait_for_load_state("domcontentloaded", timeout=20000)
                except Exception:
                    pass
                await self._page.wait_for_timeout(random.uniform(1500, 3000))
            except Exception as e:
                print(f"[browser] continue-shopping click failed: {type(e).__name__}: {str(e)[:200]}")
                return await self._page.content()
            here = (self._page.url or "").split("?")[0].rstrip("/")
            want = (wanted_url or "").split("?")[0].rstrip("/")
            if want and want != here and "/errors/" not in want:
                try:
                    print(f"[browser] continue-shopping sent us to {here or 'unknown'}, going back to the page we wanted")
                    await self._page.goto(wanted_url, wait_until="domcontentloaded", timeout=30000)
                    await self._page.wait_for_timeout(random.uniform(1500, 3000))
                except Exception as e:
                    print(f"[browser] return after continue-shopping failed: {type(e).__name__}: {str(e)[:200]}")
                    break
        return await self._page.content()

    async def get(self, url, headers=None, timeout=20, proxies=None):
        """The whole page load has a time limit; if Chrome hangs anyway, give up on it and start a new one next time."""
        async with self._lock:
            try:
                return await asyncio.wait_for(self._get_inner(url, headers, timeout), timeout + 240)
            except asyncio.TimeoutError:
                print(f"[browser] Chrome stopped responding while loading {url}; it will be replaced")
                self._crashed = True
                raise TimeoutError(f"Chrome stopped responding while loading {url}")

    async def _get_inner(self, url, headers, timeout):
        if self._page is None:
            await self._start()
        elif not self._alive():
            await self._restart("it crashed, closed or stopped responding")
        elif self._pages >= self.RECYCLE_PAGES:
            await self._restart("routine memory cleanup")
        referer = (headers or {}).get("Referer")
        try:
            resp = await self._page.goto(url, referer=referer, wait_until="domcontentloaded",
                                         timeout=(timeout + 10) * 1000)
            await self._page.wait_for_timeout(random.uniform(1500, 3500))   # let the page finish loading
            html = await self._dismiss_continue_shopping(url)
        except Exception as e:
            print(f"[browser] loading {url} failed: {type(e).__name__}: {str(e)[:300]}")
            raise                      # a dead Chrome is restarted on the next request
        finally:
            self._pages += 1
        try:
            await self._page.goto("about:blank", timeout=10000)   # free the heavy page while we wait
        except Exception:
            pass
        return PageResult(html, resp.status if resp else 0)

    async def describe(self):
        """Startup check through Chrome itself: version, window mode, and the IP Amazon will see."""
        async with self._lock:
            if not self._alive():
                await self.close()
                await self._start()
            try:
                await self._page.goto("https://api.ipify.org", timeout=20000)
                ip = (await self._page.inner_text("body")).strip()[:60]
            except Exception as e:
                ip = f"check failed ({type(e).__name__}: {str(e).splitlines()[0][:120]})"
            try:
                await self._page.wait_for_timeout(500)
                await self._page.goto("about:blank", timeout=10000)
            except Exception:
                pass
            mode = "hidden (headless)" if HEADLESS else "normal window on a virtual screen"
            line = f"Chrome {self._browser.version}, {mode}. Chrome's own outbound IP: `{ip}`"
            print(f"[browser] {line}")
            return line

    async def close(self):
        """Close Chrome, but never wait more than 15 seconds per step for a hung one."""
        self._closing = True
        for obj in (self._ctx, self._browser):
            if obj is not None:
                try:
                    await asyncio.wait_for(obj.close(), 15)
                except Exception:
                    pass
        if self._pw is not None:
            try:
                await asyncio.wait_for(self._pw.stop(), 15)
            except Exception:
                pass
        self._pw = self._browser = self._ctx = self._page = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()


def make_session(proxy=None):
    if FETCHER == "curl":
        return AsyncSession(impersonate="chrome")
    return BrowserSession(proxy)


# ------------------------------------------------------------ dashboard link
class Dashboard:
    """Talks to your Lovable dashboard. Headers on every request: x-bot-key, plus x-bot-store: amazon so the
    dashboard can give this bot its own Pause button and online indicator, separate from the Walmart bot."""

    def __init__(self, base_url, key, session=None):
        self.base = (base_url or "").rstrip("/")
        self.headers = {"x-bot-key": key, "x-bot-store": "amazon"}
        self.session = session if session is not None else AsyncSession(impersonate="chrome")

    async def get_config(self):
        r = await self.session.get(f"{self.base}/api/public/bot/config", headers=self.headers, timeout=15)
        if r.status_code in (401, 403):
            raise RuntimeError(f"HTTP {r.status_code}: the dashboard rejected BOT_API_KEY (check the Railway variable)")
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")
        return r.json()

    async def post_log(self, kind, content, sku=None):
        body = {"kind": kind, "content": (content or "")[:1500]}
        if sku:
            body["sku"] = sku
        r = await self.session.post(f"{self.base}/api/public/bot/log", headers=self.headers, json=body, timeout=15)
        if r.status_code >= 300:
            raise RuntimeError(f"HTTP {r.status_code}")


class LogShipper:
    """Sends log lines to the dashboard in the background so Amazon checks never wait on it."""

    def __init__(self, dashboard, sleep=asyncio.sleep):
        self.dash = dashboard
        self.sleep = sleep
        self.q = asyncio.Queue(maxsize=300)

    def push(self, kind, content, sku=None):
        item = (kind, content, sku)
        try:
            self.q.put_nowait(item)
        except asyncio.QueueFull:
            try:
                self.q.get_nowait()       # drop the oldest
                self.q.put_nowait(item)
            except Exception:
                pass

    async def run(self):
        while True:
            kind, content, sku = await self.q.get()
            for attempt in range(3):
                try:
                    await self.dash.post_log(kind, content, sku)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"[dashboard] couldn't send a log line ({type(e).__name__}: {e}), try {attempt + 1}/3")
                    await self.sleep(5 * (attempt + 1))


# ------------------------------------------------------------ the monitor
class Monitor:
    """All the logic. Talks to Discord only through send_alert / send_text callbacks."""

    def __init__(self, session, send_alert, send_text, proxy=None,
                 clock=time.monotonic, sleep=asyncio.sleep, rand=random.uniform,
                 dashboard=None, shipper=None, session_factory=None, send_maint=None):
        self.session = session
        self.send_maint = send_maint
        self.session_factory = session_factory
        self.referer = None
        self.dashboard = dashboard
        self.shipper = shipper
        self.synced_once = False
        self._err_seen = {}
        self.send_alert = send_alert
        self.send_text = send_text
        self.proxy = proxy
        self.clock = clock
        self.sleep = sleep
        self.rand = rand
        self.alive_at = clock()
        self.exit = os._exit

        st = load_json(STATE_FILE)
        self.first_run = st is None
        self.state = st if isinstance(st, dict) else {}
        cfg = load_json(CONFIG_FILE)
        if isinstance(cfg, dict) and isinstance(cfg.get("watch"), list):
            self.watch = [parse_asin(s) for s in cfg["watch"] if parse_asin(s)]
        elif dashboard is not None:
            self.watch = []        # the dashboard decides; start empty until the first sync
        else:
            self.watch = [s for s in (parse_asin(x) for x in WATCH_ASINS) if s]
        # Discord !watch / !remove made while the dashboard is connected (the bot can't write them to the dashboard).
        self.discord_added = set(cfg.get("discord_added") or []) if isinstance(cfg, dict) else set()
        self.discord_removed = set(cfg.get("discord_removed") or []) if isinstance(cfg, dict) else set()
        self.dash_last = None          # the dashboard's ASIN list at the last sync
        self.dash_paused_last = None   # the dashboard's pause setting at the last sync
        # Pace, changeable with !speed. Saved in the config file; starts from GAP_MIN/GAP_MAX/MIN_SECONDS_PER_ROUND.
        speed = cfg.get("speed") if isinstance(cfg, dict) else None
        speed = speed if isinstance(speed, dict) else {}
        self.gap_min = float(speed.get("gap_min", GAP_MIN))
        self.gap_max = float(speed.get("gap_max", GAP_MAX))
        self.min_round = float(speed.get("min_round", MIN_SECONDS_PER_ROUND))

        self.paused = False
        self.blocked_until = 0.0
        self.backoff = BACKOFF_START
        self.was_blocked = False
        self.blocks_in_row = 0
        self.block_started = self.last_block_note = 0.0
        self.block_places = {}
        self.good_pages = 0
        self.last_request = clock() - GAP_MAX
        self.requests_made = 0
        self.round = deque()
        self.round_open = False
        self.next_round_at = 0.0
        self.round_no = 0
        self.priority = deque([("home",)])
        self.refresh = False
        self.restart_chrome = False
        self.reasons = {}      # asin -> (text, time string)
        self.names = {}
        self.checked = {}      # asin -> clock time of last product-page check
        self.verify_budget = MAX_VERIFY_PER_ROUND

    # ---- small helpers
    def dash_log(self, kind, content, sku=None):
        if self.shipper:
            self.shipper.push(kind, LOG_TAG + re.sub(r"[*`]", "", content), sku)

    def dash_error(self, key, content, sku=None):
        """Every error goes to the maintenance channel; the dashboard gets the same one at most every ERROR_REPEAT_SECONDS."""
        self.maint_channel(content)
        now = self.clock()
        if now - self._err_seen.get(key, -1e9) < ERROR_REPEAT_SECONDS:
            return
        self._err_seen[key] = now
        self.dash_log("error", content, sku)

    def maint_channel(self, text):
        """Post to the Discord maintenance channel (errors, blocks, startup info), without making the caller wait."""
        if self.send_maint is None:
            return

        async def send():
            try:
                await self.send_maint(text)
            except Exception as e:
                print(f"[discord] couldn't send to the maintenance channel: {e}")
        try:
            asyncio.get_running_loop().create_task(send())
        except RuntimeError:
            pass

    async def _say(self, text, kind="info", sku=None, to="main"):
        """Status messages, always copied to the dashboard.
        to="main": the main channel (only when there's no dashboard, or STATUS_IN_DISCORD=1).
        to="maint": the maintenance channel, never the main channel.
        to="dash": dashboard only (the maintenance channel already got its own copy)."""
        print(f"[status] {text}")
        if to == "maint":
            self.maint_channel(text)
        elif to == "main" and (not self.shipper or STATUS_IN_DISCORD):
            try:
                await self.send_text(text)
            except Exception as e:
                print(f"[discord] couldn't send message: {e}")
        self.dash_log(kind, text, sku)

    async def _alert(self, alert):
        try:
            await self.send_alert(alert)
        except Exception as e:
            print(f"[discord] couldn't send alert: {e}")
        v = alert["v"]
        self.dash_log("message",
                      f"{alert['kind']}: {alert['name']} | {money(v)} | Seller: {seller_text(v)} | "
                      f"ASIN {alert['sku']} | {alert['why']}", alert["sku"])

    def set_reason(self, asin, text):
        changed = self.reasons.get(asin, ("",))[0] != text
        self.reasons[asin] = (text, local_now().strftime("%I:%M:%S %p"))
        return changed

    def save_config(self):
        save_json(CONFIG_FILE, {"watch": self.watch, "speed": {"gap_min": self.gap_min, "gap_max": self.gap_max,
                                                               "min_round": self.min_round},
                                "discord_added": sorted(self.discord_added),
                                "discord_removed": sorted(self.discord_removed)})

    def pace_text(self):
        return f"{self.gap_min:g}-{self.gap_max:g}s"

    def round_estimate(self):
        jobs = len(self.watch)
        if SEARCH_SCAN:
            jobs += len(SEARCH_TERMS) * SEARCH_PAGES
        elif BROWSE_FIRST and jobs:
            jobs += 1
        return max(int(jobs * (self.gap_min + self.gap_max) / 2), int(self.min_round) if jobs else 0)

    async def wait_turn(self):
        gap = self.rand(self.gap_min, self.gap_max)
        wait = self.last_request + gap - self.clock()
        if wait > 0:
            await self.sleep(wait)
        self.last_request = self.clock()
        self.requests_made += 1

    # ---- rounds
    def build_round(self):
        jobs = []
        if SEARCH_SCAN and self.round_no % SEARCH_EVERY_N_ROUNDS == 0:
            for term in SEARCH_TERMS:
                for page in range(1, SEARCH_PAGES + 1):
                    jobs.append(("search", term, page))
        elif BROWSE_FIRST and self.watch:
            jobs.append(("browse", random.choice(SEARCH_TERMS), 1))
        sku_jobs = [("sku", asin, False) for asin in self.watch]
        random.shuffle(sku_jobs)              # different order every round, less robotic
        jobs.extend(sku_jobs)
        self.round = deque(jobs)
        self.next_round_at = self.clock() + self.min_round
        self.round_no += 1
        self.verify_budget = MAX_VERIFY_PER_ROUND

    def finish_round(self):
        self.first_run = False
        save_json(STATE_FILE, self.state)

    def next_job(self):
        if self.refresh:
            self.refresh = False
            self.round_no = 0            # a refresh always includes the search scan
            self.build_round()
            self.round_open = True
        if self.priority:
            return self.priority.popleft()
        if not self.round:
            if self.round_open:
                self.finish_round()
                self.round_open = False
            if self.clock() < self.next_round_at:
                return None              # idle: don't re-check the same pages too soon
            self.build_round()
            self.round_open = True
        return self.round.popleft() if self.round else None

    async def run_watchdog(self):
        """If the check loop stops moving (a hung browser call), report it and restart the whole bot.
        Railway starts it again on its own (railway.json: restartPolicyType ON_FAILURE)."""
        while True:
            await self.sleep(60)
            stuck = self.clock() - self.alive_at
            if stuck < WATCHDOG_SECONDS:
                continue
            msg = (f"The Amazon bot froze: its check loop hasn't moved for {stuck / 60:.0f} min (not paused, not "
                   f"waiting out a block). Restarting the bot now; Railway will start it again.")
            print(f"[watchdog] {msg}")
            self.maint_channel(msg)
            self.dash_log("error", msg)
            await asyncio.sleep(5)       # give the messages a moment to go out
            self.exit(1)

    async def run(self):
        while True:
            job = None
            self.alive_at = self.clock()   # heartbeat for the watchdog: the loop passes here every few seconds
            try:
                if self.restart_chrome:
                    self.restart_chrome = False
                    await self._restart_chrome()
                if self.paused:
                    await self.sleep(1)
                    continue
                remaining = self.blocked_until - self.clock()
                if remaining > 0:
                    await self.sleep(min(5, remaining))
                    continue
                job = self.next_job()
                if job is None:
                    await self.sleep(5)
                    continue
                await self.wait_turn()
                if self.paused:                      # paused while we were waiting
                    self.priority.appendleft(job)
                    continue
                await self.run_job(job)
                self.good_pages += 1
                if self.was_blocked:
                    self.was_blocked = False
                    self.backoff = BACKOFF_START
                    mins = (self.clock() - self.block_started) / 60
                    tries = (f" after {self.blocks_in_row} blocked tries over {mins:.0f} min"
                             if self.blocks_in_row > 1 else "")
                    places = ", ".join(f"{p} x{n}" if n > 1 else p
                                       for p, n in self.block_places.items())
                    await self._say(f"Back to normal{tries}: Amazon is returning real pages again, resuming checks. "
                                    f"Blocked on: {places}. Got through on: {page_name(job)}.", to="maint")
            except Blocked as b:
                first_block = not self.was_blocked
                self.was_blocked = True
                self.blocks_in_row = 1 if first_block else self.blocks_in_row + 1
                where = page_name(job)
                if first_block:
                    self.block_places = {}
                self.block_places[where] = self.block_places.get(where, 0) + 1
                good, self.good_pages = self.good_pages, 0
                wait = (f"{self.backoff / 60:.0f} min" if self.backoff >= 120 else f"{self.backoff:.0f} seconds")
                self.blocked_until = self.clock() + self.backoff
                if job is not None and job[0] != "home":
                    self.priority.appendleft(job)
                    if job[0] in ("sku", "verify"):
                        self.set_reason(job[1], BLOCK_REASON)
                await self.reset_session()
                msg = (f"Amazon is showing its robot-check page to this connection. Blocked on: {where}, "
                       f"after {good} page(s) loaded fine. I'm pausing ALL requests for {wait}, "
                       f"then I'll retry on my own. Amazon returned: {b}.")
                print(f"[BLOCKED] (#{self.blocks_in_row} in a row) {msg}")
                self.maint_channel(f"BLOCKED (#{self.blocks_in_row} in a row): {msg}")
                now = self.clock()
                if first_block:          # dashboard: one message when a block starts, then an update every 10 minutes
                    self.block_started = self.last_block_note = now
                    await self._say(msg, kind="error", to="dash")
                elif now - self.last_block_note >= BLOCK_UPDATE_SECONDS:
                    self.last_block_note = now
                    await self._say(f"Still blocked by Amazon: {(now - self.block_started) / 60:.0f} min so far, "
                                    f"{self.blocks_in_row} tries in a row. Last blocked on: {where}. "
                                    f"Next retry in {wait}.", kind="error", to="maint")
                self.backoff = min(self.backoff * 2, BACKOFF_MAX)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[error] {type(e).__name__}: {e}")
                self.dash_error(f"exc:{type(e).__name__}", f"Unexpected error in the bot: {type(e).__name__}: {e}")
                await self.sleep(5)

    async def _restart_chrome(self):
        """Close the current Chrome and open a new one. Used by !restartchrome."""
        print("[browser] !restartchrome: closing Chrome and opening a new one")
        try:
            await self.session.close()
        except Exception as e:
            print(f"[browser] close during restart failed: {type(e).__name__}: {e}")
        if self.session_factory is not None:
            self.session = self.session_factory()
        self.referer = None

    async def reset_session(self):
        """After a block, go back through the homepage; only throw away the browser if FRESH_BROWSER_ON_BLOCK."""
        self.referer = None
        if FRESH_BROWSER_ON_BLOCK and self.session_factory is not None:
            try:
                await self.session.close()
            except Exception:
                pass
            self.session = self.session_factory()
        if not any(j[0] == "home" for j in self.priority):
            self.priority.appendleft(("home",))

    async def run_job(self, job):
        kind = job[0]
        if kind == "home":
            await fetch_home(self.session, self.proxy)
            self.referer = f"{BASE}/"
        elif kind == "browse":
            await fetch_items(self.session, job[1], self.proxy, job[2], referer=self.referer)
            self.referer = search_url(job[1], job[2])
        elif kind == "search":
            await self.do_search(job[1], job[2])
            self.referer = search_url(job[1], job[2])
        elif kind == "sku":
            await self.check_sku(job[1], explicit=True, confirm=job[2])
        elif kind == "verify":
            await self.check_sku(job[1], explicit=False, meta=job[2])

    # ---- search scan: spots new listings / tiles that flip to in-stock
    def schedule_verify(self, asin, it):
        """True if handled (queued, already covered, or recently checked). False if out of budget."""
        if asin in self.watch:
            return True
        if any(j[0] == "verify" and j[1] == asin for j in self.priority):
            return True
        if self.clock() - self.checked.get(asin, -1e9) < RECHECK_SECONDS:
            return True
        if self.verify_budget <= 0:
            return False
        self.verify_budget -= 1
        self.priority.append(("verify", asin, {"name": it["name"], "url": it["url"],
                                               "img": it["image"], "tries": 0}))
        return True

    async def do_search(self, term, page):
        items = await fetch_items(self.session, term, self.proxy, page, referer=self.referer)
        if items is None:
            return
        for it in items:
            asin = it["sku"]
            if not asin or excluded(it["name"]):
                continue
            self.names[asin] = it["name"]
            prev = self.state.get(asin)
            if not isinstance(prev, dict):
                if self.first_run:
                    self.state[asin] = {"in_stock": it["in_stock"]}
                elif self.schedule_verify(asin, it):
                    self.state[asin] = {"in_stock": it["in_stock"]}
            elif (not prev["in_stock"]) and it["in_stock"]:
                if self.schedule_verify(asin, it):
                    prev["in_stock"] = True
            else:
                prev["in_stock"] = it["in_stock"]

    # ---- the product-page check (the only thing that can trigger an alert)
    async def check_sku(self, asin, explicit, confirm=False, meta=None):
        meta = meta or {}
        try:
            v, status, _ = await get_verified(self.session, asin, self.proxy, referer=self.referer)
            status = f"HTTP {status}"
        except Blocked:
            raise
        except Exception as e:
            detail = (str(e).strip().splitlines() or [""])[0][:150]
            v, status = None, f"error: {type(e).__name__}{': ' + detail if detail else ''}"
        self.checked[asin] = self.clock()

        if v is None:
            reason = reason_text(None, False, status)
            if status == "HTTP 404":
                reason += asin_hint(asin)
            if self.set_reason(asin, reason):
                print(f"[not posted] {asin}: {reason}")
            self.dash_error(f"unreadable:{asin}", f"{asin}: {reason}", asin)
            if not explicit and meta.get("tries") is not None:
                tries = meta["tries"] + 1
                if tries < MAX_VERIFY_TRIES:
                    self.priority.append(("verify", asin, dict(meta, tries=tries)))
            if confirm:
                await self._say(f"Added `{asin}` to the watchlist, but: {reason}", kind="error", sku=asin, to="maint")
            return

        name = v["name"] or meta.get("name") or self.names.get(asin) or asin
        self.names[asin] = name
        ok = is_ok(v)
        key = f"watch:{asin}"
        prev = self.state.get(key)
        self.state[key] = ok
        prev_reason = self.reasons.get(asin, (None,))[0]
        reason = reason_text(v, ok)
        if self.set_reason(asin, reason):
            print(f"[{'watch' if explicit else 'verify'}] {asin}: {reason}")

        kind = decide(prev, ok, explicit, self.first_run)
        if kind:
            if kind == "Restock":
                why = reason
                if prev_reason and prev_reason != reason and prev_reason != BLOCK_REASON:
                    why += f" Before: {prev_reason}"
            else:
                why = "New listing found. " + reason
            await self._alert({
                "kind": kind, "name": name, "sku": asin, "v": v, "why": why,
                "url": product_url(asin), "img": meta.get("img"),
            })
        if confirm:
            await self._say(f"Now watching **{name}** (`{asin}`). Right now: {reason}", sku=asin)

    # ---- dashboard sync
    def apply_config(self, cfg):
        """Apply the dashboard's settings: pause state + the Amazon ASIN list (store starting with 'amazon')."""
        if not isinstance(cfg, dict):
            return
        paused = bool(cfg.get("paused"))
        raw = cfg.get("skus")
        if isinstance(raw, list):
            new, labels = [], {}
            for item in raw:
                if not isinstance(item, dict):
                    continue
                store = str(item.get("store") or "").lower()
                if not store.startswith("amazon"):
                    continue                      # Walmart / EB Games / Hobbies belong to other bots
                asin = parse_asin(str(item.get("sku") or ""))
                if not asin:
                    self.dash_error(f"badsku:{item.get('sku')}",
                                    f"Ignoring '{item.get('sku')}': that isn't a valid Amazon ASIN "
                                    f"(10 letters/numbers, like B0ABCD1234, or an amazon.ca link).")
                    continue
                if asin not in new:
                    new.append(asin)
                label = str(item.get("label") or "").strip()
                if label:
                    labels[asin] = label
            for asin, label in labels.items():
                self.names.setdefault(asin, label)
            # Discord commands work too, so apply only what CHANGED on the dashboard since the last sync;
            # the newest change wins, whether it came from Discord or the dashboard.
            if self.dash_last is None:
                target = [x for x in new if x not in self.discord_removed]
                target += [x for x in sorted(self.discord_added) if x not in target and x not in self.discord_removed]
            else:
                target = list(self.watch)
                for x in new:
                    if x not in self.dash_last:            # added on the dashboard
                        self.discord_removed.discard(x)
                        if x not in target:
                            target.append(x)
                for x in self.dash_last:
                    if x not in new:                       # removed on the dashboard
                        self.discord_added.discard(x)
                        if x in target:
                            target.remove(x)
            self.dash_last = new
            added = [x for x in target if x not in self.watch]
            removed = [x for x in self.watch if x not in target]
            self.watch = target
            for asin in added:
                self.priority.append(("sku", asin, self.synced_once))   # confirm message only after the first sync
            for asin in removed:
                self.round = deque(j for j in self.round if not (j[0] == "sku" and j[1] == asin))
                self.priority = deque(j for j in self.priority if not (j[0] in ("sku", "verify") and j[1] == asin))
            if added or removed:
                self.save_config()
                if self.synced_once:
                    self.dash_log("info", f"Watchlist updated from the dashboard: {len(added)} added, "
                                          f"{len(removed)} removed, {len(target)} total.")
        # Only a CHANGE of the dashboard's pause button applies, so a Discord !pause / !resume isn't undone a minute later.
        dash_changed = self.dash_paused_last is None or paused != self.dash_paused_last
        self.dash_paused_last = paused
        if dash_changed and paused != self.paused:
            self.paused = paused
            if self.synced_once or paused:
                note = ("Paused from the dashboard. No requests to Amazon until you resume."
                        if paused else "Resumed from the dashboard.")
                print(f"[dashboard] {note}")
                self.dash_log("info", note)
        self.synced_once = True

    async def run_sync(self):
        """Every ~minute: ask the dashboard for settings (this also counts as the check-in)."""
        if not self.dashboard:
            return
        fails = 0
        while True:
            try:
                cfg = await self.dashboard.get_config()
                self.apply_config(cfg)
                if fails >= 3:
                    print("[dashboard] connection restored")
                fails = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                fails += 1
                print(f"[dashboard] couldn't get settings ({type(e).__name__}: {e}); keeping the last settings")
            await self.sleep(self.rand(*CONFIG_EVERY_SECONDS))

    # ---- Discord commands (returns reply text, or None if not a command)
    def command(self, text):
        parts = (text or "").strip().split()
        if not parts or not parts[0].startswith("!"):
            return None
        cmd, args = parts[0][1:].lower(), parts[1:]

        dash_note = (" (Changed from Discord: the dashboard won't show this, but the bot keeps it until it's "
                     "changed again in Discord or on the dashboard.)") if self.dashboard else ""

        if cmd in ("watch", "add"):
            if not args:
                return "Usage: `!watch ASIN` (an ASIN like B0ABCD1234, or an amazon.ca link). You can list several."
            added, already, bad = [], [], []
            for a in args:
                asin = parse_asin(a)
                if not asin:
                    bad.append(a)
                elif asin in self.watch:
                    already.append(asin)
                else:
                    self.watch.append(asin)
                    self.priority.append(("sku", asin, True))
                    self.discord_added.add(asin)
                    self.discord_removed.discard(asin)
                    added.append(asin)
            if added:
                self.save_config()
            out = []
            if added:
                tail = " Resume with `!resume` to get the confirmation." if self.paused else \
                       " I'll check it next and tell you what it is and whether Amazon.ca is selling it."
                out.append("Added: " + ", ".join(f"`{s}`" for s in added) + "." + tail + dash_note)
            if already:
                out.append("Already watching: " + ", ".join(f"`{s}`" for s in already) + ".")
            if bad:
                out.append("Doesn't look like an ASIN: " + ", ".join(f"`{b}`" for b in bad) + ".")
            return " ".join(out)

        if cmd in ("remove", "unwatch", "delete"):
            if not args:
                return "Usage: `!remove ASIN`"
            removed, missing = [], []
            for a in args:
                asin = parse_asin(a)
                if asin and asin in self.watch:
                    self.watch.remove(asin)
                    self.round = deque(j for j in self.round if not (j[0] == "sku" and j[1] == asin))
                    self.priority = deque(j for j in self.priority if not (j[0] in ("sku", "verify") and j[1] == asin))
                    self.discord_removed.add(asin)
                    self.discord_added.discard(asin)
                    removed.append(asin)
                else:
                    missing.append(a)
            if removed:
                self.save_config()
            out = []
            if removed:
                out.append("Removed: " + ", ".join(f"`{s}`" for s in removed) + "." + dash_note)
            if missing:
                out.append("Wasn't watching: " + ", ".join(f"`{m}`" for m in missing) + ".")
            return " ".join(out)

        if cmd == "list":
            if not self.watch:
                return "Not watching anything. Add one with `!watch ASIN`."
            lines = []
            for asin in self.watch:
                name = (self.names.get(asin) or "name unknown yet")[:50]
                r = self.reasons.get(asin)
                lines.append(f"`{asin}` {name}: {r[0] if r else 'not checked yet'}")
            return f"Watching {len(self.watch)}:\n" + "\n".join(lines)

        if cmd == "pause":
            self.paused = True
            return "Paused. I'm making NO requests to Amazon until you send `!resume`." + dash_note

        if cmd == "resume":
            self.paused = False
            return f"Resumed. Checks continue (each request {self.pace_text()} apart)." + dash_note

        if cmd == "speed":
            if not args:
                return (f"Right now: each request is {self.pace_text()} apart, and each round (every ASIN once) "
                        f"takes at least {self.min_round:g}s, so each ASIN is checked about every "
                        f"{self.round_estimate()}s.\n"
                        f"Change it: `!speed MIN MAX` (seconds between requests), e.g. `!speed 3 5`. "
                        f"Add a third number to change the shortest round too, e.g. `!speed 3 5 20`.")
            try:
                nums = [float(a) for a in args[:3]]
            except ValueError:
                return "Use numbers, e.g. `!speed 3 5` or `!speed 3 5 20`."
            if len(nums) < 2:
                return "Give both numbers, e.g. `!speed 3 5` (seconds between requests: shortest and longest)."
            lo, hi = nums[0], nums[1]
            if lo < 3 or hi < lo or hi > 600:
                return ("The first number must be at least 3 (this bot never goes below 3 seconds), the second "
                        "at least the first, and at most 600.")
            if len(nums) == 3 and not 0 <= nums[2] <= 3600:
                return "The round time must be between 0 and 3600 seconds."
            self.gap_min, self.gap_max = lo, hi
            if len(nums) == 3:
                self.min_round = nums[2]
            self.save_config()
            return (f"Done: each request is now {self.pace_text()} apart, rounds take at least "
                    f"{self.min_round:g}s, so each ASIN is checked about every {self.round_estimate()}s. "
                    f"Faster checking means Amazon challenges the bot sooner; watch the maintenance channel. "
                    f"`!refresh` puts it back to normal ({GAP_MIN:g}-{GAP_MAX:g}s).")

        if cmd == "restartchrome":
            if self.session_factory is None:
                return "This bot isn't running Chrome, so there's nothing to restart."
            self.restart_chrome = True
            return "Restarting Chrome. The check in progress finishes or fails, then a new window opens and checks continue."

        if cmd == "refresh":
            # Also undoes any !speed change: back to the bot's normal pace.
            self.gap_min, self.gap_max, self.min_round = float(GAP_MIN), float(GAP_MAX), float(MIN_SECONDS_PER_ROUND)
            self.save_config()
            self.refresh = True
            return (f"Refresh: speed is back to normal ({self.pace_text()} between requests, rounds at least "
                    f"{self.min_round:g}s), and I'm starting a fresh round now "
                    f"({'search scan + ' if SEARCH_SCAN else ''}every watched ASIN). The first request goes out "
                    f"within {self.gap_max:g} seconds, and a full round takes about {self.round_estimate()} seconds.")

        if cmd == "status":
            if self.paused:
                state = "PAUSED"
            elif self.was_blocked:
                state = (f"BLOCKED by Amazon for {(self.clock() - self.block_started) / 60:.0f} min "
                         f"({self.blocks_in_row} tries in a row), retrying on its own")
            else:
                state = "running"
            since = int(self.clock() - self.last_request)
            return (f"Status: {state}\nWatching {len(self.watch)} ASIN(s), each checked about every "
                    f"{self.round_estimate()}s (requests spaced {self.pace_text()} apart).\n"
                    f"Requests made since start: {self.requests_made}. Last request {since}s ago.\n"
                    f"Queued extra checks: {len(self.priority)}.")

        if cmd == "why":
            if not args:
                return "Usage: `!why ASIN`"
            asin = parse_asin(args[0])
            if not asin:
                return "That doesn't look like an ASIN."
            r = self.reasons.get(asin)
            if not r:
                where = "I'm watching it but haven't checked it yet." if asin in self.watch else \
                        "I'm not watching it. Add it with `!watch`."
                return f"No decision recorded for `{asin}` yet. {where}"
            name = self.names.get(asin, asin)
            return f"**{name}** (`{asin}`), last checked {r[1]}: {r[0]}"

        if cmd == "help":
            return ("**Commands** (Amazon bot)\n"
                    "`!watch ASIN` - start watching a product (ASIN or amazon.ca link)\n"
                    "`!remove ASIN` - stop watching it\n"
                    "`!list` - what I'm watching and why each isn't alerting\n"
                    "`!why ASIN` - the reason for my latest decision on one product\n"
                    "`!pause` / `!resume` - stop or restart all requests\n"
                    "`!refresh` - put the speed back to normal and start a fresh check round right now\n"
                    "`!restartchrome` - close Chrome and open a new window\n"
                    "`!speed` / `!speed MIN MAX [ROUND]` - show or change the seconds between requests\n"
                    "`!status` - am I running, blocked or paused")
        return None


# ------------------------------------------------------------ test + debug
def print_verdict(v):
    if v is None:
        print("  COULD NOT READ the product page (no product title). The bot will NOT alert on this item.")
        return
    for k, val in v.items():
        if k != "image":
            print(f"  {k}: {val}")
    ok = is_ok(v)
    print(f"\n  Why: {reason_text(v, ok)}")
    print(f"  Would the bot alert right now? {'YES' if ok else 'no'}")


async def test_mode():
    async with make_session(get_proxy()) as s:
        for term in SEARCH_TERMS:
            items = await fetch_items(s, term, get_proxy())
            if items is None:
                continue
            print(f"\n[{term}] got {len(items)} products (search-tile data, NOT verified, no seller info)")
            for it in items[:10]:
                flag = "price" if it["in_stock"] else "no price"
                print(f"  {it['sku']}  tile ${it['tile_price']}  {flag:8}  {it['name'][:55]}")
            await asyncio.sleep(random.uniform(GAP_MIN, GAP_MAX))


async def debug_sku(asin):
    proxy = get_proxy()
    async with make_session(proxy) as s:
        print(f"Using the {'real browser' if FETCHER != 'curl' else 'curl'} method.")
        print("Opening the homepage, then a search page, like the bot does...")
        await fetch_home(s, proxy)
        await asyncio.sleep(random.uniform(GAP_MIN, GAP_MAX))
        await fetch_items(s, SEARCH_TERMS[0], proxy, referer=f"{BASE}/")
        await asyncio.sleep(random.uniform(GAP_MIN, GAP_MAX))
        print("--- PRODUCT PAGE (what the bot will alert with) ---")
        v, status, html = await get_verified(s, asin, proxy, referer=search_url(SEARCH_TERMS[0]))
        print(f"  page status: {status}")
        print_verdict(v)
        if v is None or v.get("seller") is None or v.get("price") is None:
            fname = f"debug_{asin}.html"
            with open(fname, "w") as f:
                f.write(html or "")
            print(f"  Saved the page to {fname} so the layout can be checked.")


def parse_file(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        html = f.read()
    if is_blocked(html):
        print("This is Amazon's robot-check page, not a product page.")
        return
    print_verdict(parse_product(html))


# ------------------------------------------------------------ discord bot
def run_bot():
    import discord

    token = os.environ.get("DISCORD_TOKEN") or DISCORD_TOKEN
    channel_raw = os.environ.get("CHANNEL_ID") or CHANNEL_ID
    role_id = os.environ.get("ROLE_ID") or ROLE_ID or None
    proxy = get_proxy()
    dash_url = os.environ.get("DASHBOARD_URL", "").strip()
    dash_key = os.environ.get("BOT_API_KEY", "").strip()
    if not token or not channel_raw:
        sys.exit("Missing DISCORD_TOKEN or CHANNEL_ID. Set them in Railway Variables.")
    channel_id = int(channel_raw)
    # Maintenance channel: errors, blocks and startup info go here, never to the main channel. Commands work in both.
    maint_channel_id = int(os.environ.get("MAINTENANCE_CHANNEL_ID", "").strip() or MAINTENANCE_CHANNEL_ID)
    command_channels = {channel_id, maint_channel_id}

    intents = discord.Intents.default()
    intents.message_content = True      # needed to read !commands (enable it in the Developer Portal too)

    class Bot(discord.Client):
        async def setup_hook(self):
            dash = shipper = None
            if dash_url and dash_key:
                dash = Dashboard(dash_url, dash_key)
                shipper = LogShipper(dash)
            elif dash_url or dash_key:
                print("[dashboard] DASHBOARD_URL and BOT_API_KEY must BOTH be set. Running without the dashboard.")
            self.shipper = shipper

            def new_session():
                return make_session(proxy)
            self.mon = Monitor(new_session(), self.post_alert, self.post_text,
                               proxy, dashboard=dash, shipper=shipper, session_factory=new_session,
                               send_maint=self.post_maint)
            self.bg = asyncio.create_task(self.run_monitor())

        async def run_monitor(self):
            try:
                await self.wait_until_ready()
                where = "dashboard connected" if self.mon.dashboard else "no dashboard, using Discord commands"
                if proxy:
                    safe = "(proxy set)"
                    try:
                        u = urlsplit(proxy)
                        safe = f"{u.scheme}://{u.username + ':****@' if u.username else ''}{u.hostname}:{u.port}"
                    except Exception:
                        pass
                    proxy_line = f"Proxy: `{safe}`"
                else:
                    proxy_line = "Proxy: **not set** (using Railway IP)"
                if isinstance(self.mon.session, BrowserSession):
                    try:
                        method = "real Chrome browser\n" + await self.mon.session.describe()
                    except Exception as e:
                        method = f"real Chrome browser\nChrome failed to start: `{type(e).__name__}: {e}`"
                else:
                    method = "curl (no browser). Delete the FETCHER variable in Railway to use Chrome."
                await self.mon._say(
                    f"Amazon bot online ({where}).\n{proxy_line}\n"
                    f"Method: {method}\n"
                    f"Alerts only for 'Ships from and sold by Amazon.ca'. Every request to Amazon is "
                    f"{self.mon.pace_text()} apart. Type `!help` for commands.",
                    to="maint"
                )
                jobs = [self.mon.run(), self.mon.run_sync(), self.mon.run_watchdog()]
                if self.shipper:
                    jobs.append(self.shipper.run())
                await asyncio.gather(*jobs)
            except Exception as e:
                print(f"[fatal] monitor stopped: {type(e).__name__}: {e}. Restarting the bot.")
                os._exit(1)              # Railway starts it again; better than answering Discord while checking nothing

        async def post_text(self, text):
            ch = self.get_channel(channel_id)
            if ch is not None:
                await ch.send(text[:1900])

        async def post_maint(self, text):
            ch = self.get_channel(maint_channel_id)
            if ch is None:
                try:
                    ch = await self.fetch_channel(maint_channel_id)
                except discord.NotFound:
                    print(f"[discord] no channel has the ID {maint_channel_id}. Re-copy it: right-click the "
                          f"channel name > Copy Channel ID, and set MAINTENANCE_CHANNEL_ID.")
                    return
                except discord.Forbidden:
                    print(f"[discord] the bot isn't allowed into the maintenance channel {maint_channel_id}. Give it "
                          f"View Channel and Send Messages in that channel's permissions.")
                    return
            await ch.send(f"[{local_now():%I:%M %p}] [Amazon] {text}"[:1900])

        async def post_alert(self, a):
            v, kind = a["v"], a["kind"]
            e = discord.Embed(title=(a["name"] or a["sku"])[:250], url=a["url"],
                              color=0x2ECC71 if kind == "Restock" else 0x3498DB)
            e.set_author(name=f"Amazon CA - {kind}")
            e.add_field(name="Price", value=money(v))
            e.add_field(name="Sold by", value=v["seller"] or "?")
            e.add_field(name="Ships from", value=v["ships_from"] or v["seller"] or "?")
            e.add_field(name="Cart Limit", value=str(v["cart_limit"] or "?"))
            e.add_field(name="ASIN", value=a["sku"])
            if v.get("offer_id"):
                e.add_field(name="Offer ID", value=v["offer_id"], inline=False)
            e.add_field(name="Why this alert", value=a["why"][:1000], inline=False)
            img = v["image"] or a.get("img")
            if img:
                e.set_thumbnail(url=img)
            e.set_footer(text=f"Amazon CA - verified on product page - {local_now():%I:%M:%S %p}")
            ping = f"<@&{role_id}> " if role_id and (kind == "Restock" or PING_ON_NEW_ITEM) else ""
            ch = self.get_channel(channel_id)
            if ch is not None:
                await ch.send(ping, embed=e)

        def allowed(self, user):
            if ALLOWED_USER_IDS:
                return user.id in ALLOWED_USER_IDS
            perms = getattr(user, "guild_permissions", None)
            return bool(perms and perms.manage_guild)

        async def on_message(self, message):
            if message.author.bot or message.channel.id not in command_channels:
                return
            if not message.content.startswith("!"):
                return
            if not self.allowed(message.author):
                await message.reply("You don't have permission to control this bot.", mention_author=False)
                return
            reply = self.mon.command(message.content)
            if reply is None:
                return
            await message.reply(reply[:1900], mention_author=False)

    Bot(intents=intents).run(token)


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "test":
            asyncio.run(test_mode())
        elif len(sys.argv) > 2 and sys.argv[1] == "debug":
            asyncio.run(debug_sku(parse_asin(sys.argv[2]) or sys.argv[2]))
        elif len(sys.argv) > 2 and sys.argv[1] == "parse":
            parse_file(sys.argv[2])
        else:
            run_bot()
    except Blocked as b:
        print(f"\nBLOCKED ({b}): Amazon is showing its robot-check page to this connection right now.\n"
              "Stop, wait 30-60 minutes (or switch to a different network, like a phone hotspot), then try again.")
