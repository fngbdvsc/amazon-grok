"""Loaded automatically on startup, before amazon_monitor.py reads its settings.

Forces the fast pace even if Railway still has the old GAP_MIN / MIN_SECONDS_PER_ROUND
variables. Also treats a live buy box as in stock when Amazon's page text is stale.
Delete this file to go back to the slow defaults in amazon_monitor.py.
"""
import os
import sys
import threading

os.environ["GAP_MIN"] = "3"
os.environ["GAP_MAX"] = "5"
os.environ["MIN_SECONDS_PER_ROUND"] = "3"
os.environ["BROWSE_FIRST"] = "0"


def _patch(mod):
    if mod is None or getattr(mod, "_buybox_patched", False):
        return
    if not hasattr(mod, "parse_product") or not hasattr(mod, "is_ok"):
        return
    old = mod.parse_product

    def parse_product(html, _old=old):
        v = _old(html)
        if not v or not html:
            return v
        low = html.lower()
        has_button = (
            'id="add-to-cart-button"' in low
            or "id='add-to-cart-button'" in low
            or 'id="buy-now-button"' in low
            or "id='buy-now-button'" in low
            or 'id="preorder-button"' in low
            or 'name="submit.add-to-cart"' in low
            or "name='submit.add-to-cart'" in low
            or 'name="submit.preorder"' in low
        )
        # Amazon leaves "Temporarily out of stock" up while the price or
        # Add to Cart / Buy Now button is already on the page.
        if v.get("price") is not None or has_button:
            v["in_stock"] = True
        return v

    def is_ok(v, _mod=mod):
        if not v or not v.get("in_stock"):
            return False
        return bool(v.get("internal")) or not _mod.ONLY_AMAZON_CA_SOLD

    mod.parse_product = parse_product
    mod.is_ok = is_ok
    mod._buybox_patched = True


def _watch():
    for _ in range(400):
        for name in ("__main__", "amazon_monitor"):
            _patch(sys.modules.get(name))
        if getattr(sys.modules.get("__main__"), "_buybox_patched", False):
            return
        import time
        time.sleep(0.05)


threading.Thread(target=_watch, daemon=True).start()
