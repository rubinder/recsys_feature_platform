import sys
from playwright.sync_api import sync_playwright
img = sys.argv[1]
pages = [("phase3-dagster-lineage.png", "http://127.0.0.1:3000/assets/offline_metrics?view=lineage&lineageScope=upstream&lineageDepth=10", 1700, 1000),
         ("phase3-dagster-checks-online-store.png", "http://127.0.0.1:3000/assets/online_store?view=checks", 1440, 900),
         ("phase3-dagster-checks-batch-features.png", "http://127.0.0.1:3000/assets/batch_features?view=checks", 1440, 900),
         ("phase3-dagster-automation.png", "http://127.0.0.1:3000/automation", 1440, 600)]
with sync_playwright() as p:
    b = p.chromium.launch()
    ctx = b.new_context()
    for name, url, w, h in pages:
        pg = ctx.new_page(); pg.set_viewport_size({"width": w, "height": h})
        pg.goto(url); pg.wait_for_timeout(4000)
        skip = pg.get_by_role("button", name="Skip")
        if skip.count():
            skip.first.click(); pg.wait_for_timeout(1500)
        pg.screenshot(path=f"{img}/{name}")
        print("captured", name)
