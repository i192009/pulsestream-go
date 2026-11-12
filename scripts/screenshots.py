"""Real browser assertions and screenshots. pip install playwright; playwright install chromium."""
from pathlib import Path
import re
from playwright.sync_api import sync_playwright, expect
ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'docs/screenshots';OUT.mkdir(parents=True,exist_ok=True)
with sync_playwright() as p:
    browser=p.chromium.launch(headless=True)
    page=browser.new_page(viewport={'width':1512,'height':1100},device_scale_factor=1)
    errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
    page.goto('http://localhost:13000')
    page.get_by_label('Tenant API key').fill('demo-key')
    page.get_by_role('button',name='Open workspace').click()
    expect(page.get_by_text('Live',exact=True)).to_be_visible(timeout=20000)
    expect(page.get_by_role('heading',name='Event throughput')).to_be_visible()
    page.screenshot(path=str(OUT/'overview.png'),full_page=True)
    for name in ['Event explorer','Stream processing','Analytics','Anomaly detection','Recovery & replay','System health']:
        page.get_by_role('button',name=re.compile('^'+re.escape(name))).click()
        expect(page.get_by_role('heading',name=name,exact=True)).to_be_visible()
        page.wait_for_timeout(600)
        if name=='Event explorer':
            expect(page.locator('tbody tr').first).to_be_visible(timeout=20000)
            page.locator('tbody tr').first.click()
            expect(page.get_by_role('heading',name='Event payload')).to_be_visible()
        page.screenshot(path=str(OUT/(name.lower().replace(' ','-').replace('&','and')+'.png')),full_page=True)
    page.set_viewport_size({'width':390,'height':844})
    page.get_by_role('button',name='Overview',exact=True).click()
    page.screenshot(path=str(OUT/'mobile.png'),full_page=True)
    assert not errors,errors
    page.set_viewport_size({'width':1512,'height':1100})
    page.goto('http://localhost:3001/d/pulse/pulsestream')
    expect(page.get_by_text('PulseStream',exact=True).first).to_be_visible(timeout=30000)
    page.wait_for_timeout(5000)
    page.screenshot(path=str(OUT/'grafana.png'),full_page=True)
    browser.close()
print('All seven dashboard pages rendered, WebSocket connected, event payload opened, no browser exceptions.')
