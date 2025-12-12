from playwright.sync_api import sync_playwright
import time
import os

def capture_seat_layout(url, output_path):
    """
    Navigates to the seat layout URL, handles popup, and saves screenshot.
    """
    print(f"[Layout] Navigating to: {url}")
    
    with sync_playwright() as p:
        # headless=True for production/background running
        browser = p.chromium.launch(headless=True)
        
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        try:
            page.goto(url, timeout=60000)

            # --- Handle Popup ---
            try:
                # Look for "Select Seats" button by Role/Text
                select_seats_btn = page.get_by_role("button", name="Select Seats")
                select_seats_btn.wait_for(state="visible", timeout=8000)
                select_seats_btn.click()
                print("[Layout] Popup handled.")
            except:
                print("[Layout] No popup or already skipped.")

            # --- Wait for Canvas ---
            page.wait_for_selector('canvas', state="visible", timeout=15000)
            time.sleep(3) # Wait for animation

            # --- Screenshot ---
            # Try to get the container (includes legend), fallback to canvas
            element = page.locator(".seat-layout-container")
            if element.count() == 0:
                element = page.locator("canvas").first
            
            element.screenshot(path=output_path)
            print(f"[Layout] Screenshot saved: {output_path}")
            return True

        except Exception as e:
            print(f"[Layout] Error capturing layout: {e}")
            return False
            
        finally:
            browser.close()