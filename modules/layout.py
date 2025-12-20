from playwright.async_api import async_playwright
import os
import asyncio

async def capture_seat_layout(url, output_path):
    """
    Navigates to the seat layout URL, handles popup, and saves screenshot.
    """
    print(f"[Layout] Navigating to: {url}")
    
    async with async_playwright() as p:
        # headless=True for production/background running
        browser = await p.chromium.launch(headless=True)
        
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            await page.goto(url, timeout=60000)

            # --- Handle Popup ---
            try:
                # Look for "Select Seats" button by Role/Text
                select_seats_btn = page.get_by_role("button", name="Select Seats")
                if await select_seats_btn.is_visible(timeout=5000): 
                    # FIX: Added 'await' here
                    await select_seats_btn.click()
                    print("[Layout] Popup clicked.")
                else:
                    print("[Layout] No popup found.")
            except Exception: 
                # If element doesn't exist or other error, just move on
                pass
                # select_seats_btn.wait_for(state="visible", timeout=8000)
                # select_seats_btn.click()
                # print("[Layout] Popup handled.")
            # except:
            #     print("[Layout] No popup or already skipped.")

            # --- Wait for Canvas ---
            await page.wait_for_selector('canvas', state="visible", timeout=15000)
            await asyncio.sleep(3) # Wait for animation

            # --- Screenshot ---
            # Try to get the container (includes legend), fallback to canvas
            element = page.locator(".seat-layout-container")
            if await element.count() == 0:
                element = page.locator("canvas").first
            
            await element.screenshot(path=output_path)
            print(f"[Layout] Screenshot saved: {output_path}")
            return True

        except Exception as e:
            print(f"[Layout] Error capturing layout: {e}")
            return False
            
        finally:
            await browser.close()