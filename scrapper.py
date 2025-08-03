import asyncio
from playwright.async_api import async_playwright
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import logging
import uuid
import pandas as pd
from typing import Optional, Callable, List, Dict, Any
import random

# Configuration
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
BUCKET_NAME = 'quote-images'

# Initialisation Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

async def scrape_quotes(topic: str, 
                      progress_callback: Optional[Callable[[int, int], None]] = None
                      ) -> List[Dict[str, Any]]:
    url = f"https://www.brainyquote.com/topics/{topic}-quotes"
    quotes = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            slow_mo=random.uniform(50, 200),
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-maximized",
                "--no-sandbox",
                f"--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90,110)}.0.{random.randint(4000,5000)}.{random.randint(100,200)} Safari/537.36"
            ]
        )

        context = await browser.new_context(
            viewport={'width': 1366, 'height': 768},
            locale="en-US",
            timezone_id="America/New_York",
            color_scheme="light"
        )

        # Masquage avancé
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => false });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            window.chrome = { runtime: {} };
        """)

        page = await context.new_page()

        try:
            page.set_default_timeout(150000)
            response = await page.goto(url, wait_until="networkidle", timeout=90000)

            if not response or not response.ok:
                raise Exception(f"Échec du chargement: {response.status if response else 'Aucune réponse'}")

            if await page.query_selector('#captcha-form'):
                await page.screenshot(path="captcha_detected.png")
                raise Exception("CAPTCHA détecté - Veuillez résoudre manuellement")

            selectors = [
                {'selector': '.qtw_listing', 'type': 'css'},
                {'selector': '//div[contains(@class, "grid-item")]', 'type': 'xpath'},
                {'selector': '.bqQt', 'type': 'css'}
            ]

            found = False
            for s in selectors:
                try:
                    await page.wait_for_selector(s['selector'], timeout=20000)
                    found = True
                    break
                except:
                    continue

            if not found:
                content = await page.content()
                if "quote" not in content.lower():
                    await page.screenshot(path="debug_fallback.png")
                    raise Exception("Échec de détection du contenu")

            await auto_scroll(page)

            quote_elements = await page.query_selector_all('.grid-item')

            for i, element in enumerate(quote_elements):
                try:
                    quote_data = await process_quote_element(element, topic)

                    if quote_data:
                        if 'image_bytes' in quote_data:
                            img_path = f"{quote_data['id']}.jpg"
                            upload_response = supabase.storage.from_(BUCKET_NAME).upload(
                                path=img_path,
                                file=quote_data['image_bytes'],
                                file_options={"content-type": "image/jpeg"}
                            )

                            if hasattr(upload_response, 'error') and upload_response.error:
                                raise Exception(upload_response.error.message)

                            quote_data['image_path'] = img_path
                            del quote_data['image_bytes']

                        quotes.append(quote_data)

                        if progress_callback:
                            progress_callback(i + 1, len(quote_elements))

                except Exception as e:
                    logging.error(f"Error processing quote {i}: {str(e)}", exc_info=True)
                    continue

            if quotes:
                insert_response = supabase.table('quotes').upsert(quotes).execute()
                if hasattr(insert_response, 'error') and insert_response.error:
                    raise Exception(insert_response.error.message)

            return quotes

        except Exception as e:
            await page.screenshot(path="error_fullpage.png", full_page=True)
            logging.error(f"Scraping failed: {str(e)}", exc_info=True)
            raise

        finally:
            await context.close()
            await browser.close()

async def process_quote_element(element, topic: str) -> Optional[Dict[str, Any]]:
    try:
        author = await element.query_selector('.bq-aut')
        quote_text = await element.query_selector('.b-qt')
        quote_link = await element.query_selector('a')
        img = await element.query_selector('img')

        if not (author and quote_text and quote_link):
            return None

        quote_data = {
            'id': str(uuid.uuid4()),
            'author': (await author.inner_text()).strip(),
            'quote': (await quote_text.inner_text()).strip(),
            'link': await quote_link.get_attribute('href'),
            'topic': topic.lower()
        }

        if img:
            img_url = await img.get_attribute('src')
            if img_url and img_url.startswith('http'):
                img_data = await element.page.evaluate("""async (url) => {
                    const response = await fetch(url);
                    const buffer = await response.arrayBuffer();
                    return Array.from(new Uint8Array(buffer));
                }""", img_url)
                quote_data['image_bytes'] = bytes(img_data)

        return quote_data
    except Exception as e:
        logging.error(f"Error processing element: {str(e)}")
        return None

async def auto_scroll(page):
    try:
        await page.evaluate("""() => {
            return new Promise((resolve) => {
                const maxAttempts = 10;
                let attempts = 0;
                const scrollInterval = setInterval(() => {
                    window.scrollBy(0, 500);
                    attempts++;
                    if (window.innerHeight + window.scrollY >= document.body.scrollHeight || attempts >= maxAttempts) {
                        clearInterval(scrollInterval);
                        resolve();
                    }
                }, 500);
            });
        }""")
    except Exception as e:
        logging.warning(f"Auto-scroll interrupted: {str(e)}")
        raise

def save_to_file(quotes: List[Dict[str, Any]], format: str = 'json') -> Optional[str]:
    try:
        df = pd.DataFrame(quotes)
        if format == 'json':
            return df.to_json(orient='records', force_ascii=False)
        elif format == 'csv':
            return df.to_csv(index=False)
        return None
    except Exception as e:
        logging.error(f"Failed to save file: {str(e)}")
        return None

async def main():
    try:
        print("🚀 Starting enhanced scraper...")
        print(f"🔗 Supabase URL: {SUPABASE_URL}")
        print(f"🔑 Supabase Key: {SUPABASE_KEY[:5]}...{SUPABASE_KEY[-5:]}")

        quotes = await scrape_quotes("motivational")

        if quotes:
            print(f"✅ Successfully scraped {len(quotes)} quotes")

            json_data = save_to_file(quotes, 'json')
            if json_data:
                with open('quotes_backup.json', 'w', encoding='utf-8') as f:
                    f.write(json_data)
                print("📄 Local backup created: quotes_backup.json")
            else:
                print("⚠️ La sauvegarde locale a échoué.")

            count_response = supabase.from_("quotes").select("count", count="exact").execute()
            if hasattr(count_response, 'count'):
                print(f"📊 Total quotes in database: {count_response.count}")
        else:
            print("⚠️ No quotes found")

    except Exception as e:
        print(f"💥 Critical error: {str(e)}")
        logging.exception("Scraping failed")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename='scraper.log'
    )
    asyncio.run(main())
