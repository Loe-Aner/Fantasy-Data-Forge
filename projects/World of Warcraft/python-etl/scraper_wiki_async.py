import asyncio
import time
import os
import random
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession, RequestsError
from tenacity import AsyncRetrying, RetryError, retry_if_exception_type, stop_after_attempt, wait_exponential

import scraper_wiki_main as parser_lib

USER_AGENT = "WoW_PolishTranslationProject -> (reachable on your Discord: Loe'Aner)"
DEFAULT_TIMEOUT = 30
WIKI_DELAY = float(os.getenv("WIKI_DELAY_SECONDS", "0.65"))
WOWHEAD_DELAY = float(os.getenv("WOWHEAD_DELAY_SECONDS", "0.65"))

class HostThrottle:
    def __init__(self, min_delay: float):
        self.min_delay = max(0.0, min_delay)
        self._lock = asyncio.Lock()
        self._last_call = 0.0

    async def wait(self) -> None:
        async with self._lock:
            elapsed = time.monotonic() - self._last_call
            jitter = random.uniform(0, self.min_delay * 0.5) 
            target_delay = self.min_delay + jitter
            
            remaining = target_delay - elapsed
            
            if remaining > 0:
                await asyncio.sleep(remaining)
            
            self._last_call = time.monotonic()

def cpu_bound_parsing_task(html: str, url: str) -> dict | None:
    if not html:
        return None
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    tresc = parser_lib.pobierz_tresc(soup)
    if not tresc:
        return None

    html_skompresowany = parser_lib.skompresuj_html(tresc)

    podsumowanie = parser_lib.parsuj_podsumowanie_misji(tresc)
    cele = parser_lib.parsuj_cele_misji(tresc)
    opis = parser_lib.parsuj_opis(tresc)
    postep = parser_lib.parsuj_postep(tresc)
    zakonczenie = parser_lib.parsuj_zakonczenie(tresc)
    nagrody = parser_lib.parsuj_nagrode(tresc)

    sequence = parser_lib.parsuj_wspolna_kolejnosc_gossipow_i_dymkow(tresc)
    for el in sequence:
        el["wypowiedzi_EN"] = parser_lib.indeksuj_linie(el["tekst_en"])
        if "tekst_en" in el:
            del el["tekst_en"]

    sequence = parser_lib.agreguj_kolejne_wypowiedzi(sequence)
    sequence = parser_lib.renumeruj_id(sequence)

    cele_zlaczone = parser_lib.złącz_cele(cele)

    hash_sekcji = {
        "Cele_EN": {
            "Główny": parser_lib.policz_hash_z_tekstu(cele_zlaczone["Główny"]),
            "Podrzędny": parser_lib.policz_hash_z_tekstu(cele_zlaczone["Podrzędny"]),
        },
        "Treść_EN": parser_lib.policz_hash_z_tekstu(parser_lib.złącz_slownik_linii(opis)),
        "Postęp_EN": parser_lib.policz_hash_z_tekstu(parser_lib.złącz_slownik_linii(postep)),
        "Zakończenie_EN": parser_lib.policz_hash_z_tekstu(parser_lib.złącz_slownik_linii(zakonczenie)),
        "Nagrody_EN": parser_lib.policz_hash_z_tekstu(parser_lib.złącz_slownik_linii(nagrody)),
        "Dialogi_EN": {
            "Dymki_EN": parser_lib.policz_hash_z_tekstu(parser_lib.złącz_dialogi(sequence, {"dymek"})),
            "Gossipy_EN": parser_lib.policz_hash_z_tekstu(parser_lib.złącz_dialogi(sequence, {"gossip"})),
        },
    }

    return {
        "Źródło": {
            "url": url,
            "html_skompresowany": html_skompresowany
        },
        "Misje_EN": {
            "Podsumowanie_EN": podsumowanie,
            "Cele_EN": cele,
            "Treść_EN": opis,
            "Postęp_EN": postep,
            "Zakończenie_EN": zakonczenie,
            "Nagrody_EN": nagrody,
        },
        "Dialogi_EN": {"Gossipy_Dymki_EN": sequence},
        "Hash_HTML": hash_sekcji,
    }

class WoWScraperService:
    def __init__(self, concurrency: int = 5):
        naglowki_przegladarki = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,pl;q=0.8",
            "Referer": "https://www.google.com/",
            "X-Bot-Contact": "Discord: Loe'Aner (WoW Translation Project)" 
        }
        
        self.client = AsyncSession(
            headers=naglowki_przegladarki,
            timeout=DEFAULT_TIMEOUT,
            impersonate="chrome120",
            allow_redirects=True
        )
        
        self.throttles = {
            "wiki": HostThrottle(WIKI_DELAY),
            "wowhead": HostThrottle(WOWHEAD_DELAY),
        }
        self.semaphores = {
            "wiki": asyncio.Semaphore(concurrency),
            "wowhead": asyncio.Semaphore(concurrency),
        }

    async def close(self):
        self.client.close()

    async def _fetch_html(self, url: str) -> str | None:
        host = "wowhead" if "wowhead" in url else "wiki"
        dlawik = self.throttles[host]
        semafor = self.semaphores[host]

        try:
            async for proba in AsyncRetrying(
                stop=stop_after_attempt(6),
                wait=wait_exponential(multiplier=1, min=2, max=60),
                retry=retry_if_exception_type(RequestsError),
                reraise=True,
            ):
                with proba:
                    async with semafor:
                        await dlawik.wait()
                        odpowiedz = await self.client.get(url)
                    
                    if odpowiedz.status_code in (429, 502, 503):
                        raise RequestsError(
                            f"{odpowiedz.status_code} THROTTLE", response=odpowiedz
                        )
                    
                    if odpowiedz.status_code == 403:
                        print(f"CRITICAL: 403 Forbidden na {url} - WoWhead nadal blokuje mimo impersonate.")
                        return None

                    odpowiedz.raise_for_status()
                    return odpowiedz.text

        except RetryError as exc:
            print(f"SKIP: Błąd pobierania {url}: {exc}")
            return None
        except Exception as e:
            print(f"CRITICAL: Nieoczekiwany błąd {url}: {e}")
            return None

    async def process_url(self, url: str):
        html = await self._fetch_html(url)
        if not html:
            return None
        
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, cpu_bound_parsing_task, html, url)
        return result

    async def run_batch(self, urls: list[str]):
        zadania = [self.process_url(u) for u in urls]
        return await asyncio.gather(*zadania)

async def parsuj_wiele_misji_async(quest_urls: list[str], max_concurrency: int = 5, batch_size: int = 20):
    service = WoWScraperService(concurrency=max_concurrency)
    wynik_lista = []
    
    print(f"Rozpoczynam scrapowanie {len(quest_urls)} adresów w paczkach po {batch_size}...")

    try:
        paczki = [quest_urls[i:i + batch_size] for i in range(0, len(quest_urls), batch_size)]
        
        for i, paczka in enumerate(paczki, 1):
            print(f"--- Przetwarzanie paczki {i}/{len(paczki)} ({len(paczka)} elementów) ---")
            
            rezultaty = await service.run_batch(paczka)
            
            for url, res in zip(paczka, rezultaty):
                if res:
                    wynik_lista.append((url, res))

            await asyncio.sleep(2) 

        return wynik_lista

    finally:
        await service.close()