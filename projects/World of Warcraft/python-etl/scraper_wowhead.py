import asyncio
import os
import re
import pandas as pd
from openpyxl import Workbook, load_workbook
from bs4 import BeautifulSoup


from scraper_wiki_async import WoWScraperService


def wyciagnij_patch_logic(soup: BeautifulSoup) -> str:
    """Logika wyciągania patcha z soup."""
    for s in soup.find_all("script"):
        t = s.get_text(" ", strip=True)
        if "Added in patch" not in t:
            continue
        m = re.search(
            r'Added in patch\s*\[acronym=\\?"[^"]*\\?"\]([0-9]+\.[0-9]+\.[0-9]+)\[\\?/acronym\]',
            t
        )
        if m:
            return m.group(1)
    return ""

def parsuj_wowhead_html(html: str, url: str) -> tuple[str, str, str, str]:
    """
    Funkcja worker. Przyjmuje czysty HTML, zwraca gotowe dane.
    Zwraca krotkę: (url, storyline, patch, error_msg)
    """
    if not html:
        return url, "", "", "Brak HTML"

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    element = soup.select_one(".quick-facts-storyline-title")
    storyline = element.get_text().strip() if element else ""
    patch = wyciagnij_patch_logic(soup)
    
    return url, storyline, patch, ""

# --- KLASA SCRAPERA WOWHEAD (DZIEDZICZENIE) ---

class WowheadScraper(WoWScraperService):
    """
    Korzystamy z silnika WoWScraperService (retry, limity, klient HTTP),
    ale zmieniamy metodę process_url, żeby parsowała pod Wowheada.
    """
    async def process_url(self, url: str):
        html = await self._fetch_html(url)
        
        if not html:
            return url, "", "", "Błąd pobierania (HTTP)"

        loop = asyncio.get_running_loop()
        wynik = await loop.run_in_executor(None, parsuj_wowhead_html, html, url)
        
        return wynik

# --- FUNKCJE POMOCNICZE EXCEL ---

def normalize_cell(v):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if pd.isna(v):
        return None
    return v

def zapisz_excel_w_tle(wb: Workbook, path: str):
    wb.save(path)

# --- GŁÓWNA PĘTLA ---

async def buduj_mapping_01_async():
    raw_path = r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\surowe\wowhead_id_kraina_dodatek.xlsx"
    out_path = r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\mapping_01.xlsx"

    input_sheet = "prawie_gotowe_dane"
    output_sheet = "mapping_01"
    url_col = "MISJA_URL_WOWHEAD"

    FINAL_HEADERS = [
        "KRAINA_EN",
        "MISJA_ID_Z_GRY",
        "MISJA_TYTUL_EN",
        "DODATEK_EN",
        "MISJA_URL_WOWHEAD",
        "NAZWA_LINII_FABULARNEJ_EN",
        "DODANO_W_PATCHU"
    ]

    MAX_CONCURRENCY = 8
    BATCH_SIZE = 48

    print(f"Wczytuję dane z: {raw_path}")
    df_raw = pd.read_excel(raw_path, sheet_name=input_sheet)
    
    if url_col not in df_raw.columns:
        raise ValueError(f"Brak kolumny {url_col}")

    df_raw[url_col] = df_raw[url_col].astype(str).str.strip()
    df_raw = df_raw[df_raw[url_col].notna() & (df_raw[url_col] != "")].copy()

    if not os.path.exists(out_path):
        wb = Workbook()
        ws = wb.active
        ws.title = output_sheet
        ws.append(FINAL_HEADERS)
        wb.save(out_path)
        print("Utworzono nowy plik mapping_01.xlsx")
    else:
        df_check = pd.read_excel(out_path, sheet_name=output_sheet, nrows=0)

    df_out = pd.read_excel(out_path, sheet_name=output_sheet)
    if url_col in df_out.columns:
        existing_urls = set(df_out[url_col].dropna().astype(str).str.strip().tolist())
    else:
        existing_urls = set()

    df_new = df_raw[~df_raw[url_col].isin(existing_urls)].copy()
    print(f"Postęp: {len(existing_urls)} zrobionych. Nowych do pobrania: {len(df_new)}")

    if df_new.empty:
        print("Wszystko zrobione! Idź pograć w WoW-a.")
        return

    row_by_url = {
        str(row[url_col]).strip(): row.to_dict()
        for _, row in df_new.iterrows()
    }
    
    urls_to_process = list(row_by_url.keys())

    wb = load_workbook(out_path)
    ws = wb[output_sheet]

    scraper = WowheadScraper(concurrency=MAX_CONCURRENCY)
    loop = asyncio.get_running_loop()

    dopisane = 0
    bledy = 0

    try:
        print(f"Start scrapowania {len(urls_to_process)} misji...")
        
        for start in range(0, len(urls_to_process), BATCH_SIZE):
            batch_urls = urls_to_process[start : start + BATCH_SIZE]
            print(f"\nBatch {start + 1}-{start + len(batch_urls)} / {len(urls_to_process)}")

            tasks = [scraper.process_url(u) for u in batch_urls]
            wyniki = await asyncio.gather(*tasks)

            bufor = []
            
            for link, storyline, patch, err in wyniki:
                if err:
                    bledy += 1
                    print(f" [FAIL] {link} -> {err}")
                    continue
                
                row_data = row_by_url.get(link)
                if not row_data:
                    continue

                row_data["storyline"] = storyline
                row_data["patch"] = patch

                excel_row = []
                for header in FINAL_HEADERS:
                    val = row_data.get(header)
                    excel_row.append(normalize_cell(val))

                bufor.append(excel_row)
                print(f" [OK] {link} (Patch: {patch})")

            if bufor:
                for r in bufor:
                    ws.append(r)

                print(" -> Zapisuję batch do pliku...")
                await loop.run_in_executor(None, zapisz_excel_w_tle, wb, out_path)
                dopisane += len(bufor)

    finally:
        await scraper.close()

    print(f"\nPodsumowanie: Sukces: {dopisane}, Błędy: {bledy}")

def buduj_mapping_01():
    asyncio.run(buduj_mapping_01_async())

if __name__ == "__main__":
    buduj_mapping_01()