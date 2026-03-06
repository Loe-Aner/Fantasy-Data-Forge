import pandas as pd
from datetime import datetime
import re

from scraper_wiki_main import pobierz_soup

def wyscrapuj_linki_z_kategorii_wiki(tresc) -> list[str]:
    """
    Wyciąga linki /wiki/... z div.mw-category (kategorie questów) i zwraca pełne URL-e.
    """
    if not tresc:
        return []

    baza_url = "https://warcraft.wiki.gg"
    # albo kraina albo przedzialy poziomow
    kontener = tresc.select_one("div.mw-category-columns") or tresc.select_one("div.mw-category")
    if not kontener:
        return []

    wynik = []
    for a in kontener.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if not href.startswith("/wiki/"):
            continue
        if href.lower().endswith("_storyline"): # aby nie brać pierdół
            continue

        wynik.append(f"{baza_url}{href}")

    return list(dict.fromkeys(wynik))


def wyszukaj_link_nastepnej_strony_kategorii(tresc) -> str | None:
    if not tresc:
        return None

    baza_url = "https://warcraft.wiki.gg"

    kontener = tresc.find(id="mw-pages") or tresc.select_one("div.mw-category")
    if not kontener:
        kontener = tresc

    for a in kontener.find_all("a", href=True):
        txt = a.get_text(" ", strip=True).lower()
        if txt != "next page":
            continue

        href = (a.get("href") or "").strip()
        if not href:
            continue

        if href.startswith("/"):
            return f"{baza_url}{href}"
        if href.startswith("http"):
            return href

    return None

def pobierz_przerzuc_questy_per_lvle(silnik, url: str):
    soup = pobierz_soup(url)
    if soup is None:
        print("Błąd w zupce!")
        return

    wyniki = []

    for item in soup.select("div.CategoryTreeItem"):
        link = item.select_one("bdi a")
        span = item.select_one("span[dir='ltr']")

        if not link or not span:
            continue

        match_zakres = re.search(r"Quests at (\d+(?:-\d+)?)$", link.get_text(strip=True))
        match_liczba = re.search(r"\(([\d,]+)\s*P\)", span.get_text(strip=True))

        if match_zakres and match_liczba:
            wyniki.append({
                "zakres": match_zakres.group(1),
                "liczba_misji": int(match_liczba.group(1).replace(",", ""))
            })

    try:
        (
        pd.DataFrame(wyniki)
        .assign(DATA_STATUS=datetime.now().replace(microsecond=0))
        .rename(columns={"zakres": "ZAKRES", "liczba_misji": "LICZBA_MISJI"})
        .to_sql(schema="dbo", name="MISJE_ZMIANY_WIKI", con=silnik, if_exists="append", index=False)
        )
    except Exception as e:
        print(f"--- Błąd podczas przerzucania danych: {e}")