import requests
from bs4 import BeautifulSoup
import time
from scraper import pobierz_soup, pobierz_tresc

# def wyscrapuj_linki_z_progression_wiki(tresc):
#     if not tresc:
#         return []

#     progression = tresc.find(id="Progression")
#     if not progression:
#         return []

#     h2 = progression.find_parent("h2")
#     if not h2:
#         return []

#     ol = h2.find_next("ol")
#     if not ol:
#         return []

#     wynik = []
#     baza_url = "https://warcraft.wiki.gg"

#     for span_quest in ol.find_all("span", class_="questlink"):
#         a = span_quest.find("a", href=True)
#         if not a:
#             continue

#         href = (a.get("href") or "").strip()
#         if not href:
#             continue

#         if not href.startswith("/wiki/"):
#             continue

#         pelny_url = f"{baza_url}{href}"
#         wynik.append(pelny_url)

#     return wynik

def wyscrapuj_linki_z_kategorii_wiki(tresc) -> list[str]:
    """
    Wyciąga linki /wiki/... z div.mw-category (kategorie questów) i zwraca pełne URL-e.
    """
    if not tresc:
        return []

    baza_url = "https://warcraft.wiki.gg"
    kontener = tresc.select_one("div.mw-category")
    if not kontener:
        return []

    wynik = []
    for a in kontener.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if not href.startswith("/wiki/"):
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

def wyscrapuj_linki_z_kategorii_z_paginacja(
        kategoria_url: str,
        sleep_s: int = 1,
        printuj_paginacje: bool = True
    ) -> list[str]:
    """
    Przechodzi po stronach kategorii (z paginacją) i zbiera wszystkie linki questów.
    """
    wynik = []
    odwiedzone = set()

    nastepny_url = kategoria_url
    nr_strony = 0

    while nastepny_url and nastepny_url not in odwiedzone:
        odwiedzone.add(nastepny_url)
        nr_strony += 1

        if printuj_paginacje:
            print(f"    - Strona kategorii #{nr_strony}: {nastepny_url}")

        soup = pobierz_soup(nastepny_url)
        tresc = pobierz_tresc(soup)

        linki = wyscrapuj_linki_z_kategorii_wiki(tresc)
        wynik.extend(linki)

        nastepny_url = wyszukaj_link_nastepnej_strony_kategorii(tresc)
        time.sleep(sleep_s)

    return list(dict.fromkeys(wynik))