import time
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from scraper_wiki_main import pobierz_soup, pobierz_tresc, parsuj_misje_z_url

from moduly.analiza_html import (
    wyscrapuj_linki_z_kategorii_wiki,
    wyszukaj_link_nastepnej_strony_kategorii
)

from moduly.repo_kolejka_linkow import zapisz_link_do_scrapowania
from moduly.repo_zrodlo import zapisz_zrodlo_do_db

from moduly.db_core import _czy_duplikat

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
        if soup is None:
            break

        tresc = pobierz_tresc(soup)
        if not tresc:
            break

        linki = wyscrapuj_linki_z_kategorii_wiki(tresc)
        wynik.extend(linki)

        nastepny_url = wyszukaj_link_nastepnej_strony_kategorii(tresc)
        time.sleep(sleep_s)

    return list(dict.fromkeys(wynik))

def wyscrapuj_kategorie_questow_i_zapisz_linki_do_db(
        silnik,
        kategorie_urls: list[str],
        zrodlo: str = "wiki",
        sleep_s: int = 1,
        printuj_paginacje: bool = True
    ) -> None:
    """
    Dla każdej kategorii:
    - zbiera linki questów (z paginacją),
    - zapisuje do dbo.LINKI_DO_SCRAPOWANIA,
    - printuje co dodano / co pominięto (duplikat).
    """

    q_insert_url = text("""
        INSERT INTO dbo.LINKI_DO_SCRAPOWANIA (URL, ZRODLO_NAZWA)
        VALUES (:url, :zrodlo)
    """)

    for kat_url in kategorie_urls:
        print(f"\n=== KATEGORIA: {kat_url} ===")

        linki = wyscrapuj_linki_z_kategorii_z_paginacja(
            kategoria_url=kat_url,
            sleep_s=0,
            printuj_paginacje=printuj_paginacje
        )

        print(f"    Zebrano linków: {len(linki)}")

        with silnik.begin() as conn:
            for i, url in enumerate(linki, start=1):
                if not url:
                    continue

                try:
                    conn.execute(q_insert_url, {"url": url, "zrodlo": zrodlo})
                    print(f"    [{i}/{len(linki)}] + DODANO: {url}")
                except IntegrityError as e:
                    if _czy_duplikat(e):
                        print(f"    [{i}/{len(linki)}] - POMINIĘTO (duplikat): {url}")
                    else:
                        raise

        time.sleep(sleep_s)

def hashuj_kategorie_i_zapisz_zrodlo(
        silnik,
        kategorie: list[str],
        zrodlo: str,
        sleep_s: int = 0,
        tabela_misje: str = "dbo.MISJE",
        tabela_zrodlo: str = "dbo.ZRODLO"
    ) -> None:

    q_select_misja_id = text(f"""
        SELECT MISJA_ID_MOJE_PK
        FROM {tabela_misje}
        WHERE MISJA_URL_WIKI = :url
    """)

    for kat_i, kat_url in enumerate(kategorie, start=1):
        print(f"\n=== KATEGORIA [{kat_i}/{len(kategorie)}]: {kat_url} ===")

        questy = wyscrapuj_linki_z_kategorii_z_paginacja(
            kategoria_url=kat_url,
            sleep_s=sleep_s
        )

        print(f"Znaleziono {len(questy)} questów")

        for i, url in enumerate(questy, start=1):
            print(f"  [{i}/{len(questy)}] Hashuję: {url}")

            try:
                wynik = parsuj_misje_z_url(url)
                misja_url = wynik.get("Źródło", {}).get("url")

                with silnik.connect() as conn:
                    row = conn.execute(q_select_misja_id, {"url": misja_url}).first()

                if not row:
                    print("    → brak w MISJE, dodaję do LINKI_DO_SCRAPOWANIA")
                    zapisz_link_do_scrapowania(
                        silnik=silnik,
                        url=misja_url,
                        zrodlo=zrodlo
                    )
                    time.sleep(sleep_s)
                    continue

                misja_id = row[0]

                zapisz_zrodlo_do_db(
                    silnik=silnik,
                    tabela_zrodlo=tabela_zrodlo,
                    misja_id=misja_id,
                    wynik=wynik,
                    zrodlo=zrodlo
                )

                print("    + zapisano hashe do ZRODLO")

            except Exception as e:
                print(f"    ! BŁĄD: {e}")

            time.sleep(sleep_s)