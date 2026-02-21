import asyncio

from moduly.db_core import utworz_engine_do_db
from moduly.repo_kolejka_linkow import (
    pobierz_linki_do_scrapowania, 
    usun_link_z_kolejki
)
from moduly.repo_zrodlo import zapisz_zrodlo_do_db
from moduly.scraping_jobs import hashuj_kategorie_i_zapisz_zrodlo
from moduly.maintenance_hashe import roznice_hashe_usun_rekordy_z_db
from moduly.services_persist_wynik import (
    zapisz_npc_i_status_do_db_z_wyniku,
    zapisz_misje_i_statusy_do_db_z_wyniku,
    zaktualizuj_misje_z_wowhead_w_db_z_wyniku,
    zapisz_dialogi_statusy_do_db_z_wyniku
)
from scraper_wiki_async import parsuj_wiele_misji_async
from scraper_wiki_main import parsuj_misje_z_url, dekompresuj_html


# BAZA
kategorie = [
    "https://warcraft.wiki.gg/wiki/Category:Quests_at_1-80",
    "https://warcraft.wiki.gg/wiki/Category:Quests_at_10-80",
    "https://warcraft.wiki.gg/wiki/Category:Quests_at_20-80",
    "https://warcraft.wiki.gg/wiki/Category:Quests_at_30-80",
    "https://warcraft.wiki.gg/wiki/Category:Quests_at_40-80",
    "https://warcraft.wiki.gg/wiki/Category:Quests_at_50-80"

    # "https://warcraft.wiki.gg/wiki/Category:Quests_at_25-80"
    # "https://warcraft.wiki.gg/wiki/Category:Quests_at_90",
    # "https://warcraft.wiki.gg/wiki/Category:Quests_at_80",
    # "https://warcraft.wiki.gg/wiki/Category:Quests_at_80-83",
    # "https://warcraft.wiki.gg/wiki/Category:Quests_at_80-90",
    # "https://warcraft.wiki.gg/wiki/Category:Quests_at_83",
    # "https://warcraft.wiki.gg/wiki/Category:Quests_at_83-88",
    # "https://warcraft.wiki.gg/wiki/Category:Quests_at_88-90"

    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_70",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_70-73",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_70-80",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_73-75",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_75-78",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_78-80",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_60",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_68-70",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_68-80"
]
silnik = utworz_engine_do_db()

# OSTATNIE REVIEW: 20.01.2026

hashuj_kategorie_i_zapisz_zrodlo(
    silnik=silnik, 
    kategorie=kategorie, 
    zrodlo="wiki"
    )


# USUWA MISJE I POWIAZANE REKORDY Z NIA W PRZYPADKU GDY NA WIKI ZOSTANIE WYKRYTA NOWA TRESC DLA TEJ MISJI
# USUNIETA MISJA (DOKLADNIEJ JEJ URL) JEST WRZUCANY NA DBO.LINKI_DO_SCRAPOWANIA, BY POBRANO TA TRESC PONOWNIE PRZEZ KODY PONIZEJ
# NIE SCRAPUJE OD NOWA TYLKO ODCZYTUJE ZAHASHOWANY BODY ZE STRONY
roznice_hashe_usun_rekordy_z_db(
    silnik=silnik, 
    zrodlo_insert_url="wiki"
    )


# SCRAPOWANIE TYLKO TE MISJE, KTORYCH NIE MA W BAZIE DANYCH
# CZYLI BAZUJE NA TYM CO JEST W dbo.LINKI_DO_SCRAPOWANIA (TABELA PRZYGOTOWANA W SKRYPCIE WYŻEJ)
linki_z_kolejki = pobierz_linki_do_scrapowania(silnik)

print(f"Do przerobienia: {len(linki_z_kolejki)} misji")

MAX_CONCURRENCY = 4
BATCH_SIZE = 100


def chunks(lista, size):
    for i in range(0, len(lista), size):
        yield lista[i : i + size]

przerobione = 0

for batch_nr, batch in enumerate(chunks(linki_z_kolejki, BATCH_SIZE), start=1):
    print(f"\n=== PACZKA {batch_nr} | {len(batch)} linków ===")

    zadania_lokalne = [z for z in batch if z["html_skompresowany"] is not None]
    zadania_sieciowe = [z for z in batch if z["html_skompresowany"] is None]
    
    gotowe_wyniki = []

    for item in zadania_lokalne:
        url = item["url"]
        try:
            print(f" [CACHE] Przetwarzam lokalnie: {url}")
            czysty_html = dekompresuj_html(item["html_skompresowany"])
            wynik = parsuj_misje_z_url(url, html_content=czysty_html)
            gotowe_wyniki.append((url, wynik))
        except Exception as e:
            print(f" ! Błąd cache dla {url}: {e}. Przenoszę do pobrania z sieci.")
            zadania_sieciowe.append(item)

    if zadania_sieciowe:
        urls_only = [z["url"] for z in zadania_sieciowe]
        print(f" [WEB] Pobieram {len(urls_only)} linków z wiki...")
        pary_z_sieci = asyncio.run(parsuj_wiele_misji_async(urls_only, max_concurrency=MAX_CONCURRENCY))
        gotowe_wyniki.extend(pary_z_sieci)

    for i, (url, wynik) in enumerate(gotowe_wyniki, start=1):
        print(f"\n[{i}/{len(batch)}] Zapisuję: {url}")

        if wynik is None:
            print("SKIP - nie udało się pobrać/sparsować (wynik=None)")
            continue

        try:
            zapisz_npc_i_status_do_db_z_wyniku(
                silnik=silnik,
                tabela_npc="dbo.NPC",
                tabela_npc_statusy="dbo.NPC_STATUSY",
                szukaj_wg=["Start_NPC", "Koniec_NPC"],
                wyscrapowana_tresc=wynik,
                zrodlo="wiki",
                status="0_ORYGINAŁ",
                jezyk="EN"
            )

            misja_id = zapisz_misje_i_statusy_do_db_z_wyniku(
                silnik=silnik,
                wynik=wynik,
                tabela_npc="dbo.NPC",
                tabela_misje="dbo.MISJE",
                tabela_misje_statusy="dbo.MISJE_STATUSY",
                status="0_ORYGINAŁ",
                jezyk="EN"
            )

            zaktualizuj_misje_z_wowhead_w_db_z_wyniku(
                silnik=silnik,
                wynik=wynik,
                misja_id=misja_id,
                tabela_misje="dbo.MISJE"
            )

            zapisz_dialogi_statusy_do_db_z_wyniku(
                silnik=silnik,
                wynik=wynik,
                misja_id=misja_id,
                tabela_npc="dbo.NPC",
                tabela_npc_statusy="dbo.NPC_STATUSY",
                tabela_dialogi_statusy="dbo.DIALOGI_STATUSY",
                zrodlo="wiki",
                status="0_ORYGINAŁ",
                jezyk="EN"
            )

            zapisz_zrodlo_do_db(
                silnik=silnik,
                tabela_zrodlo="dbo.ZRODLO",
                misja_id=misja_id,
                wynik=wynik,
                zrodlo="wiki",
            )

            print(f"OK - zapisano MISJA_ID = {misja_id}")
            usun_link_z_kolejki(silnik, url)
            przerobione += 1

        except Exception as e:
            print(f"BŁĄD przy zapisie {url}: {e}")

print(f"\nKoniec. Przerobione OK: {przerobione}/{len(linki_z_kolejki)}")