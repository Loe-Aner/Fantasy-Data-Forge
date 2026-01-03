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
    zapisz_dialogi_statusy_do_db_z_wyniku
)
from scraper_wiki_async import parsuj_wiele_misji_async


# BAZA
kategorie = [
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_80",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_80-83",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_80-90",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_83",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_83-88",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_88-90",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_1-10",
    "https://warcraft.wiki.gg/wiki/Category:Quests_at_2",
    "https://warcraft.wiki.gg/wiki/Category:Quests_at_6-10",
    "https://warcraft.wiki.gg/wiki/Category:Quests_at_7-10",
    "https://warcraft.wiki.gg/wiki/Category:Quests_at_10"
]
silnik = utworz_engine_do_db()


# HASHUJE MISJE Z LINKOW PONIZEJ W 'KATEGORIE' I ZAPISUJE DO ZRODLO.
# PO DRODZE SPRAWDZA CZY MISJA JUZ ISTNIEJE
hashuj_kategorie_i_zapisz_zrodlo(
    silnik=silnik, 
    kategorie=kategorie, 
    zrodlo='wiki'
    )


# USUWA MISJE I POWIAZANE REKORDY Z NIA W PRZYPADKU GDY NA WIKI ZOSTANIE WYKRYTA NOWA TRESC DLA TEJ MISJI
# USUNIETA MISJA (DOKLADNIEJ JEJ URL) JEST WRZUCANY NA DBO.LINKI_DO_SCRAPOWANIA, BY POBRANO TA TRESC PONOWNIE PRZEZ KODY PONIZEJ
roznice_hashe_usun_rekordy_z_db(
    silnik=silnik, 
    zrodlo_insert_url="wiki"
    )


# SCRAPOWANIE TYLKO TE MISJE, KTORYCH NIE MA W BAZIE DANYCH
# CZYLI BAZUJE NA TYM CO JEST W dbo.LINKI_DO_SCRAPOWANIA (TABELA PRZYGOTOWANA W SKRYPCIE WYŻEJ)
linki = pobierz_linki_do_scrapowania(silnik)

print(f"Do przerobienia: {len(linki)} misji")

MAX_CONCURRENCY = 4
BATCH_SIZE = 32


def chunks(lista: list[str], size: int):
    for i in range(0, len(lista), size):
        yield lista[i : i + size]

przerobione = 0

for batch_nr, batch in enumerate(chunks(linki, BATCH_SIZE), start=1):
    print(f"\n=== PACZKA {batch_nr} | {len(batch)} linków ===")

    pary = asyncio.run(parsuj_wiele_misji_async(batch, max_concurrency=MAX_CONCURRENCY))

    for i, (url, wynik) in enumerate(pary, start=1):
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
            )

            misja_id = zapisz_misje_i_statusy_do_db_z_wyniku(
                silnik=silnik,
                wynik=wynik,
                tabela_npc="dbo.NPC",
                tabela_misje="dbo.MISJE",
                tabela_misje_statusy="dbo.MISJE_STATUSY",
                status="0_ORYGINAŁ",
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

print(f"\nKoniec. Przerobione OK: {przerobione}/{len(linki)}")