from pomocnicze_funkcje_sql import *
from scraper import parsuj_misje_z_url
import time
from sqlalchemy import text

# BAZA 
kategorie = [
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_80-83",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_80-90",
    "https://warcraft.wiki.gg/wiki/Category:Quests_at_83",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_83-88",
    #"https://warcraft.wiki.gg/wiki/Category:Quests_at_88-90",
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


# SCRAPOWANIE TYLKO TYCH MISJI, KTORYCH NIE MA W BAZIE DANYCH
linki = pobierz_linki_do_scrapowania(silnik)

print(f"Do przerobienia: {len(linki)} misji")

for i, url in enumerate(linki, start=1):
    print(f"\n[{i}/{len(linki)}] Przetwarzam: {url}")

    try:
        wynik = parsuj_misje_z_url(url)

        zapisz_npc_i_status_do_db_z_wyniku(
            silnik=silnik,
            tabela_npc="dbo.NPC",
            tabela_npc_statusy="dbo.NPC_STATUSY",
            szukaj_wg=["Start_NPC", "Koniec_NPC"],
            wyscrapowana_tresc=wynik,
            zrodlo="wiki",
            status="0_ORYGINAŁ"
        )

        misja_id = zapisz_misje_i_statusy_do_db_z_wyniku(
            silnik=silnik,
            wynik=wynik,
            tabela_npc="dbo.NPC",
            tabela_misje="dbo.MISJE",
            tabela_misje_statusy="dbo.MISJE_STATUSY",
            status="0_ORYGINAŁ"
        )

        zapisz_dialogi_statusy_do_db_z_wyniku(
            silnik=silnik,
            wynik=wynik,
            misja_id=misja_id,
            tabela_npc="dbo.NPC",
            tabela_npc_statusy="dbo.NPC_STATUSY",
            tabela_dialogi_statusy="dbo.DIALOGI_STATUSY",
            zrodlo="wiki",
            status="0_ORYGINAŁ"
        )

        zapisz_zrodlo_do_db(
            silnik=silnik,
            tabela_zrodlo="dbo.ZRODLO",
            misja_id=misja_id,
            wynik=wynik,
            zrodlo="wiki"
        )

        print(f"OK - zapisano MISJA_ID = {misja_id}")
        usun_link_z_kolejki(silnik, url)

    except Exception as e:
        print(f"BŁĄD przy {url}: {e}")

    time.sleep(3)