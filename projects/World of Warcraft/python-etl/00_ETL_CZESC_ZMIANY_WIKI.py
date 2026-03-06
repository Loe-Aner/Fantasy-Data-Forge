from moduly.analiza_html import pobierz_przerzuc_questy_per_lvle
from moduly.db_core import utworz_engine_do_db
from moduly.utils import usun_stare_daty_pokaz_zmiany

silnik = utworz_engine_do_db()

# PONIZSZY SKRYPT TWORZY DF Z LICZBA DOSTEPNYCH MISJI PER GRUPY POZIOMOW, NP 1-10, 10-20 ITP
# NATEPNIE PRZERZUCA JE DO DBO.MISJE_ZMIANY_WIKI
pobierz_przerzuc_questy_per_lvle(
    silnik=silnik,
    url="https://warcraft.wiki.gg/wiki/Category:Quests_by_level"
)

# PONIŻSZY SKRYPT SPRAWDZA ZMIANY W LICZBIE MISJI I PRINTUJE REZULTAT
# CZYLI NAJNOWSZE (POWYZSZA FUNKCJA) VS POPRZEDNIE
# W BAZIE DANYCH ZATRZYMYWANE SA TYLKO DWA OSTATNIE SCRAPOWANIA, PONIEWAZ NIE MA SENSU WIECEJ
# CELEM JEST SPRAWDZENIE GDZIE DOSZLY MISJE I WYSCRAPOWANIE ICH
usun_stare_daty_pokaz_zmiany(
    silnik
)