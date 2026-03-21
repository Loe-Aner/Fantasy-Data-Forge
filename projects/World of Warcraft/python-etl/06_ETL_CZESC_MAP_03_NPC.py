from moduly.ai import pobierz_metadane_npc_do_csv
from moduly.db_core import utworz_engine_do_db

silnik = utworz_engine_do_db()

# TEN SKRYPT ROBI RESEARCH NA TEMAT DANYCH NPC-A
pobierz_metadane_npc_do_csv(
    silnik,
    liczba_watkow=10
)

# POTEM WEJSC DO PLIKU I ODSWIEZYC, A NASTEPNIE WERYFIKACJA I KOREKTA TLUMACZEN