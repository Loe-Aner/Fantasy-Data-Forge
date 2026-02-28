from moduly.ai import przetlumacz_nazwy_npc, zaladuj_api_i_klienta
from moduly.db_core import utworz_engine_do_db

silnik = utworz_engine_do_db()
klient = zaladuj_api_i_klienta("API_TLUMACZENIE")

# PROPOZYCJA TLUMACZEN NAZW NPCOW
# NAJPIERW WYKONAC KROKI W SKRYPCIE 01_NPC
przetlumacz_nazwy_npc(
    silnik=silnik, 
    klient=klient
    )

# POTEM WEJSC DO PLIKU I ODSWIEZYC, A NASTEPNIE WERYFIKACJA I KOREKTA TLUMACZEN
