from moduly.ai import tych_npcow_nie_tlumacz, zaladuj_api_i_klienta
from moduly.db_core import utworz_engine_do_db

silnik = utworz_engine_do_db()
klient = zaladuj_api_i_klienta("API_TLUMACZENIE")

# WYRZUCA DO CSV TYLKO TYCH NPCOW, KTORYCH NIE POWINNO SIE TLUMACZYC - ZGODNIE Z PROMPTEM W FUNKCJI
# CHCE MIEC NAD TLUMACZENIAMI I JAKOSCIA MAKSYMALNA KONTROLE
# NAJPIERW ODSWIEZYC PLIK A POTEM WYJSC
tych_npcow_nie_tlumacz(
    silnik=silnik, 
    klient=klient
    )

# PO ODSWIEZEINU WEJSC DO PLIKU I ODSWIEZYC, A NASTEPNIE PRZYPISAC ICH
