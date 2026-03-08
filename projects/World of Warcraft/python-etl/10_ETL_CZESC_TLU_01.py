from moduly.ai import misje_dialogi_przetlumacz_zredaguj_zapisz
from moduly.db_core import utworz_engine_do_db
from moduly.repo_misje import ujednolic_tytuly_misji
from moduly.services_persist_wynik import usun_niezredagowane

silnik = utworz_engine_do_db()

# TŁUMACZY A POTEM REDAGUJE, ZAPISUJĄC DO BAZY DANYCH Z ODPOWIEDNIMI STATUSAMI
# MOŻNA PODAĆ DOWOLNIE KTORY PARAMETR
# BIERZE POD UWAGE TYLKO MISJE Z TRESCIA
misje_dialogi_przetlumacz_zredaguj_zapisz(silnik, dodatek="Shadowlands", liczba_watkow=1, 
                                                 dostawca_tlumaczenie="claude", dostawca_redakcja="claude")

# KOREKTA MISJI - JEŻELI NP. DWIE MISJE O ID 37, 38 MAJĄ TEN SAM TYTUŁ PO ENG, ALE INNY PO PL, TO WYBIERAM PIERWSZY TYTUŁ
ujednolic_tytuly_misji(silnik)

# TUTAJ SKRYPT USUWAJACY MISJE (ZE STATUSEM PRZETLUMACZONE), KTORE DOSTALY BLAD PRZY REDAKCJI
# DLA BEZPIECZENSTWA NIECH WYKONA PROCES JESZCZE RAZ
usun_niezredagowane(silnik)