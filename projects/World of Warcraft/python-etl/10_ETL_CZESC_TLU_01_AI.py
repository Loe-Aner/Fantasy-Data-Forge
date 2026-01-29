from moduly.ai_gemini import misje_dialogi_po_polsku_zapisz_do_db_multithread
from moduly.db_core import utworz_engine_do_db
from moduly.repo_misje import ujednolic_tytuly_misji

silnik = utworz_engine_do_db()

# TŁUMACZY A POTEM REDAGUJE, ZAPISUJĄC DO BAZY DANYCH Z ODPOWIEDNIMI STATUSAMI
# MOŻNA PODAĆ DOWOLNIE KTORY PARAMETR
misje_dialogi_po_polsku_zapisz_do_db_multithread(silnik, fabula="The Dragon Isles")

# KOREKTA MISJI - JEŻELI NP. DWIE MISJE O ID 37, 38 MAJĄ TEN SAM TYTUŁ PO ENG, ALE INNY PO PL, TO WYBIERAM PIERWSZY TYTUŁ
ujednolic_tytuly_misji(silnik)