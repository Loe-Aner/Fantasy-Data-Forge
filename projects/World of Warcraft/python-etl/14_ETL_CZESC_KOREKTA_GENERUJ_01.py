from moduly.zatwierdzanie import stworz_excele_do_recznych_tlumaczen
from moduly.db_core import utworz_engine_do_db
from moduly.sciezki import sciezka_excel_zatwierdzenia

silnik = utworz_engine_do_db()

# TUTAJ JEST PROCES, W KTORYM GENERUJE TLUMACZENIA, DLA KTORYCH WSKAZNIK CZESCI WSPOLNEJ (TZN LICZBA ZNAKOW I ZDAN Z NOWO WYSCRAPOWANEJ TRESCI VS ARCHIWUM) 
# JEST WIEKSZA NIZ 0.7000
# ZMIAN JEST MALO, DLATEGO RECZNIE TO TLUMACZE
stworz_excele_do_recznych_tlumaczen(
    silnik,
    fabula="The Path of Light",
    sciezka=sciezka_excel_zatwierdzenia("The Path of Light_RECZNE.xlsx")
)