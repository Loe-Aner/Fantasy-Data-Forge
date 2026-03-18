from moduly.zatwierdzanie import stworz_excele_do_zatwierdzenia_tlumaczen
from moduly.db_core import utworz_engine_do_db
from moduly.sciezki import sciezka_excel_zatwierdzenia

silnik = utworz_engine_do_db()

# SKRYPT GENERUJACY PRZYGOTOWANEGO EXCELA Z FORMATOWANIEM DO ZATWIERDZENIA TEKSTU
# TUTAJ TLUMACZE NOWOSCI
stworz_excele_do_zatwierdzenia_tlumaczen(
    silnik, 
    fabula="Path of de Hash'ey",
    sciezka=sciezka_excel_zatwierdzenia("Path of de Hash'ey.xlsx")
)