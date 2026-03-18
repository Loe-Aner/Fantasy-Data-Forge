from moduly.zatwierdzanie import stworz_excele_do_recznych_tlumaczen
from moduly.db_core import utworz_engine_do_db
from moduly.sciezki import sciezka_excel_zatwierdzenia

silnik = utworz_engine_do_db()

stworz_excele_do_recznych_tlumaczen(
    silnik,
    fabula="Midnight",
    sciezka=sciezka_excel_zatwierdzenia("Midnight_RECZNE.xlsx")
)
