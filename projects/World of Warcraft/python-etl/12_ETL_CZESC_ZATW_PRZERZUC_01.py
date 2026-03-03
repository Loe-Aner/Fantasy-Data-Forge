from moduly.zatwierdzanie import zatwierdz_tlumaczenia
from moduly.db_core import utworz_engine_do_db
from moduly.sciezki import sciezka_excel_zatwierdzenia

silnik = utworz_engine_do_db()

zatwierdz_tlumaczenia(
    silnik,
    sciezka_excel_zatwierdzenia("The Cult Within_DONE.xlsx")
)