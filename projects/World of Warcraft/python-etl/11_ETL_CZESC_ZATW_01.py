from moduly.zatwierdzanie import stworz_excele_do_zatwierdzenia_tlumaczen
from moduly.db_core import utworz_engine_do_db

silnik = utworz_engine_do_db()

stworz_excele_do_zatwierdzenia_tlumaczen(
    silnik, 
    fabula="The Cult Within",
    sciezka=r"C:\Users\piotr\OneDrive\____Moje-MOJE\MyProjects_4Fun\projects\World of Warcraft\excel-zatwierdzenia\The Cult Within.xlsx"
)