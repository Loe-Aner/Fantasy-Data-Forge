import pandas as pd

from moduly.db_core import utworz_engine_do_db
from moduly.etl_excel import aktualizuj_misje_z_excela

silnik = utworz_engine_do_db()

sciezka = r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\mapping_01.xlsx"
arkusz = "mapping_01"
kolumny = ["MISJA_ID_Z_GRY", "MISJA_TYTUL_EN", "DODATEK_EN", "MISJA_URL_WOWHEAD", 
           "NAZWA_LINII_FABULARNEJ_EN", "DODANO_W_PATCHU", "KONTYNENT_EN",
           "KONTYNENT_PL", "KRAINA_EN_FINAL", "KRAINA_PL", "DODATEK_PL"]
df = pd.read_excel(sciezka, sheet_name=arkusz, usecols=kolumny).dropna(how="all")
df["NAZWA_LINII_FABULARNEJ_EN"] = df["NAZWA_LINII_FABULARNEJ_EN"].fillna("NoData")


# TUTAJ JEST ROBIONY UPDATE TABELI dbo.MISJE O POZOSTALE ATRYBUTY JAK LINK DO WOWHEAD, GLOWNE ID MISJI Z GRY ITP.
# ZANIM ODPALE SKRYPT, TRZEBA SPRAWDZIC CZY FORMULY W EXCELU NIE WYSYPALY SIE PO DODANIU DANYCH WYZEJ
# LINKI DO WOWHEAD I ID MISJI JUŻ SĄ W DB - WYKLUCZYŁEM TE KOLUMNY Z UPDATE'U
# OSTATNIE REVIEW: 21.01.2026
aktualizuj_misje_z_excela(
    df, 
    silnik
)