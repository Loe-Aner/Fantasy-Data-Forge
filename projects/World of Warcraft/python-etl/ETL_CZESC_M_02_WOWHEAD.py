import pandas as pd

from scraper_wowhead import *
from pomocnicze_funkcje_sql import *

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
aktualizuj_misje_z_excela(
    df, 
    silnik
)