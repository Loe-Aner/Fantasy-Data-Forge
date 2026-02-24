import pandas as pd

from moduly.db_core import utworz_engine_do_db
from moduly.etl_excel import aktualizuj_id_misji_wowhead_z_excela
from moduly.sciezki import sciezka_excel_mappingi

silnik = utworz_engine_do_db()

sciezka = sciezka_excel_mappingi("korekta_id_misji_wowhead.xlsx")
arkusz = "do_db"
kolumny = ["MISJA_ID_MOJE_PK", "MISJA_ID_Z_GRY_FINAL", "MISJA_URL_WOWHEAD_FINAL"]
df = pd.read_excel(sciezka, sheet_name=arkusz, usecols=kolumny)

# NIESTETY JEST TAK, ZE JEDEN TYTUL MOZE MIEC 2 ROZNE ID W GRZE I TYM SAMYM DWA ROZNE LINKI
# PRZYKLAD: MISJA O TYM SAMYM TYTULE W GRZE JEST PODZIALONA NA 3 KATEGORIA: ALLIANCE, HORDA ALBO MONK_ALLIANCE ITP
# PONIEWAZ NIE ZACZYNAM OD ID TYLKO OD DANYCH NA WARCRAFT WIKI (TO MOJE GLOWNE ZRODLO, BO TAM JEST KOMPLET INFORMACJI OD MISJI AZ PO DIALOGI NPCOW), TRZEBA SKORYGOWAC NIEKTORE MISJE - MOIM KLUCZEM JEST TYTUL MISJI MIEDZY WIKI A WOWHEAD (WLASNE ID JEST NADAWANE DOPIERO PO INSERTCIE DO DB)
# PONIŻSZY SKRYPT TO ROBI (KORYGUJE MAPPINGI DLA ID Z GRY KTORE SIE POWTARZAJA)

# MISJE O ID 123456789 NIGDY NIE BEDA BRANE POD UWAGE

aktualizuj_id_misji_wowhead_z_excela(
    df, 
    silnik
    )
