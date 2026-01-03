import pandas as pd

from moduly.db_core import utworz_engine_do_db
from moduly.repo_misje import pobierz_liste_id_dla_dodatku
from moduly.ai_gemini import pobierz_przetworz_zapisz_batch_lista



# TUTAJ GENEROWANE SA PACZKI CSV'EK - NUMERY W NAZWIE OZNACZAJA PRZEDZIALY ID PRZYPISANE PRZEZE MNIE W DB
# PRZED WYGENEROWANIEM, KOD SPRAWDZA W ARKUSZU 'do_tabeli_misje_slowa_kluczowe' CZY MISJE JUŻ SĄ - JAK TAK TO ICH NIE PRZERABIA
# PACZKI TE POTEM SA LACZONE W POWER QUERY W EXCELU

silnik = utworz_engine_do_db()

NAZWA_DODATKU = 'Midnight'
BATCH_SIZE = 100
SCIEZKA_EXCEL = r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\slowa_kluczowe.xlsx"


wszystkie_id_sql = pobierz_liste_id_dla_dodatku(silnik, NAZWA_DODATKU)

try:
    print("Sprawdzam, które misje są już w Excelu...")
    df_excel = pd.read_excel(
        SCIEZKA_EXCEL, 
        sheet_name="do_tabeli_misje_slowa_kluczowe", 
        usecols=["MISJA_ID_MOJE_FK"]
    )
    zrobione_id = set(df_excel["MISJA_ID_MOJE_FK"].dropna().astype(int))
    print(f"W Excelu znaleziono {len(zrobione_id)} unikalnych, przetworzonych misji.")
    
except FileNotFoundError:
    print("⚠️ Nie znaleziono pliku Excel - zakładam, że to pierwsze uruchomienie.")
    zrobione_id = set()
except ValueError:
    print("⚠️ Arkusz lub kolumna nie istnieje - zakładam 0 zrobionych.")
    zrobione_id = set()

id_do_przerobienia = sorted(list(set(wszystkie_id_sql) - zrobione_id))

liczba_misji = len(id_do_przerobienia)

if liczba_misji == 0:
    print(f"🎉 Wszystkie misje dla dodatku '{NAZWA_DODATKU}' są już w Excelu!")
else:
    print(f"--- START ZADANIA ---")
    print(f"Dodatek: {NAZWA_DODATKU}")
    print(f"W bazie łącznie: {len(wszystkie_id_sql)}")
    print(f"Już w Excelu: {len(zrobione_id)}")
    print(f"Pozostało do zrobienia: {liczba_misji}")
    print(f"Planowane paczki: {(liczba_misji // BATCH_SIZE) + 1}")
    print("---------------------")

    for i in range(0, liczba_misji, BATCH_SIZE):   
        paczka_id = id_do_przerobienia[i : i + BATCH_SIZE]
        
        print(f"Przetwarzam misje {i+1} do {min(i+BATCH_SIZE, liczba_misji)} (z puli {liczba_misji})...")
        
        pobierz_przetworz_zapisz_batch_lista(
            silnik=silnik,
            lista_id_batch=paczka_id,
            nazwa_dodatku=NAZWA_DODATKU
        )
    print("--- KONIEC ---")