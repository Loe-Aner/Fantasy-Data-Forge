import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed
from moduly.db_core import utworz_engine_do_db
from moduly.repo_misje import pobierz_liste_id_dla_dodatku
from moduly.ai_gemini import pobierz_przetworz_zapisz_batch_lista
from moduly.sciezki import sciezka_excel_mappingi

# OSTATNIE REVIEW: 22.01.2026 -> brak zmian

# TUTAJ GENEROWANE SA PACZKI CSV'EK - NUMERY W NAZWIE OZNACZAJA PRZEDZIALY ID PRZYPISANE PRZEZE MNIE W DB
# PRZED WYGENEROWANIEM, KOD SPRAWDZA W ARKUSZU 'do_tabeli_misje_slowa_kluczowe' CZY MISJE JUŻ SĄ - JAK TAK TO ICH NIE PRZERABIA
# PACZKI TE POTEM SA LACZONE W POWER QUERY W EXCELU

silnik = utworz_engine_do_db()

NAZWA_DODATKU = 'Midnight'
BATCH_SIZE = 100
MAX_WORKERS = 4
SCIEZKA_EXCEL = sciezka_excel_mappingi("slowa_kluczowe.xlsx")

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
    print("Nie znaleziono pliku Excel.")
    zrobione_id = set()
except ValueError:
    print("Arkusz lub kolumna nie istnieje.")
    zrobione_id = set()

id_do_przerobienia = sorted(list(set(wszystkie_id_sql) - zrobione_id))

liczba_misji = len(id_do_przerobienia)

def zadanie_dla_watku(paczka_id, indeks_startowy):
    try:
        pobierz_przetworz_zapisz_batch_lista(
            silnik=silnik,
            lista_id_batch=paczka_id,
            nazwa_dodatku=NAZWA_DODATKU
        )
        return f"Zakończono paczkę od ID {paczka_id[0]} (rozmiar: {len(paczka_id)})"
    except Exception as e:
        return f"Błąd w paczce od indeksu {indeks_startowy}: {e}"

if liczba_misji == 0:
    print(f"Wszystkie misje dla dodatku '{NAZWA_DODATKU}' są już w Excelu!")
else:
    print("--- START ZADANIA ---")
    print(f"Dodatek: {NAZWA_DODATKU}")
    print(f"W bazie łącznie: {len(wszystkie_id_sql)}")
    print(f"Pozostało do zrobienia: {liczba_misji}")
    print(f"Wielkość paczki: {BATCH_SIZE}")
    print(f"Liczba wątków: {MAX_WORKERS}")
    print("---------------------")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        lista_zadan = []
        
        for i in range(0, liczba_misji, BATCH_SIZE):
            paczka_id = id_do_przerobienia[i : i + BATCH_SIZE]
            zadanie = executor.submit(zadanie_dla_watku, paczka_id, i)
            lista_zadan.append(zadanie)
        
        for ukonczone_zadanie in as_completed(lista_zadan):
            print(ukonczone_zadanie.result())

    print("--- KONIEC ---")
