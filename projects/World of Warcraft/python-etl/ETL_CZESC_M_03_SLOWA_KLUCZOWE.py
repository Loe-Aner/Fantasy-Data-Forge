import pandas as pd

from scraper_wowhead import *
from pomocnicze_funkcje_sql import *

silnik = utworz_engine_do_db()

# TUTAJ GENEROWANE SA PACZKI CSV'EK - NUMERY W NAZWIE OZNACZAJA PRZEDZIALY ID PRZYPISANE PRZEZE MNIE W DB
# PACZKI TE POTEM SA LACZONE W POWER QUERY W EXCELU
NAZWA_DODATKU = 'Midnight'
BATCH_SIZE = 100

wszystkie_id = pobierz_liste_id_dla_dodatku(silnik, NAZWA_DODATKU)
liczba_misji = len(wszystkie_id)

if liczba_misji == 0:
    print(f"Brak misji dla dodatku '{NAZWA_DODATKU}'.")
else:
    print(f"--- START ZADANIA ---")
    print(f"Dodatek: {NAZWA_DODATKU}")
    print(f"Znaleziono misji: {liczba_misji}")
    print(f"Planowane paczki: {(liczba_misji // BATCH_SIZE) + 1}")
    print("---------------------")

    for i in range(0, liczba_misji, BATCH_SIZE):  
        paczka_id = wszystkie_id[i : i + BATCH_SIZE]
        
        print(f"Przetwarzam misje {i+1} do {min(i+BATCH_SIZE, liczba_misji)} (z puli {liczba_misji})...")
        
        pobierz_przetworz_zapisz_batch_lista(
            silnik=silnik,
            lista_id_batch=paczka_id,
            nazwa_dodatku=NAZWA_DODATKU
        )
    print("--- KONIEC ---")