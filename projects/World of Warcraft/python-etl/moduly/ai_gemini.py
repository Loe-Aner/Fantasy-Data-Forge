import json
import os
import time
import concurrent.futures
from datetime import datetime

from dotenv import load_dotenv
from google import genai

from sqlalchemy import text, bindparam
import pandas as pd

from moduly.services_persist_wynik import przefiltruj_dane_misji, zapisz_misje_dialogi_ai_do_db
from scraper_wiki_main import parsuj_misje_z_url

import json
import zlib
import base64

def zaladuj_api_i_klienta(
        nazwa_api: str
    ):
    load_dotenv()
    API_KEY = os.environ.get(nazwa_api)

    if not API_KEY:
        raise ValueError("BRAK KLUCZA!")
    else:
        print("KLUCZ ZWARTY I GOTOWY!")
        return genai.Client(api_key=API_KEY)

def pobierz_przetworz_zapisz_batch_lista(
        silnik, 
        lista_id_batch, 
        nazwa_dodatku,
        folder_zapisz: str = r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\surowe\slowa_kluczowe_batche"
    ):
    
    min_b = min(lista_id_batch)
    max_b = max(lista_id_batch)
    nazwa_pliku = f"batch_{min_b}_{max_b}.csv"
    pelna_sciezka = os.path.join(folder_zapisz, nazwa_pliku)

    klient = zaladuj_api_i_klienta("API_SŁOWA_KLUCZOWE")
    
    q = text("""
    WITH Statusy_Agg AS (
        SELECT MISJA_ID_MOJE_FK, STRING_AGG(ISNULL(TRESC, ''), '. ') AS TRESC_STATUSOW
        FROM dbo.MISJE_STATUSY 
        WHERE STATUS = '0_ORYGINAŁ' 
        GROUP BY MISJA_ID_MOJE_FK
    ),
    Dialogi_Agg AS (
        SELECT MISJA_ID_MOJE_FK, STRING_AGG(ISNULL(TRESC, ''), '. ') AS TRESC_DIALOGOW
        FROM dbo.DIALOGI_STATUSY 
        WHERE STATUS = '0_ORYGINAŁ' 
        GROUP BY MISJA_ID_MOJE_FK
    )
    SELECT 
        m.MISJA_ID_MOJE_PK,
        m.MISJA_TYTUL_EN + '. ' + COALESCE(s.TRESC_STATUSOW, '') + '. ' + COALESCE(d.TRESC_DIALOGOW, '') AS PELNY_TEKST
    FROM dbo.MISJE AS m
    LEFT JOIN Statusy_Agg AS s 
        ON m.MISJA_ID_MOJE_PK = s.MISJA_ID_MOJE_FK
    LEFT JOIN Dialogi_Agg d 
        ON m.MISJA_ID_MOJE_PK = d.MISJA_ID_MOJE_FK
    WHERE m.DODATEK_EN = :dodatek
      AND m.MISJA_ID_MOJE_PK IN :lista_id
    """).bindparams(bindparam('lista_id', expanding=True))

    with silnik.connect() as conn:
        slownik = conn.execute(q, {
            "lista_id": list(lista_id_batch), 
            "dodatek": nazwa_dodatku
        }).mappings()
        
        wsad_dla_geminisia = [
            {"id": w["MISJA_ID_MOJE_PK"], "txt": w["PELNY_TEKST"]} 
            for w in slownik
        ]

    if not wsad_dla_geminisia:
        print(f"Brak danych dla batcha {min_b}-{max_b}.")
        return None

    instrukcja = """
    You are an expert World of Warcraft Translator and Lorewalker.
    Analyze the provided quest texts.
    
    TASK:
    1. Extract Proper Nouns (Names, Locations, Organizations, Items).
    2. Provide a Polish translation for each extracted term based on WoW context.
       - Names (Jaina): Keep original or standard Polish equivalent if exists.
       - Items/Objects (Twilight's Blade): Translate to Polish (Ostrze Zmierzchu).
       - Locations (Stormwind): Use official PL localization (Wichrogród) or keep English if untranslatable.
    3. Assign a Category: NPC, LOCATION, ITEM, ORG, OTHER.

    CRITICAL OUTPUT RULES:
    - Return ONLY a JSON list of objects.
    - Structure:
      [
        {
          "quest_id": 123,
          "extracted": [
             {"en": "Jaina Proudmoore", "pl": "Jaina Proudmoore", "type": "NPC"},
             {"en": "Dalaran", "pl": "Dalaran", "type": "LOCATION"},
             {"en": "Strange Key", "pl": "Dziwny Klucz", "type": "ITEM"}
          ]
        }
      ]
    - Return "extracted": [] if nothing found.
    - Do not skip any Quest ID.
    """

    try:
        odpowiedz = klient.models.generate_content(
            model="gemini-3-pro-preview",
            contents=json.dumps(wsad_dla_geminisia),
            config={
                "system_instruction": instrukcja,
                "response_mime_type": "application/json"
            }
        )
        
        wynik_lista = json.loads(odpowiedz.text)

        df = pd.DataFrame(wynik_lista)
        df_exploded = df.explode("extracted")
        df_exploded = df_exploded.dropna(subset=["extracted"])

        if df_exploded.empty:
            print(f"Batch {min_b}-{max_b} przetworzony, ale nie znaleziono słów kluczowych.")
            return None

        dane_szczegolowe = df_exploded["extracted"].apply(pd.Series)
        
        df_final = pd.concat([df_exploded["quest_id"], dane_szczegolowe], axis=1)
        df_final.to_csv(pelna_sciezka, index=False, encoding="utf-8-sig", sep=";")
        
        print(f"Zapisano: {nazwa_pliku} (Ilość wierszy: {len(df_final)})")
        time.sleep(2) 
        return pelna_sciezka
                
    except Exception as e:
        print(f"Błąd w batchu {min_b}-{max_b}: {e}")
        return None

def instrukcja_tlumacz(tekst_npc: str, tekst_slowa_kluczowe: str) -> str:
    return f"""
            Jesteś profesjonalnym tłumaczem fantasy specjalizującym się w grze World of Warcraft i powiązanych z tą marką dziełach (gry, książki, short stories, komiksy, wszystko). Twoim zadaniem jest przetłumaczenie danych misji (tytuły, cele, opisy, dialogi) z języka angielskiego na wysokiej jakości język polski, z zachowaniem ścisłych reguł formatowania danych.
            DANE WEJŚCIOWE: Otrzymasz słowniki nazw własnych oraz główny obiekt JSON z treścią misji.
            OBOWIĄZKOWE SŁOWNIKI NAZW WŁASNYCH: Podczas tłumaczenia musisz bezwzględnie stosować się do poniższych mapowań. Czasami NPC może mieć tytuł "Brak Danych" - w takim przypadku zwróć dokładnie ten sam tytuł, nazwa "Brak Danych" jest zabiegiem technicznym i uznajmy że to też faktyczny NPC. W drugą stronę to samo: jeżeli npc_en będzie równy '', to zamiast '' w npc_pl daj 'Brak Danych'.
            Lista NPC (Angielski -> Polski): {tekst_npc}
            Lista Słów Kluczowych (Angielski -> Polski): {tekst_slowa_kluczowe}

            ZASADY TŁUMACZENIA (STYL I TREŚĆ):
            Klimat i Styl: Zachowaj sens, emocje i ton oryginału. Tłumaczenie musi brzmieć naturalnie dla polskiego gracza fantasy (klimat World of Warcraft). Unikaj kalk językowych.
            Płynność: Unikaj sztywnego, dosłownego przekładu. Stwórz płynny, profesjonalny tekst literacki. Jeśli angielska konstrukcja brzmi topornie, wygładź ją w polszczyźnie, zachowując pierwotny sens.
            Wierność i Czystość: Nie dodawaj żadnych informacji od siebie, nie komentuj tekstu. Jeśli w oryginale jest literówka – skoryguj ją w tłumaczeniu po cichu.
            Placeholdery Techniczne: Zachowaj nienaruszone wszelkie znaczniki techniczne, takie jak: {{PLAYER_NAME}}, %s, %d, $n, $g, znaczniki kolorów (np. |cFFFF0000...|r) oraz tagi formatowania (, \n). Muszą znaleźć się w tłumaczeniu w odpowiednim miejscu.
            Nazwy Własne: Używaj nazw z podanych wyżej słowników. Jeśli nazwy nie ma w słowniku – zostaw ją w oryginale (chyba że jest to pospolite słowo możliwe do naturalnego przetłumaczenia, np. "Lake" -> "Jezioro", ale nazwy miast/krain zostawiasz, np. "Ironforge" no chyba, że podano tłumaczenie w słowniku).
            Kontekst: Pamiętaj, że teksty są ze sobą powiązane. Dialogi (gossipy/dymki) dotyczą tej konkretnej misji.

            ZASADY TECHNICZNE (STRUKTURA JSON I KLUCZE):
            Struktura: Zwrócony JSON musi mieć identyczną strukturę zagnieżdżenia jak oryginał. Nie usuwaj żadnych obiektów.
            Podmiana Kluczy Językowych:
            Wszędzie tam, gdzie klucz kończy się na "_EN" (np. "Misje_EN", "Treść_EN", "npc_en"), zmień końcówkę na "_PL" (np. "Misje_PL", "Treść_PL", "npc_pl").
            Zachowaj wielkość liter przedrostka (np. "npc_en" -> "npc_pl", ale "NPC_EN" -> "NPC_PL").
            Klucze bez sufiksu językowego (np. "id", "typ", "nr_bloku") pozostaw BEZ ZMIAN.
            Puste Pola: Jeśli jakakolwiek sekcja, lista lub pole w oryginale jest puste (np. "Gossipy_Dymki_EN": [] lub "TRESC": ""), w wynikowym JSONie musi pozostać puste (z odpowiednio zmienioną nazwą klucza na _PL). Nie usuwaj pustych kluczy.
            Kolejność i ID: Każdy element list (np. dialogi) musi zostać zwrócony w tej samej kolejności i z tym samym ID, co w oryginale.
            Format Wyjściowy: Zwróć tylko i wyłącznie poprawny kod JSON. Bez bloków markdown (```json), bez komentarzy wstępnych czy końcowych.
    """


def instrukcja_redaktor(tekst_oryginalny: str, tekst_npc: str, tekst_slowa_kluczowe: str) -> str:
    return f"""
### ROLA:
Jesteś Głównym Redaktorem (Lead Editor) polskiej lokalizacji gry AAA z gatunku High Fantasy (World of Warcraft).
Twoim zadaniem NIE JEST tłumaczenie, lecz REDAKCJA I SZLIFOWANIE istniejącego tekstu roboczego. Masz uczynić go barwnym, epickim i naturalnym.

### DANE KONTEKSTOWE:
1. Oryginalny tekst misji (dla kontekstu fabularnego): {tekst_oryginalny}
2. Obowiązujące nazwy NPC: {tekst_npc}
3. Słowa kluczowe: {tekst_slowa_kluczowe}

### ZASADY STYLU (POLSKI LANGUAGE GUIDE):
Twój cel to styl "Literackie Fantasy", a nie "Techniczne Tłumaczenie".
1. **Unikaj "Zaimkozy":** W języku polskim podmiot jest domyślny. Zamiast "On poszedł do lasu" pisz "Ruszył do lasu". Zamiast "Ja ci dziękuję" pisz "Dziękuję".
2. **Szyk zdania:** Unikaj szyku angielskiego (Podmiot-Orzeczenie-Dopełnienie). Wersja polska ma być elastyczna.
3. **Słownictwo:** Używaj słownictwa budującego klimat (np. zamiast "duży" użyj "ogromny", "potężny", "zwalisty" w zależności od kontekstu).
4. **Dynamika:** W opisach walki i zagrożenia zdania mają być krótkie i mocne. W opisach lore – bardziej kwieciste.
5. **NPC Voice (Dopasowanie do postaci):**
   Rozpoznaj charakter mówiącego po kontekście i stylu oryginału. Pamiętaj, że poniższe to tylko przykłady – każda rasa ma swoją specyfikę:
   - **Ork/Wojownik:** Prosty, żołnierski język, zgrubienia, honor, siła. Krótko i na temat.
   - **Elf/Mag:** Język wyrafinowany, pełne zdania, lekka archaizacja, dystans, czasami wyższość.
   - **Krasnolud:** Styl rubaszny, serdeczny, głośny. Odniesienia do brody, piwa, gór, kamienia i przodków.
   - **Goblin:** Cwaniacki, szybki, kupiecki żargon, slang uliczny. Nastawienie na zysk, wybuchy i "interesy" ("Czas to pieniądz, przyjacielu!").

### ZADANIA TECHNICZNE:
1. Otrzymasz JSON z parami: `Tekst_EN` (oryginał) oraz `Tekst_PL` (wersja robocza).
2. **MODYFIKUJESZ TYLKO `Tekst_PL`.** `Tekst_EN` służy tylko do weryfikacji sensu.
3. Zachowaj wierność merytoryczną oryginałowi (nie zmieniaj faktów, liczb, imion).
4. **Placeholder Security:** BEZWZGLĘDNIE zachowaj nienaruszone tagi: {{PLAYER_NAME}}, $n, $g, |c...|r, \n. Muszą być w tekście wynikowym dokładnie tam, gdzie wymaga tego logika zdania.
5. Nie usuwaj pustych pól. Zachowaj strukturę JSON i ID.

### OUTPUT:
Zwróć wyłącznie JSON o identycznej strukturze jak wejściowy, z poprawionymi polami `_PL`.
"""

def przetworz_pojedyncza_misje(wiersz, silnik, klient):
    """
    Ta funkcja wykonuje całą pracę dla JEDNEJ misji.
    Będzie uruchamiana równolegle w wielu wątkach.
    """
    misja_id = wiersz["MISJA_ID_MOJE_PK"]
    zakodowane_dane = wiersz["HTML_SKOMPRESOWANY"]
    
    with silnik.connect() as conn:
        try:
            q_select_npc = text("""
            WITH wszystkie_idki AS (
                SELECT tabela_wartosci.ID_NPC FROM dbo.MISJE AS m
                CROSS APPLY (VALUES (m.NPC_START_ID), (m.NPC_KONIEC_ID)) AS tabela_wartosci (ID_NPC)
                WHERE m.MISJA_ID_MOJE_PK = :misja_id
                UNION
                SELECT ds.NPC_ID_FK FROM dbo.DIALOGI_STATUSY AS ds WHERE ds.MISJA_ID_MOJE_FK = :misja_id
            ),
            oczyszczone_dane AS (
                SELECT wi.ID_NPC, ns.STATUS,
                CASE WHEN CHARINDEX('[', ns.NAZWA) > 0 THEN RTRIM(LEFT(ns.NAZWA, CHARINDEX('[', ns.NAZWA) - 1)) ELSE ns.NAZWA END AS CZYSTA_NAZWA
                FROM wszystkie_idki AS wi
                INNER JOIN dbo.NPC_STATUSY AS ns ON wi.ID_NPC = ns.NPC_ID_FK
            )
            SELECT DISTINCT pvt.[0_ORYGINAŁ], pvt.[3_ZATWIERDZONO]
            FROM oczyszczone_dane
            PIVOT (MAX(CZYSTA_NAZWA) FOR STATUS IN ([0_ORYGINAŁ], [3_ZATWIERDZONO])) AS pvt;
            """)

            q_select_sk = text("""
                SELECT sk.SLOWO_EN, sk.SLOWO_PL FROM dbo.MISJE_SLOWA_KLUCZOWE AS msk
                INNER JOIN dbo.SLOWA_KLUCZOWE AS sk ON msk.SLOWO_ID = sk.SLOWO_ID_PK
                WHERE msk.MISJA_ID_MOJE_FK = :misja_id
            """)

            npc_z_bazy = conn.execute(q_select_npc, {"misja_id": misja_id}).all()
            slowa_kluczowe_z_bazy = conn.execute(q_select_sk, {"misja_id": misja_id}).all()

            if not zakodowane_dane:
                print(f"SKIP [ID: {misja_id}] - Brak danych.")
                return

            skompresowane_bajty = base64.b64decode(zakodowane_dane)
            tekst_html = zlib.decompress(skompresowane_bajty).decode("utf-8")
            surowe_dane = parsuj_misje_z_url(None, html_content=tekst_html)
            przetworzone_dane = przefiltruj_dane_misji(surowe_dane, jezyk="EN")

            wsad_npc = set(n for n in npc_z_bazy)
            wsad_sk = set(s for s in slowa_kluczowe_z_bazy)
            wsad_json = json.dumps(przetworzone_dane, indent=4, ensure_ascii=False)
            
            txt_npc = "\n".join([f"- {n[0]} -> {n[1]}" for n in wsad_npc if n[0] and n[1]])
            txt_sk = "\n".join([f"- {k[0]} -> {k[1]}" for k in wsad_sk if k[0] and k[1]])

            # 4. ETAP 1: TŁUMACZENIE
            print(f"--- [ID: {misja_id}] Start Tłumaczenia... ---")
            odp_tlumacz = klient.models.generate_content(
                model="gemini-3-pro-preview",
                contents=wsad_json,
                config={"system_instruction": instrukcja_tlumacz(txt_npc, txt_sk), "response_mime_type": "application/json"}
            )
            przetlumaczone = json.loads(odp_tlumacz.text)
            
            # Zapis Etapu 1
            zapisz_misje_dialogi_ai_do_db(silnik, misja_id, przetlumaczone, "1_PRZETŁUMACZONO")

            # 5. ETAP 2: REDAKCJA
            print(f"--- [ID: {misja_id}] Start Redakcji... ---")
            wsad_redakcja = json.dumps(przetlumaczone, indent=4, ensure_ascii=False)
            odp_redaktor = klient.models.generate_content(
                model="gemini-3-pro-preview",
                contents=wsad_redakcja,
                config={"system_instruction": instrukcja_redaktor(wsad_json, txt_npc, txt_sk), "response_mime_type": "application/json"}
            )
            zredagowane = json.loads(odp_redaktor.text)

            # Zapis Etapu 2
            zapisz_misje_dialogi_ai_do_db(silnik, misja_id, zredagowane, "2_ZREDAGOWANO")
            
            print(f"+++ [ID: {misja_id}] GOTOWE (Tłumaczenie + Redakcja) +++")

        except Exception as e:
            print(f"!!! BŁĄD przy misji {misja_id}: {e}")

def misje_dialogi_po_polsku_zapisz_do_db_multithread(
        silnik, kraina: str | None = None, fabula: str | None = None, id_misji: int | None = None, liczba_watkow: int = 5
    ):
    
    klient = zaladuj_api_i_klienta("API_TLUMACZENIE")
    
    warunki_sql = ""
    
    if id_misji is not None:
        warunki_sql = "AND m.MISJA_ID_MOJE_PK = :id_misji"
    
    else:
        czesci_warunku = []
        
        if kraina is not None:
            czesci_warunku.append("AND m.KRAINA_EN = :kraina_en")
            
        if fabula is not None:
            czesci_warunku.append("AND m.NAZWA_LINII_FABULARNEJ_EN = :fabula_en")
        
        if czesci_warunku:
            warunki_sql = " ".join(czesci_warunku)
        else:
            print("BŁĄD: Nie podano żadnych parametrów filtrowania (ID, Kraina lub Fabuła). Przerywam, żeby nie pobrać całej bazy.")
            return

    q_select_tresc = text(f"""
    WITH hashe AS (
        SELECT m.MISJA_ID_MOJE_PK, z.HTML_SKOMPRESOWANY,
            ROW_NUMBER() OVER (PARTITION BY z.MISJA_ID_MOJE_FK ORDER BY z.DATA_WYSCRAPOWANIA DESC) AS r
        FROM dbo.ZRODLO AS z
        INNER JOIN dbo.MISJE AS m ON z.MISJA_ID_MOJE_FK = m.MISJA_ID_MOJE_PK
        WHERE 1=1 AND m.MISJA_ID_Z_GRY IS NOT NULL AND m.MISJA_ID_Z_GRY <> 123456789
        
        {warunki_sql}  /* <--- Tutaj wstawiają się dynamiczne filtry */
        
        AND NOT EXISTS (SELECT 1 FROM dbo.MISJE_STATUSY AS ms WHERE ms.MISJA_ID_MOJE_FK = m.MISJA_ID_MOJE_PK AND ms.STATUS = N'1_PRZETŁUMACZONO')
    )
    SELECT MISJA_ID_MOJE_PK, HTML_SKOMPRESOWANY FROM hashe WHERE r = 1 ORDER BY MISJA_ID_MOJE_PK;
    """)

    parametry = {"kraina_en": kraina, "fabula_en": fabula, "id_misji": id_misji}

    print("Pobieranie listy misji do przetworzenia...")
    with silnik.connect() as conn:
        lista_zadan = conn.execute(q_select_tresc, parametry).mappings().all()

    liczba_zadan = len(lista_zadan)
    
    if liczba_zadan == 0:
        print("Nie znaleziono żadnych misji pasujących do kryteriów.")
        return

    print(f"Znaleziono {liczba_zadan} misji do przetworzenia. Uruchamiam {liczba_watkow} wątków...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=liczba_watkow) as executor:
        futures = []
        for wiersz in lista_zadan:
            future = executor.submit(przetworz_pojedyncza_misje, wiersz, silnik, klient)
            futures.append(future)
        
        for future in concurrent.futures.as_completed(futures):
            pass

    print("\n--- ZAKOŃCZONO PRZETWARZANIE WIELOWĄTKOWE ---")


def tych_npcow_nie_tlumacz(silnik, klient):
    sciezka = r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\npc.xlsx"
    sciezka_zapis = r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\surowe\npc_nie_do_tlumaczenia"
    zakladka = "surowe"

    print(f"--- START ---")
    print(f"1. Wczytuję dane z pliku: {sciezka}")
    df = pd.read_excel(sciezka, sheet_name=zakladka, usecols=["NPC_ID_MOJE_PK", "NAZWA", "NAZWA_PL_FINAL"], index_col="NPC_ID_MOJE_PK")
    
    df = df.loc[df["NAZWA_PL_FINAL"].isna()]
    liczba_wierszy = len(df)
    
    dzisiejsza_data = datetime.now().strftime("%d_%m_%Y")
    print(f"2. Data dla plików: {dzisiejsza_data}")
    print(f"3. Liczba NPC do sprawdzenia (puste NAZWA_PL_FINAL): {liczba_wierszy}")

    co_ile = 100
    licznik_batchy = 1
    total_batchy = (liczba_wierszy + co_ile - 1) // co_ile

    for i in range(0, liczba_wierszy, co_ile):
        start = i
        koniec = i + co_ile
        
        print(f"\n[Batch {licznik_batchy}/{total_batchy}] Przetwarzanie wierszy od {start} do {koniec}...")
        
        batch_seria = df["NAZWA"].iloc[start:koniec]
        dfj = batch_seria.to_json(force_ascii=False)

        instrukcja = """
            Jesteś ekspertem od uniwersum World of Warcraft. 
            Analizujesz listę NPC (ID: NAZWA_ANGIELSKA). 
            Twoim jedynym zadaniem jest zwrócenie JSONa zawierającego WYŁĄCZNIE te wpisy, których NIE NALEŻY tłumaczyć na język polski.

            Kryteria pozostawienia w oryginale:
            1. Pojedyncze imiona własne (np. Agatha, Om'sirik, Orwenya).
            
            Kogo NIE zwracaj (tych będę tłumaczył):
            1. Pełnych imion i nazwisk (np. Jaina Proudmoore, Corithras Moonrage).
            2. Nazw z tytułami, przydomkami lub nazwami pospolitymi (np. Sergeant Willem, Jack the Hammer, Stormwind Guard). 

            Zwróć wyłącznie poprawny JSON w tej samej strukturze (ID: NAZWA). 
            Jeśli w danej paczce wszyscy NPC wymagają tłumaczenia, zwróć pusty słownik {}.
        """

        print(f"   -> Wysyłam zapytanie do API (rozmiar JSON: {len(dfj)} znaków)...")
        start_czas = time.time()
        
        odpowiedz = klient.models.generate_content(
            model="gemini-3-flash-preview",
            contents=dfj,
            config={
                "system_instruction": instrukcja, 
                "response_mime_type": "application/json"
            }
        )
        
        czas_trwania = time.time() - start_czas
        print(f"   <- Otrzymano odpowiedź w {czas_trwania:.2f} sek.")
        
        zaladowane = json.loads(odpowiedz.text)
        liczba_znalezionych = len(zaladowane)

        if zaladowane:
            df_wynikowy = pd.DataFrame.from_dict(zaladowane, orient="index", columns=["NAZWA"])
            nazwa_pliku = f"{dzisiejsza_data}_odrzuty_batch_{start}_{koniec}.csv"
            sciezka_kompletna = os.path.join(sciezka_zapis, nazwa_pliku)
            df_wynikowy.to_csv(sciezka_kompletna, sep=";", encoding="utf-8-sig", index_label="NPC_ID_MOJE_PK")
            print(f"   -> SUKCES: Znaleziono {liczba_znalezionych} wpisów nie do tłumaczenia. Zapisano plik: {nazwa_pliku}")
        else:
            print(f"   -> INFO: Brak wpisów nie do tłumaczenia w tej paczce (wszystkie do tłumaczenia). Plik nie został utworzony.")
        
        licznik_batchy += 1

    print(f"\n--- KONIEC PROCESU ---")


def przetlumacz_nazwy_npc(silnik, klient):
    sciezka = r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\npc.xlsx"
    sciezka_zapis = r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\surowe\propozycja_tlumaczen_npc"
    zakladka = "surowe"

    print(f"--- START TŁUMACZENIA (Z WYSZUKIWARKĄ GOOGLE) ---")
    print(f"1. Wczytuję dane z pliku: {sciezka}")
    df = pd.read_excel(sciezka, sheet_name=zakladka, usecols=["NPC_ID_MOJE_PK", "NAZWA", "NAZWA_PL_FINAL"], index_col="NPC_ID_MOJE_PK")
    
    df = df.loc[df["NAZWA_PL_FINAL"].isna()]
    liczba_wierszy = len(df)
    
    dzisiejsza_data = datetime.now().strftime("%d_%m_%Y")
    print(f"2. Data dla plików: {dzisiejsza_data}")
    print(f"3. Liczba NPC do przetłumaczenia: {liczba_wierszy}")

    co_ile = 100
    licznik_batchy = 1
    total_batchy = (liczba_wierszy + co_ile - 1) // co_ile

    for i in range(0, liczba_wierszy, co_ile):
        start = i
        koniec = i + co_ile
        
        print(f"\n[Batch {licznik_batchy}/{total_batchy}] Tłumaczenie wierszy od {start} do {koniec}...")
        
        batch_seria = df["NAZWA"].iloc[start:koniec]
        dfj = batch_seria.to_json(force_ascii=False)

        instrukcja = """
        Jesteś Głównym Specjalistą ds. Lokalizacji (Lead Localization Expert) uniwersum World of Warcraft na rynek polski. 
        Twoim zadaniem jest przetłumaczenie listy nazw NPC z języka angielskiego na polski, zachowując oficjalną nomenklaturę Blizzarda (tzw. "Canon") oraz klimat High Fantasy.

        KONTEKST:
        Otrzymujesz surowy obiekt JSON w formacie {ID: "English Name"}. Musisz zwrócić identyczną strukturę JSON {ID: "Polska Nazwa"}.

        NARZĘDZIA:
        Masz dostęp do wyszukiwarki Google. ZANIM przetłumaczysz nazwę, która wygląda na unikalną lub specyficzną dla lore (np. nazwiska bohaterów, nazwy frakcji), UŻYJ WYSZUKIWARKI, aby sprawdzić oficjalne polskie tłumaczenie na stronach takich jak Wowhead (pl) lub WoW Wiki.

        ZASADY LOKALIZACJI (STYLE GUIDE):

        1. WIERNOŚĆ LORE (CANON):
        - Nazwy miast i krain muszą być zgodne z polską wersją gry (np. Stormwind -> Wichrogród, Ironforge -> Żelazna Kuźnia, Undercity -> Podmiasto).
        - Znane nazwiska tłumaczymy zgodnie z oficjalną wersją (np. Hellscream -> Piekłorycz, Stormrage -> Burzogniewny). Jeśli postać nie ma spolszczonego nazwiska w grze (np. Jaina Proudmoore), pozostaw je w oryginale.

        2. GRAMATYKA I SKŁADNIA:
        - Konstrukcje "X of Y": Zawsze używaj dopełniacza, nigdy przyimka "z" (chyba że to konieczne geograficznie).
            * ŹLE: Strażnik z Wichrogrodu.
            * DOBRZE: Strażnik Wichrogrodu.
        - Konstrukcje "The [Noun]": W języku polskim pomijamy przedimki lub stosujemy inwersję, by brzmiało to naturalnie.
            * The Butcher -> Rzeźnik (nie "Ten Rzeźnik").
            * The Lich King -> Król Licz.

        3. TYTUŁY I RANGI:
        - Tłumacz stopnie wojskowe i dworskie na polskie odpowiedniki (Sergeant -> Sierżant, Captain -> Kapitan, Squire -> Giermek, Lady -> Lady/Dama - zależnie od kontekstu, Peon -> Robotnik).

        4. POTWORY I ZWIERZĘTA (WIELKIE VS MAŁE LITERY):
        - W języku angielskim nazwy mobów są pisane Title Case (np. "Rabid Wolf"). W języku polskim nazwy pospolite piszemy małą literą, chyba że to nazwa własna.
            * Angry Boar -> Wściekły dzik (nie "Wściekły Dzik").
            * Defias Thug -> Zbir Nieskalanych (Nieskalani to nazwa własna organizacji).

        5. PRZYDOMKI:
        - Tłumacz przydomki tak, by oddawały charakter postaci.
            * Gruul the Dragonkiller -> Gruul Zabójca Smoków.
            * Scarred Visage -> Bliznowaty.

        6. NAZWY NIEPRZETŁUMACZALNE:
        - Jeśli nazwa jest imieniem własnym bez znaczenia (np. "Zargh", "Om'riggor"), pozostaw ją bez zmian.

        INSTRUKCJA TECHNICZNA:
        - Nie dodawaj żadnych wyjaśnień, wstępów ani znaczników markdown (```json).
        - Zwróć czysty, poprawny syntaktycznie obiekt JSON.
        """

        print(f"   -> Wysyłam dane do API (rozmiar JSON: {len(dfj)} znaków)...")
        start_czas = time.time()
        
        try:
            odpowiedz = klient.models.generate_content(
                model="gemini-3-pro-preview",
                contents=dfj,
                config={
                    "system_instruction": instrukcja, 
                    "response_mime_type": "application/json",
                    "tools": [{"google_search": {}}]
                }
            )
            
            czas_trwania = time.time() - start_czas
            print(f"   <- Otrzymano odpowiedź w {czas_trwania:.2f} sek.")
            
            zaladowane = json.loads(odpowiedz.text)
            
            if isinstance(zaladowane, list):
                print(f"   -> INFO: API zwróciło listę zamiast słownika. Próbuję przekonwertować...")
                nowy_slownik = {}
                for item in zaladowane:
                    if isinstance(item, dict):
                        nowy_slownik.update(item)
                zaladowane = nowy_slownik

            liczba_przetlumaczonych = len(zaladowane)

            if zaladowane:
                df_wynikowy = pd.DataFrame.from_dict(zaladowane, orient="index", columns=["NAZWA_PL_PROPOZYCJA"])
                nazwa_pliku = f"{dzisiejsza_data}_tlumaczenia_batch_{start}_{koniec}.csv"
                sciezka_kompletna = os.path.join(sciezka_zapis, nazwa_pliku)
                df_wynikowy.to_csv(sciezka_kompletna, sep=";", encoding="utf-8-sig", index_label="NPC_ID_MOJE_PK")
                print(f"   -> SUKCES: Zapisano plik: {nazwa_pliku} ({liczba_przetlumaczonych} rekordów)")
            else:
                print(f"   -> WARNING: Pusty wynik (słownik) dla tego batcha.")

        except Exception as e:
            print(f"   !!! BŁĄD w batchu {start}-{koniec}: {e}")
            if 'odpowiedz' in locals() and hasattr(odpowiedz, 'text'):
                print(f"   !!! Fragment otrzymanego JSONa: {odpowiedz.text[:500]}...")
        
        licznik_batchy += 1

    print(f"\n--- KONIEC PROCESU ---")