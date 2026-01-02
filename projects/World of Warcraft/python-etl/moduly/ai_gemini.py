import json
import os
import time

from dotenv import load_dotenv
from google import genai

from sqlalchemy import text, bindparam

import pandas as pd

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
            model="gemini-3-flash-preview",
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