import json
import os
import time

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

def instrukcja_tlumacz(tekst_npc: str, tekst_slowa_kluczowe: str) -> str:
    return f"""
            Jesteś profesjonalnym tłumaczem fantasy specjalizującym się w grze World of Warcraft i powiązanych z tą marką dziełach (gry, książki, short stories, komiksy, wszystko). Twoim zadaniem jest przetłumaczenie danych misji (tytuły, cele, opisy, dialogi) z języka angielskiego na wysokiej jakości język polski, z zachowaniem ścisłych reguł formatowania danych.
            DANE WEJŚCIOWE: Otrzymasz słowniki nazw własnych oraz główny obiekt JSON z treścią misji.
            OBOWIĄZKOWE SŁOWNIKI NAZW WŁASNYCH: Podczas tłumaczenia musisz bezwzględnie stosować się do poniższych mapowań. Lista NPC (Angielski -> Polski): {tekst_npc}
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

def misje_dialogi_po_polsku_zapisz_do_db(
        silnik, kraina: str | None = None, fabula: str | None = None, id_misji: int | None = None
    ):
    """
    Tłumaczy a następnie redaguje misje z podanej krainy & linii fabularnej LUB konkretną misję po ID.
    Sprawdza, czy misja nie została już przetłumaczona.
    Zapisuje treści do bazy danych.
    """

    q_select_tresc = text(f"""
    WITH hashe AS (
        SELECT 
            m.MISJA_ID_MOJE_PK,
            z.HTML_SKOMPRESOWANY,
            ROW_NUMBER() OVER (
                PARTITION BY z.MISJA_ID_MOJE_FK 
                ORDER BY z.DATA_WYSCRAPOWANIA DESC
            ) AS r
        FROM dbo.ZRODLO AS z
        INNER JOIN dbo.MISJE AS m
        ON z.MISJA_ID_MOJE_FK = m.MISJA_ID_MOJE_PK
        WHERE 1=1
        AND m.MISJA_ID_Z_GRY IS NOT NULL
        AND m.MISJA_ID_Z_GRY <> 123456789
                          
        {
            "AND m.MISJA_ID_MOJE_PK = :id_misji"
            if (kraina is None or fabula is None)
            else "AND m.KRAINA_EN = :kraina_en \
                  AND m.NAZWA_LINII_FABULARNEJ_EN = :fabula_en"
        }
                          
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.MISJE_STATUSY AS ms
            WHERE ms.MISJA_ID_MOJE_FK = m.MISJA_ID_MOJE_PK
            AND ms.STATUS = N'1_PRZETŁUMACZONO'
        )
    )
    SELECT 
        MISJA_ID_MOJE_PK, HTML_SKOMPRESOWANY
    FROM hashe
    WHERE r = 1
    ORDER BY MISJA_ID_MOJE_PK
    ;
    """)

    q_select_npc = text("""
    WITH wszystkie_idki AS (
        SELECT
            tabela_wartosci.ID_NPC
        FROM dbo.MISJE AS m
        CROSS APPLY (
            VALUES
                (m.NPC_START_ID),
                (m.NPC_KONIEC_ID)
        ) AS tabela_wartosci (ID_NPC)
        WHERE m.MISJA_ID_MOJE_PK = :misja_id

        UNION

        SELECT
            ds.NPC_ID_FK
        FROM dbo.DIALOGI_STATUSY AS ds
        WHERE ds.MISJA_ID_MOJE_FK = :misja_id
    ),

    oczyszczone_dane AS (
        SELECT
            wi.ID_NPC,
            ns.STATUS,
            CASE
                WHEN CHARINDEX('[', ns.NAZWA) > 0
                THEN RTRIM(LEFT(ns.NAZWA, CHARINDEX('[', ns.NAZWA) - 1))
                ELSE ns.NAZWA
            END AS CZYSTA_NAZWA
        FROM wszystkie_idki AS wi
        INNER JOIN dbo.NPC_STATUSY AS ns
            ON wi.ID_NPC = ns.NPC_ID_FK
    )

    SELECT DISTINCT
        pvt.[0_ORYGINAŁ],
        pvt.[3_ZATWIERDZONO]
    FROM oczyszczone_dane
    PIVOT (
        MAX(CZYSTA_NAZWA)
        FOR STATUS IN ([0_ORYGINAŁ], [3_ZATWIERDZONO])
    ) AS pvt
    ;
    """)

    q_select_slowa_kluczowe = text("""
        SELECT 
            sk.SLOWO_EN,
            sk.SLOWO_PL
        FROM dbo.MISJE_SLOWA_KLUCZOWE AS msk
        INNER JOIN dbo.SLOWA_KLUCZOWE AS sk
        ON msk.SLOWO_ID = sk.SLOWO_ID_PK
        WHERE msk.MISJA_ID_MOJE_FK = :misja_id
    """)

    parametry = {"kraina_en": kraina, "fabula_en": fabula, "id_misji": id_misji}

    with silnik.connect() as conn:
        wyniki_z_bazy = conn.execute(q_select_tresc, parametry).mappings().all()

        print(f"Znaleziono rekordów: {len(wyniki_z_bazy)}\n")
        
        for wiersz in wyniki_z_bazy:
            misja_id = wiersz["MISJA_ID_MOJE_PK"]
            zakodowane_dane = wiersz["HTML_SKOMPRESOWANY"]

            npc_z_bazy = conn.execute(q_select_npc, {"misja_id": misja_id}).all()
            slowa_kluczowe_z_bazy = conn.execute(q_select_slowa_kluczowe, {"misja_id": misja_id}).all()

            if not zakodowane_dane:
                print(f"SKIP [ID: {misja_id}] - Brak skompresowanego HTML w bazie.")
                continue

            try:
                skompresowane_bajty = base64.b64decode(zakodowane_dane)
                tekst_html = zlib.decompress(skompresowane_bajty).decode("utf-8")

                surowe_dane = parsuj_misje_z_url(None, html_content=tekst_html)
                #print(surowe_dane)
                
                przetworzone_dane = przefiltruj_dane_misji(surowe_dane, jezyk="EN")

                wsad_dla_geminisia_npc = set(npc for npc in npc_z_bazy)
                wsad_dla_geminisia_sk = set(slowo for slowo in slowa_kluczowe_z_bazy)

                wsad_dla_geminisia_cialo = json.dumps(przetworzone_dane, indent=4, ensure_ascii=False)

                npc_tekst = "\n".join([f"- {n[0]} -> {n[1]}" for n in wsad_dla_geminisia_npc if n[0] and n[1]])
                sk_tekst = "\n".join([f"- {k[0]} -> {k[1]}" for k in wsad_dla_geminisia_sk if k[0] and k[1]])
                #print(lista_sk_tekst)

                try:
                    odpowiedz = klient.models.generate_content(
                        model="gemini-3-pro-preview",
                        contents=wsad_dla_geminisia_cialo,
                        config={
                            "system_instruction": instrukcja_tlumacz(npc_tekst, sk_tekst),
                            "response_mime_type": "application/json"
                        }
                    )

                    przetlumaczone = json.loads(odpowiedz.text)
                    
                    zapisz_misje_dialogi_ai_do_db(
                        silnik=silnik, 
                        misja_id=misja_id, 
                        przetlumaczone=przetlumaczone, 
                        status="1_PRZETŁUMACZONO"
                    )

                except Exception as er:
                    print(f"BŁĄD w tłumaczeniu/zapisie misji: {misja_id} --> {er}")

            except Exception as e:
                print(f"BŁĄD ogólny przy ID {misja_id}: {e}")