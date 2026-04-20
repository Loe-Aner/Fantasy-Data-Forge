import json
import os
import time
import concurrent.futures
from datetime import datetime
from typing import Any

from google.genai import types as genai_types

from sqlalchemy import text, bindparam
import pandas as pd

from moduly.services_persist_wynik import (
    przefiltruj_dane_misji, 
    save_quests_dialogues_to_db
)
from moduly.ai_prompty import (
    instrukcja_slowa_kluczowe,
    instrukcja_tlumacz_npc,
    instrukcja_tych_npc_nie,
    instrukcja_dane_npc_stala,
    instrukcja_dane_npc_zmienna
)
from moduly.ai_core import (
    MODEL_GEMINI_POZOSTALE,
    MODEL_GEMINI_POMOCNICZY,
    SCHEMAT_ODPOWIEDZI_DANE_NPC,
    pobierz_thinking_config_gemini_high,
    zaladuj_api_i_klienta,
    zaloguj_uzycie_gemini,
)

from moduly.ai_prompty_misje import (
    translator,
    editor
)
from moduly.ai_modele import (
    llm_translator,
    llm_editor
)
from moduly.ai_logi import (
    create_logs,
    save_ai_logs_to_db
)

from moduly.sciezki import sciezka_excel_mappingi
from scraper_wiki_main import parsuj_misje_z_url
from moduly.utils import sklej_warunki_w_WHERE
import zlib
import base64

def pobierz_przetworz_zapisz_batch_lista(
        silnik, 
        lista_id_batch, 
        nazwa_dodatku,
        folder_zapisz: str = sciezka_excel_mappingi("surowe", "slowa_kluczowe_batche")
    ):
    
    min_b = min(lista_id_batch)
    max_b = max(lista_id_batch)
    
    safe_dodatek = nazwa_dodatku.replace(" ", "_").replace(":", "").replace("'", "")
    nazwa_pliku = f"batch_{min_b}_{max_b}_{safe_dodatek}.csv"
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
    """).bindparams(bindparam("lista_id", expanding=True))

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

    try:
        odpowiedz = klient.models.generate_content(
                    model=MODEL_GEMINI_POZOSTALE,
                    contents=json.dumps(wsad_dla_geminisia),
                    config={
                        "system_instruction": instrukcja_slowa_kluczowe(),
                        "response_mime_type": "application/json",
                        "tools": [{"google_search": {}}],
                        "thinking_config": pobierz_thinking_config_gemini_high(),
                    }
                )
        zaloguj_uzycie_gemini(odpowiedz, "slowa_kluczowe")

        wynik_lista = json.loads(odpowiedz.text)

        df = pd.DataFrame(wynik_lista)
        df_rozbite = (
            df
            .explode("extracted")
            .dropna(subset=["extracted"])
        )

        if df_rozbite.empty:
            print(f"Batch {min_b}-{max_b} przetworzony, ale nie znaleziono słów kluczowych.")
            return None

        dane_szczegolowe = df_rozbite["extracted"].apply(pd.Series)
        
        df_final = pd.concat([df_rozbite["quest_id"], dane_szczegolowe], axis=1)
        df_final.to_csv(pelna_sciezka, index=False, encoding="utf-8-sig", sep=";")
        
        print(f"Zapisano: {nazwa_pliku} (Ilość wierszy: {len(df_final)})")
        time.sleep(2) 
        return pelna_sciezka
                
    except Exception as e:
        print(f"Błąd w batchu {min_b}-{max_b}: {e}")
        return None

def require_mapping(value: Any, path: str, errors: list[str]) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value

    errors.append(f"{path} (oczekiwano obiektu)")
    return None


def require_list(value: Any, path: str, errors: list[str]) -> list[Any] | None:
    if isinstance(value, list):
        return value

    errors.append(f"{path} (oczekiwano listy)")
    return None


def require_keys(value: dict[str, Any] | None, path: str, keys: list[str], errors: list[str]) -> None:
    if value is None:
        return

    for key in keys:
        if key not in value:
            errors.append(f"{path}.{key}")


def validate_quest_content_response(parsed: Any, misja_id: int, stage: str) -> None:
    """
    Minimalna walidacja shape odpowiedzi AI.
    TypedDict pomaga zbudować schema dla modelu, ale runtime'owo parsed jest zwykłym dict.
    """
    errors: list[str] = []

    root = require_mapping(parsed, "root", errors)
    require_keys(root, "root", ["Misje_PL", "Dialogi_PL"], errors)

    misje = require_mapping(root.get("Misje_PL") if root else None, "Misje_PL", errors)
    require_keys(
        misje,
        "Misje_PL",
        ["Podsumowanie_PL", "Cele_PL", "Treść_PL", "Postęp_PL", "Zakończenie_PL", "Nagrody_PL"],
        errors
    )

    podsumowanie = require_mapping(misje.get("Podsumowanie_PL") if misje else None, "Misje_PL.Podsumowanie_PL", errors)
    require_keys(podsumowanie, "Misje_PL.Podsumowanie_PL", ["Tytuł"], errors)

    cele = require_mapping(misje.get("Cele_PL") if misje else None, "Misje_PL.Cele_PL", errors)
    require_keys(cele, "Misje_PL.Cele_PL", ["Główny", "Podrzędny"], errors)

    dialogi = require_mapping(root.get("Dialogi_PL") if root else None, "Dialogi_PL", errors)
    require_keys(dialogi, "Dialogi_PL", ["Gossipy_Dymki_PL"], errors)

    bloki = require_list(dialogi.get("Gossipy_Dymki_PL") if dialogi else None, "Dialogi_PL.Gossipy_Dymki_PL", errors)
    if bloki is not None:
        for idx, blok_raw in enumerate(bloki):
            blok_path = f"Dialogi_PL.Gossipy_Dymki_PL[{idx}]"
            blok = require_mapping(blok_raw, blok_path, errors)
            require_keys(blok, blok_path, ["id", "typ", "npc_pl", "wypowiedzi_PL"], errors)

            if blok is not None and "typ" in blok and blok["typ"] not in ("gossip", "dymek"):
                errors.append(f"{blok_path}.typ (nieprawidłowa wartość: {blok['typ']!r})")

    if errors:
        missing = "; ".join(errors)
        raise ValueError(f"---[ID: {misja_id}] Niepełna lub błędna struktura etapu {stage}: {missing}")


def handle_quest_stage_result(
    result,
    raw_response,
    llm,
    misja_id,
    stage,
    started_at,
    wsad_json,
    silnik,
    status_zapisu
):
    if result["parsing_error"] is not None:
        raise result["parsing_error"]

    parsed = result["parsed"]
    if parsed is None:
        raise ValueError(f"---[ID: {misja_id}] Brak sparsowanego wyniku etapu: {stage}.")

    if hasattr(parsed, "model_dump"):
        parsed_dict = parsed.model_dump(mode="python")
        parsed_json = parsed.model_dump_json(indent=2)
    else:
        parsed_dict = parsed
        parsed_json = json.dumps(parsed_dict, indent=2, ensure_ascii=False)

    validate_quest_content_response(parsed_dict, misja_id=misja_id, stage=stage)
    dms = round((time.perf_counter() - started_at) * 1000) if started_at is not None else None

    logs = create_logs(
        raw_response=raw_response,
        llm=llm,
        misja_id_moje_fk=misja_id,
        stage=stage,
        duration_ms=dms,
        input_chars=len(wsad_json),
        output_chars=len(parsed_json)
    )

    #save_quests_dialogues_to_db(silnik, misja_id, parsed_dict, status_zapisu)
    save_ai_logs_to_db(silnik=silnik, logs=logs)

    return parsed_json, logs

def przetworz_pojedyncza_misje(
    wiersz,
    silnik,
):
    """
    Ta funkcja wykonuje całą pracę dla jednej misji.
    Będzie uruchamiana równolegle w wielu wątkach.
    """
    misja_id = wiersz["MISJA_ID_MOJE_PK"]
    zakodowane_dane = wiersz["HTML_SKOMPRESOWANY"]

    with silnik.connect() as conn:
        try:
            q_select_npc = text("""
            WITH wszystkie_idki AS (
                SELECT tabela_wartosci.ID_NPC
                FROM dbo.MISJE AS m
                CROSS APPLY (VALUES (m.NPC_START_ID), (m.NPC_KONIEC_ID)) AS tabela_wartosci (ID_NPC)
                WHERE m.MISJA_ID_MOJE_PK = :misja_id

                UNION

                SELECT ds.NPC_ID_FK
                FROM dbo.DIALOGI_STATUSY AS ds
                WHERE ds.MISJA_ID_MOJE_FK = :misja_id
            ),
            oczyszczone_dane AS (
                SELECT wi.ID_NPC, ns.STATUS,
                CASE WHEN CHARINDEX('[', ns.NAZWA) > 0 THEN RTRIM(LEFT(ns.NAZWA, CHARINDEX('[', ns.NAZWA) - 1)) ELSE ns.NAZWA END AS CZYSTA_NAZWA
                FROM wszystkie_idki AS wi
                INNER JOIN dbo.NPC_STATUSY AS ns ON wi.ID_NPC = ns.NPC_ID_FK
            )
                SELECT DISTINCT
                    pvt.[0_ORYGINAŁ],
                    pvt.[3_ZATWIERDZONO],
                    n.PLEC,
                    n.RASA
                FROM oczyszczone_dane
                PIVOT (MAX(CZYSTA_NAZWA) FOR STATUS IN ([0_ORYGINAŁ], [3_ZATWIERDZONO])) AS pvt
                LEFT JOIN dbo.NPC AS n
                  ON n.NPC_ID_MOJE_PK = pvt.ID_NPC;
            """)

            q_select_sk = text("""
                SELECT sk.SLOWO_EN, sk.SLOWO_PL
                FROM dbo.MISJE_SLOWA_KLUCZOWE AS msk
                INNER JOIN dbo.SLOWA_KLUCZOWE AS sk
                   ON msk.SLOWO_ID = sk.SLOWO_ID_PK
                WHERE msk.MISJA_ID_MOJE_FK = :misja_id
            """)

            npc_z_bazy = conn.execute(q_select_npc, {"misja_id": misja_id}).all()
            slowa_kluczowe_z_bazy = conn.execute(q_select_sk, {"misja_id": misja_id}).all()

            if not zakodowane_dane:
                print(f"SKIP [ID: {misja_id}] - Brak danych.")
                return

            skompresowane_bajty = base64.b64decode(zakodowane_dane)
            tekst_html = zlib.decompress(skompresowane_bajty).decode("utf-8")
            surowe_dane = parsuj_misje_z_url(url=None, html_content=tekst_html)
            przetworzone_dane = przefiltruj_dane_misji(dane_wejsciowe=surowe_dane, jezyk="EN")

            wsad_npc = set(n for n in npc_z_bazy)
            wsad_sk = set(s for s in slowa_kluczowe_z_bazy)
            wsad_json = json.dumps(przetworzone_dane, indent=4, ensure_ascii=False)

            txt_npc = "\n".join([f"- {n[0]} -> {n[1]} | PLEC={n[2]} | RASA={n[3]}" for n in wsad_npc if n[0] and n[1]])
            txt_sk = "\n".join([f"- {k[0]} -> {k[1]}" for k in wsad_sk if k[0] and k[1]])

            _translator = llm_translator()
            _editor = llm_editor()

            result_translator = None
            result_editor = None
            raw_response = None
            started_at = None
            current_stage = None
            current_llm = None
            
            print(f"--- [ID: {misja_id}] Start Tlumaczenia... ---")
            try:

# =========================================================================================
# ======================================= TRANSLATOR ======================================
# =========================================================================================

                current_stage = "translator"
                current_llm = _translator
                raw_response = None
                started_at = time.perf_counter()
                result_translator = translator(
                    llm=_translator,
                    tekst_oryginalny=wsad_json,
                    tekst_niemiecki="",
                    tekst_npc=txt_npc,
                    tekst_slowa_kluczowe=txt_sk
                )
                raw_response = result_translator["raw"]
                translated_json, logs = handle_quest_stage_result(
                    result=result_translator,
                    raw_response=raw_response,
                    llm=_translator,
                    misja_id=misja_id,
                    stage="translator",
                    started_at=started_at,
                    wsad_json=wsad_json,
                    silnik=silnik,
                    status_zapisu="1_PRZETŁUMACZONO"
                )
                print(translated_json)
                logs_json = json.dumps(logs, indent=2, ensure_ascii=False)
                print(logs_json)

# ============================================================================================
# ========================================== EDITOR ==========================================
# ============================================================================================

                current_stage = "editor"
                current_llm = _editor
                raw_response = None
                started_at = time.perf_counter()
                result_editor = editor(
                    llm=_editor,
                    tekst_oryginalny=wsad_json,
                    tekst_przetlumaczony=translated_json,
                    tekst_pomocniczy="",
                    tekst_npc=txt_npc,
                    tekst_slowa_kluczowe=txt_sk,
                    tekst_wytyczne_rasy="",
                    tekst_rasy_przyklady="",
                    tekst_klasy_przyklady=""
                )
                raw_response = result_editor["raw"]
                edited_json, logs = handle_quest_stage_result(
                    result=result_editor,
                    raw_response=raw_response,
                    llm=_editor,
                    misja_id=misja_id,
                    stage="editor",
                    started_at=started_at,
                    wsad_json=wsad_json,
                    silnik=silnik,
                    status_zapisu="2_ZREDAGOWANO"
                )
                print(edited_json)
                logs_json = json.dumps(logs, indent=2, ensure_ascii=False)
                print(logs_json)

# ========================================================================================
# ====================================== EXCEPTIONS ======================================
# ========================================================================================

            except Exception as e:
                if raw_response is not None and current_llm is not None and current_stage is not None:
                    dms = round((time.perf_counter() - started_at) * 1000) if started_at is not None else None
                    err = str(e)
                    parsing_error = err[:997] + "..." if len(err) > 1000 else err

                    logs = create_logs(
                        raw_response=raw_response,
                        llm=current_llm,
                        misja_id_moje_fk=misja_id,
                        stage=current_stage,
                        duration_ms=dms,
                        parsing_error=parsing_error,
                        input_chars=len(wsad_json),
                        output_chars=0
                    )
                    print(json.dumps(logs, indent=2, ensure_ascii=False))
                    save_ai_logs_to_db(silnik=silnik, logs=logs)
                else:
                    print(f"---BŁĄD ETAPU {current_stage or 'unknown'}: {e}")

            print(f"+++[ID: {misja_id}] GOTOWE (Tlumaczenie) +++")

        except Exception as e:
            print(f"!!! BLAD przy misji {misja_id}: {e}")


def misje_dialogi_przetlumacz_zredaguj_zapisz(
    silnik, 
    kraina: str | None = None, 
    fabula: str | None = None, 
    dodatek: str | None = None,
    id_misji: int | None = None, 
    liczba_watkow: int = 4
):
    warunki_sql = sklej_warunki_w_WHERE(kraina, fabula, dodatek, id_misji)

    q_select_tresc = text(f"""
    WITH hashe AS (
        SELECT m.MISJA_ID_MOJE_PK, z.HTML_SKOMPRESOWANY,
            ROW_NUMBER() OVER (PARTITION BY z.MISJA_ID_MOJE_FK ORDER BY z.DATA_WYSCRAPOWANIA DESC) AS r
        FROM dbo.ZRODLO AS z
        INNER JOIN dbo.MISJE AS m 
          ON z.MISJA_ID_MOJE_FK = m.MISJA_ID_MOJE_PK
        WHERE 1=1 
          AND m.MISJA_ID_Z_GRY IS NOT NULL 
          AND m.MISJA_ID_Z_GRY <> 123456789
          AND (
            m.WSKAZNIK_ZGODNOSCI <= 0.70000
            OR m.WSKAZNIK_ZGODNOSCI IS NULL
            )
                          
        {warunki_sql}
        
          AND NOT EXISTS (
                            SELECT 1 
                            FROM dbo.MISJE_STATUSY AS ms 
                            WHERE 1=1
                              AND ms.MISJA_ID_MOJE_FK = m.MISJA_ID_MOJE_PK 
                              AND ms.STATUS = N'1_PRZETŁUMACZONO'
                          )
          AND EXISTS (
                            SELECT 1
                            FROM dbo.MISJE_STATUSY AS ms
                            WHERE 1=1
                              AND ms.MISJA_ID_MOJE_FK = m.MISJA_ID_MOJE_PK
                              AND ms.STATUS = N'0_ORYGINAŁ'
                     )
    )
    SELECT MISJA_ID_MOJE_PK, HTML_SKOMPRESOWANY 
    FROM hashe 
    WHERE r=1 
    ORDER BY MISJA_ID_MOJE_PK;
    """)

    parametry = {
        "kraina_en": kraina, 
        "fabula_en": fabula, 
        "dodatek_en": dodatek,
        "id_misji": id_misji
    }

    print("Pobieranie listy misji do przetworzenia...")
    with silnik.connect() as conn:
        lista_zadan = conn.execute(q_select_tresc, parametry).mappings().all()

    liczba_zadan = len(lista_zadan)
    
    if liczba_zadan == 0:
        print("Nie znaleziono żadnych misji pasujących do kryteriów.")
        return

    print(f"Znaleziono {liczba_zadan} misji do przetworzenia. Uruchamiam {liczba_watkow} wątków...")
    print("Dostawca tłumaczenia: langchain/openai")
    print("Redagowanie: pominięte")

    with concurrent.futures.ThreadPoolExecutor(max_workers=liczba_watkow) as executor:
        futures = []
        for wiersz in lista_zadan:
            future = executor.submit(
                przetworz_pojedyncza_misje,
                wiersz,
                silnik,
            )
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            pass

    print("\n--- ZAKOŃCZONO PRZETWARZANIE WIELOWĄTKOWE ---")


def tych_npcow_nie_tlumacz(silnik, klient):
    sciezka = sciezka_excel_mappingi("npc.xlsx")
    sciezka_zapis = sciezka_excel_mappingi("surowe", "npc_nie_do_tlumaczenia")
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

        print(f"   -> Wysyłam zapytanie do API (rozmiar JSON: {len(dfj)} znaków)...")
        start_czas = time.time()
        
        odpowiedz = klient.models.generate_content(
            model=MODEL_GEMINI_POMOCNICZY,
            contents=dfj,
            config={
                "system_instruction": instrukcja_tych_npc_nie(), 
                "response_mime_type": "application/json",
                "thinking_config": pobierz_thinking_config_gemini_high(),
            }
        )
        zaloguj_uzycie_gemini(odpowiedz, "npc_nie_tlumacz")
        
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
    sciezka = sciezka_excel_mappingi("npc.xlsx")
    sciezka_zapis = sciezka_excel_mappingi("surowe", "propozycja_tlumaczen_npc")
    zakladka = "surowe"

    print(f"--- START TŁUMACZENIA ---")
    print(f"1. Wczytuję dane z pliku: {sciezka}")
    df = pd.read_excel(sciezka, sheet_name=zakladka, usecols=["NPC_ID_MOJE_PK", "NAZWA", "NAZWA_PL_FINAL"], index_col="NPC_ID_MOJE_PK")
    
    df = df.loc[df["NAZWA_PL_FINAL"].isna()]
    liczba_wierszy = len(df)
    
    dzisiejsza_data = datetime.now().strftime("%d_%m_%Y")
    print(f"2. Data dla plików: {dzisiejsza_data}")
    print(f"3. Liczba NPC do przetłumaczenia: {liczba_wierszy}")

    co_ile = 200
    licznik_batchy = 1
    total_batchy = (liczba_wierszy + co_ile - 1) // co_ile

    for i in range(0, liczba_wierszy, co_ile):
        start = i
        koniec = i + co_ile
        
        print(f"\n[Batch {licznik_batchy}/{total_batchy}] Tłumaczenie wierszy od {start} do {koniec}...")
        
        batch_seria = df["NAZWA"].iloc[start:koniec]
        dfj = batch_seria.to_json(force_ascii=False)

        print(f"   -> Wysyłam dane do API (rozmiar JSON: {len(dfj)} znaków)...")
        start_czas = time.time()
        
        try:
            odpowiedz = klient.models.generate_content(
                model=MODEL_GEMINI_POZOSTALE,
                contents=dfj,
                config={
                    "system_instruction": instrukcja_tlumacz_npc(), 
                    "response_mime_type": "application/json",
                    "tools": [{"google_search": {}}],
                    "thinking_config": pobierz_thinking_config_gemini_high(),
                }
            )
            zaloguj_uzycie_gemini(odpowiedz, "npc_tlumacz")
            
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


def przetworz_batch_metadanych_npc(
    klient,
    model: str,
    config,
    dane_npc_json: str,
    batch_nr: int,
    liczba_batchy: int,
    liczba_rekordow_batcha: int,
    folder_zapisu: str,
    run_id: str,
):
    print(f"[Batch {batch_nr}/{liczba_batchy}] Wysylam {liczba_rekordow_batcha} NPC...")

    try:
        odpowiedz = klient.models.generate_content(
            model=model,
            contents=instrukcja_dane_npc_zmienna(dane_npc_json),
            config=config,
        )
        zaloguj_uzycie_gemini(odpowiedz, "npc_metadane")

        df_batch = pd.DataFrame(json.loads(odpowiedz.text)["records"])
        nazwa_pliku = f"{run_id}_batch_{batch_nr:03d}.csv"
        pelna_sciezka = os.path.join(folder_zapisu, nazwa_pliku)
        df_batch.to_csv(pelna_sciezka, index=False, encoding="utf-8-sig", sep=";")

        print(f"[Batch {batch_nr}/{liczba_batchy}] Zapisano: {nazwa_pliku}")
        return len(df_batch), True
    except Exception as e:
        print(f"[Batch {batch_nr}/{liczba_batchy}] BLAD: {e}")
        return 0, False


def pobierz_metadane_npc_do_csv(
    silnik,
    batch_size=50,
    liczba_watkow=4,
    folder_zapisu=sciezka_excel_mappingi("surowe", "npc_metadane"),
):

    klient=zaladuj_api_i_klienta("API_TLUMACZENIE")
    model=MODEL_GEMINI_POMOCNICZY

    run_id = datetime.now().strftime("%Y_%m_%d_%H%M%S_%f")

    q_select_npc = text("""
        SELECT
            NPC_ID_MOJE_PK,
            NAZWA
        FROM dbo.NPC
        WHERE 1=1
          AND PLEC IS NULL
          AND RASA IS NULL
          AND KLASA IS NULL
          AND TYTUL IS NULL
          AND NAZWA NOT IN ('Brak Danych', '...', 'Automatic')
          --AND RASA IN ('Unknown')
        ORDER BY NPC_ID_MOJE_PK
    """)
    # ('Brak Danych', '...', 'Automatic')
    df_wejscie = pd.read_sql_query(sql=q_select_npc, con=silnik)
    liczba_rekordow = len(df_wejscie)

    if liczba_rekordow == 0:
        print("Brak NPC do przetworzenia.")
        return

    config = genai_types.GenerateContentConfig(
        system_instruction=instrukcja_dane_npc_stala(),
        response_mime_type="application/json",
        response_schema=SCHEMAT_ODPOWIEDZI_DANE_NPC,
        tools=[
            genai_types.Tool(
                google_search=genai_types.GoogleSearch()
            )
        ],
        thinking_config=pobierz_thinking_config_gemini_high(),
        temperature=0.1,
    )

    liczba_przetworzonych = 0
    liczba_udanych_batchy = 0
    liczba_batchy = (liczba_rekordow + batch_size - 1) // batch_size

    print(
        f"Start przetwarzania NPC: {liczba_rekordow} rekordow, "
        f"batch size = {batch_size}, liczba batchy = {liczba_batchy}, watki = {liczba_watkow}"
    )

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=liczba_watkow) as executor:
        for batch_nr, start in enumerate(range(0, liczba_rekordow, batch_size), start=1):
            stop = start + batch_size
            batch = df_wejscie.iloc[start:stop]
            dane_npc_json = json.dumps(
                {
                    "records": batch[["NPC_ID_MOJE_PK", "NAZWA"]]
                    .rename(columns={"NPC_ID_MOJE_PK": "NPC_ID", "NAZWA": "NPC_NAZWA"})
                    .to_dict(orient="records")
                },
                ensure_ascii=False,
            )

            future = executor.submit(
                przetworz_batch_metadanych_npc,
                klient,
                model,
                config,
                dane_npc_json,
                batch_nr,
                liczba_batchy,
                len(batch),
                folder_zapisu,
                run_id,
            )
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            liczba_rekordow_batcha, czy_udany = future.result()
            liczba_przetworzonych += liczba_rekordow_batcha
            if czy_udany:
                liczba_udanych_batchy += 1
            print(
                f"Postep: batchy {liczba_udanych_batchy}/{liczba_batchy}, "
                f"NPC {liczba_przetworzonych}/{liczba_rekordow}"
            )

    print(f"Koniec. Udane batche: {liczba_udanych_batchy}/{liczba_batchy}. Przetworzono: {liczba_przetworzonych}/{liczba_rekordow} NPC.")
