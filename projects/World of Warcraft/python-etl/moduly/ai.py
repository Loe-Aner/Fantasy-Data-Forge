import json
import os
import time
import concurrent.futures
from datetime import datetime

from dotenv import load_dotenv
from google import genai
from google.genai import types as genai_types

from sqlalchemy import text, bindparam
import pandas as pd

from moduly.services_persist_wynik import przefiltruj_dane_misji, zapisz_misje_dialogi_ai_do_db
from moduly.ai_prompty import (
    dodatek_cache_lokalizacja,
    instrukcja_redaktor_stala,
    instrukcja_redaktor_zmienna,
    instrukcja_slowa_kluczowe,
    instrukcja_tlumacz_npc,
    instrukcja_tlumacz_stala,
    instrukcja_tlumacz_zmienna,
    instrukcja_tych_npc_nie,
    instrukcja_dane_npc_stala,
    instrukcja_dane_npc_zmienna
)
from moduly.sciezki import sciezka_excel_mappingi
from scraper_wiki_main import parsuj_misje_z_url
from moduly.utils import sklej_warunki_w_WHERE
import zlib
import base64

MODEL_GEMINI_GLOWNY = "gemini-3.1-pro-preview" # gemini-3.1-pro-preview -- gemini-3-flash-preview
MODEL_GEMINI_POMOCNICZY = "gemini-3.1-flash-lite-preview"
TTL_CACHE_GEMINI = "10800s"
MIN_CACHE_TOKENS_GEMINI = 4096
CACHE_DEBUG = False
SCHEMAT_ODPOWIEDZI_DANE_NPC = {
    "type": "OBJECT",
    "required": ["records"],
    "properties": {
        "records": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "required": [
                    "NPC_ID",
                    "NPC_NAZWA",
                    "PLEC",
                    "RASA",
                    "KLASA",
                    "TYTUL",
                    "_PEWNOSC",
                    "_ZRODLO",
                    "_NOTATKI",
                ],
                "properties": {
                    "NPC_ID": {"type": "INTEGER"},
                    "NPC_NAZWA": {"type": "STRING"},
                    "PLEC": {"type": "STRING"},
                    "RASA": {"type": "STRING", "nullable": True},
                    "KLASA": {"type": "STRING"},
                    "TYTUL": {"type": "STRING", "nullable": True},
                    "_PEWNOSC": {"type": "STRING"},
                    "_ZRODLO": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                    },
                    "_NOTATKI": {"type": "STRING", "nullable": True},
                },
            },
        },
    },
}


def log_cache_debug(tekst: str):
    if CACHE_DEBUG:
        print(tekst)


def zaladuj_api_i_klienta(
        nazwa_api: str,
        dostawca: str = "gemini"
    ):
    load_dotenv()
    API_KEY = os.environ.get(nazwa_api)

    if not API_KEY:
        raise ValueError("BRAK KLUCZA!")
    
    print("KLUCZ ZWARTY I GOTOWY!")
    
    if dostawca == "gemini":
        return genai.Client(api_key=API_KEY)
    else:
        raise ValueError(f"Nieznany dostawca: {dostawca}")


def pobierz_prompt_staly(etap: str) -> str:
    if etap == "tlumacz":
        return instrukcja_tlumacz_stala()
    if etap == "redaktor":
        return instrukcja_redaktor_stala()
    raise ValueError(f"Nieznany etap promptu: {etap}")


def zbuduj_wiadomosc_tlumaczenia(txt_npc: str, txt_sk: str, wsad_json: str) -> str:
    return "\n\n".join(
        [
            instrukcja_tlumacz_zmienna(txt_npc, txt_sk),
            "JSON MISJI DO PRZETLUMACZENIA:",
            wsad_json,
        ]
    )


def zbuduj_wiadomosc_redakcji(
    tekst_oryginalny: str,
    txt_npc: str,
    txt_sk: str,
    wsad_redakcja: str,
) -> str:
    return "\n\n".join(
        [
            instrukcja_redaktor_zmienna(tekst_oryginalny, txt_npc, txt_sk),
            "WERSJA ROBOCZA JSON DO REDAKCJI:",
            wsad_redakcja,
        ]
    )


def policz_tokeny_promptu_gemini(klient, prompt_staly: str) -> int:
    try:
        wynik = klient.models.count_tokens(
            model=MODEL_GEMINI_GLOWNY,
            contents=".",
            config=genai_types.CountTokensConfig(
                system_instruction=prompt_staly,
            ),
        )
        return wynik.total_tokens
    except Exception as e:
        log_cache_debug(
            "[CACHE][Gemini] CountTokens dla system_instruction nie jest wspierany w tym API. "
            f"Wracam do przyblizenia przez contents. Blad: {e}"
        )
        wynik = klient.models.count_tokens(
            model=MODEL_GEMINI_GLOWNY,
            contents=prompt_staly,
        )
        return wynik.total_tokens


def wygeneruj_json_gemini(klient, konfiguracja: dict, contents: str, etap: str, misja_id: int | None = None):
    config = {"response_mime_type": "application/json"}
    cached_content = konfiguracja.get("cached_content")

    if cached_content:
        config["cached_content"] = cached_content
        try:
            odpowiedz = klient.models.generate_content(
                model=konfiguracja["model"],
                contents=contents,
                config=config,
            )
            zaloguj_uzycie_cache_gemini(odpowiedz, etap, misja_id=misja_id, konfiguracja_cache=True)
            return odpowiedz
        except Exception as e:
            prefix = f"[ID: {misja_id}] " if misja_id is not None else ""
            print(
                f"[CACHE][Gemini][{etap}] {prefix}Blad podczas uzycia cached_content. "
                f"Powtarzam bez cache. Blad: {e}"
            )

    config.pop("cached_content", None)
    config["system_instruction"] = konfiguracja["prompt_staly"]
    odpowiedz = klient.models.generate_content(
        model=konfiguracja["model"],
        contents=contents,
        config=config,
    )
    zaloguj_uzycie_cache_gemini(odpowiedz, etap, misja_id=misja_id, konfiguracja_cache=False)
    return odpowiedz


def przygotuj_prompt_staly_dla_gemini(klient, etap: str) -> tuple[str, int, bool]:
    prompt_staly = pobierz_prompt_staly(etap)

    try:
        tokeny = policz_tokeny_promptu_gemini(klient, prompt_staly)
    except Exception as e:
        print(f"[CACHE][Gemini][{etap}] Nie udalo sie policzyc tokenow promptu stalego: {e}")
        prompt_staly = f"{prompt_staly}\n\n{dodatek_cache_lokalizacja()}"
        return prompt_staly, -1, True

    if tokeny >= MIN_CACHE_TOKENS_GEMINI:
        return prompt_staly, tokeny, False

    prompt_rozszerzony = f"{prompt_staly}\n\n{dodatek_cache_lokalizacja()}"
    tokeny_rozszerzone = policz_tokeny_promptu_gemini(klient, prompt_rozszerzony)
    return prompt_rozszerzony, tokeny_rozszerzone, True


def potwierdz_cache_gemini(cache, etap: str):
    usage = getattr(cache, "usage_metadata", None)
    cached_tokens = getattr(usage, "total_token_count", 0) if usage else 0
    print(
        f"[CACHE][Gemini][{etap}] cached_content={cache.name}, "
        f"cached_tokens={cached_tokens}, expire_time={getattr(cache, 'expire_time', None)}"
    )


def zaloguj_uzycie_cache_gemini(
    odpowiedz,
    etap: str,
    misja_id: int | None = None,
    konfiguracja_cache: bool = False,
):
    usage = getattr(odpowiedz, "usage_metadata", None)
    if not usage:
        return

    prefix = f"[ID: {misja_id}] " if misja_id is not None else ""
    prompt_tokens = getattr(usage, "prompt_token_count", 0)
    cached_tokens = getattr(usage, "cached_content_token_count", 0)
    output_tokens = getattr(usage, "candidates_token_count", 0)
    print(
        f"[CACHE][Gemini][{etap}] {prefix}konfiguracja_cache={'TAK' if konfiguracja_cache else 'NIE'}, "
        f"prompt_tokens={prompt_tokens}, cached_tokens={cached_tokens}, output_tokens={output_tokens}"
    )

def przygotuj_konfiguracje_promptu(klient, dostawca: str, etap: str) -> dict:
    if dostawca == "gemini":
        prompt_staly, tokeny_systemu, czy_dodano_aneks = przygotuj_prompt_staly_dla_gemini(klient, etap)
        print(
            f"[CACHE][Gemini][{etap}] tokeny_prefiksu={tokeny_systemu}, "
            f"dodano_aneks={'TAK' if czy_dodano_aneks else 'NIE'}"
        )
        try:
            cache = klient.caches.create(
                model=MODEL_GEMINI_GLOWNY,
                config=genai_types.CreateCachedContentConfig(
                    display_name=f"wow-{etap}-{int(time.time())}",
                    system_instruction=prompt_staly,
                    ttl=TTL_CACHE_GEMINI,
                ),
            )
            potwierdz_cache_gemini(cache, etap)
            cached_content = cache.name
        except Exception as e:
            print(f"[CACHE][Gemini][{etap}] Nie udalo sie utworzyc cache. Fallback bez cache. Blad: {e}")
            cached_content = None

        return {
            "dostawca": dostawca,
            "etap": etap,
            "model": MODEL_GEMINI_GLOWNY,
            "prompt_staly": prompt_staly,
            "cached_content": cached_content,
        }

    raise ValueError(f"Nieobslugiwany dostawca: {dostawca}")


def wyczysc_cache_gemini(klient, nazwa_cache: str, etap: str):
    try:
        klient.caches.delete(name=nazwa_cache)
        print(f"[CACHE][Gemini][{etap}] Usunieto cache: {nazwa_cache}")
    except Exception as e:
        print(f"[CACHE][Gemini][{etap}] Nie udalo sie usunac cache {nazwa_cache}: {e}")


def wyczysc_cache_gemini_dla_konfiguracji(klient, konfiguracja: dict | None):
    if not konfiguracja or konfiguracja.get("dostawca") != "gemini":
        return

    nazwa_cache = konfiguracja.get("cached_content")
    if not nazwa_cache:
        return

    wyczysc_cache_gemini(klient, nazwa_cache, konfiguracja.get("etap", "nieznany"))

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
                    model=MODEL_GEMINI_GLOWNY,
                    contents=json.dumps(wsad_dla_geminisia),
                    config={
                        "system_instruction": instrukcja_slowa_kluczowe(),
                        "response_mime_type": "application/json",
                        "tools": [{"google_search": {}}] 
                    }
                )
        
        wynik_lista = json.loads(odpowiedz.text)

        df = pd.DataFrame(wynik_lista)
        df_rozbite = df.explode("extracted")
        df_rozbite = df_rozbite.dropna(subset=["extracted"])

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

def przetworz_pojedyncza_misje(
    wiersz,
    silnik,
    klient_tlumacz,
    klient_redaktor,
    konfiguracja_tlumaczenie,
    konfiguracja_redakcja,
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
            surowe_dane = parsuj_misje_z_url(None, html_content=tekst_html)
            przetworzone_dane = przefiltruj_dane_misji(surowe_dane, jezyk="EN")

            wsad_npc = set(n for n in npc_z_bazy)
            wsad_sk = set(s for s in slowa_kluczowe_z_bazy)
            wsad_json = json.dumps(przetworzone_dane, indent=4, ensure_ascii=False)

            txt_npc = "\n".join([f"- {n[0]} -> {n[1]} | PLEC={n[2]} | RASA={n[3]}" for n in wsad_npc if n[0] and n[1]])
            txt_sk = "\n".join([f"- {k[0]} -> {k[1]}" for k in wsad_sk if k[0] and k[1]])

            print(f"--- [ID: {misja_id}] Start Tlumaczenia... ---")
            wiadomosc_tlumaczenia = zbuduj_wiadomosc_tlumaczenia(txt_npc, txt_sk, wsad_json)

            if konfiguracja_tlumaczenie["dostawca"] == "gemini":
                odp_tlumacz = wygeneruj_json_gemini(
                    klient_tlumacz,
                    konfiguracja_tlumaczenie,
                    wiadomosc_tlumaczenia,
                    "tlumacz",
                    misja_id=misja_id,
                )
                przetlumaczone = json.loads(odp_tlumacz.text)
            else:
                raise ValueError(
                    f"Nieobsługiwany dostawca tłumaczenia: {konfiguracja_tlumaczenie['dostawca']}"
                )

            zapisz_misje_dialogi_ai_do_db(silnik, misja_id, przetlumaczone, "1_PRZETŁUMACZONO")

            print(f"--- [ID: {misja_id}] Start Redakcji... ---")
            wsad_redakcja = json.dumps(przetlumaczone, indent=4, ensure_ascii=False)
            wiadomosc_redakcji = zbuduj_wiadomosc_redakcji(wsad_json, txt_npc, txt_sk, wsad_redakcja)

            if konfiguracja_redakcja["dostawca"] == "gemini":
                odp_redaktor = wygeneruj_json_gemini(
                    klient_redaktor,
                    konfiguracja_redakcja,
                    wiadomosc_redakcji,
                    "redaktor",
                    misja_id=misja_id,
                )
                zredagowane = json.loads(odp_redaktor.text)
            else:
                raise ValueError(
                    f"Nieobsługiwany dostawca redakcji: {konfiguracja_redakcja['dostawca']}"
                )

            zapisz_misje_dialogi_ai_do_db(silnik, misja_id, zredagowane, "2_ZREDAGOWANO")

            print(f"+++ [ID: {misja_id}] GOTOWE (Tlumaczenie + Redakcja) +++")

        except Exception as e:
            print(f"!!! BLAD przy misji {misja_id}: {e}")


def misje_dialogi_przetlumacz_zredaguj_zapisz(
    silnik, 
    kraina: str | None = None, 
    fabula: str | None = None, 
    dodatek: str | None = None,
    id_misji: int | None = None, 
    liczba_watkow: int = 4,
    dostawca_redakcja: str = "gemini",
    dostawca_tlumaczenie: str = "gemini"
):
    dostawca_tlumaczenie = dostawca_tlumaczenie.lower().strip()
    dostawca_redakcja = dostawca_redakcja.lower().strip()

    if dostawca_tlumaczenie != "gemini":
        raise ValueError("Parametr 'dostawca_tlumaczenie' musi mieć wartość: 'gemini'.")
    if dostawca_redakcja != "gemini":
        raise ValueError("Parametr 'dostawca_redakcja' musi mieć wartość: 'gemini'.")

    klient_tlumacz = zaladuj_api_i_klienta("API_TLUMACZENIE", dostawca=dostawca_tlumaczenie)
    if dostawca_redakcja == dostawca_tlumaczenie:
        klient_redaktor = klient_tlumacz
    else:
        klient_redaktor = zaladuj_api_i_klienta("API_REDAGOWANIE", dostawca=dostawca_redakcja)
    
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
    print(f"Dostawca tłumaczenia: {dostawca_tlumaczenie}")
    print(f"Dostawca redakcji: {dostawca_redakcja}")

    konfiguracja_tlumaczenie = None
    konfiguracja_redakcja = None

    try:
        konfiguracja_tlumaczenie = przygotuj_konfiguracje_promptu(
            klient_tlumacz,
            dostawca_tlumaczenie,
            "tlumacz",
        )
        konfiguracja_redakcja = przygotuj_konfiguracje_promptu(
            klient_redaktor,
            dostawca_redakcja,
            "redaktor",
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=liczba_watkow) as executor:
            futures = []
            for wiersz in lista_zadan:
                future = executor.submit(
                    przetworz_pojedyncza_misje,
                    wiersz,
                    silnik,
                    klient_tlumacz,
                    klient_redaktor,
                    konfiguracja_tlumaczenie,
                    konfiguracja_redakcja
                )
                futures.append(future)

            for future in concurrent.futures.as_completed(futures):
                pass
    finally:
        wyczysc_cache_gemini_dla_konfiguracji(klient_tlumacz, konfiguracja_tlumaczenie)
        wyczysc_cache_gemini_dla_konfiguracji(klient_redaktor, konfiguracja_redakcja)

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
                model=MODEL_GEMINI_GLOWNY,
                contents=dfj,
                config={
                    "system_instruction": instrukcja_tlumacz_npc(), 
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
        thinking_config=genai_types.ThinkingConfig(
            thinking_level=genai_types.ThinkingLevel.HIGH
        ),
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
