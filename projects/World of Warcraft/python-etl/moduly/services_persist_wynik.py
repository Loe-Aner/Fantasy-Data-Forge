from sqlalchemy import text
import re

from moduly.repo_NPC import (
    zapisz_npc_i_status_do_db as repo_zapisz_npc_i_status_do_db,
    zapewnij_npc_i_pobierz_id
)
from moduly.repo_misje import (
    zapewnij_misje_i_pobierz_id,
    dodaj_statusy_misji_batch
)
from moduly.repo_dialogi import dodaj_statusy_dialogu_batch

def normalizuj_nazwe_npc(npc: str | None) -> str:
    if npc is None:
        return ""

    npc = str(npc)
    npc = re.sub(r"\[.*?\]|\(.*?\)", "", npc)
    npc = re.sub(r"\s+", " ", npc).strip()
    return npc

def zapisz_npc_i_status_do_db(
        silnik,
        tabela_npc: str,
        tabela_npc_statusy: str,
        szukaj_wg: list[str],
        wyscrapowana_tresc: dict,
        zrodlo: str,
        status: str = "0_ORYGINAŁ",
        jezyk: str = "EN"
    ):
    podsumowanie = wyscrapowana_tresc[f"Misje_{jezyk}"][f"Podsumowanie_{jezyk}"]

    for klucz in szukaj_wg:
        npc = podsumowanie.get(klucz)
        npc = normalizuj_nazwe_npc(npc)

        if npc == "":
            npc = "Brak Nazwy"

        repo_zapisz_npc_i_status_do_db(
            silnik=silnik,
            tabela_npc=tabela_npc,
            tabela_npc_statusy=tabela_npc_statusy,
            nazwa=npc,
            zrodlo=zrodlo,
            status=status
        )

def zapisz_misje_i_statusy_do_db(
        silnik,
        wynik: dict,
        tabela_npc: str,
        tabela_misje: str,
        tabela_misje_statusy: str,
        status: str = "0_ORYGINAŁ",
        jezyk: str = "EN",
        misja_id_pl = None
    ) -> int:

    mapa_segment = {
        f"Cele_{jezyk}": "CEL",
        f"Treść_{jezyk}": "TREŚĆ",
        f"Postęp_{jezyk}": "POSTĘP",
        f"Zakończenie_{jezyk}": "ZAKOŃCZENIE",
        f"Nagrody_{jezyk}": "NAGRODY"
    }

    mapa_podsegment = {
        "Główny": "GŁÓWNY_CEL",
        "Podrzędny": "PODRZĘDNY_CEL"
    }

    misje_en = wynik.get(f"Misje_{jezyk}", {})
    podsumowanie = misje_en.get(f"Podsumowanie_{jezyk}", {})

    url = wynik.get("Źródło", {}).get("url")
    tytul = (podsumowanie.get("Tytuł") or "").strip()

    npc_start = normalizuj_nazwe_npc(podsumowanie.get("Start_NPC"))
    npc_koniec = normalizuj_nazwe_npc(podsumowanie.get("Koniec_NPC"))

    nastepna_misja = podsumowanie.get("Następna_Misja")
    poprzednia_misja = podsumowanie.get("Poprzednia_Misja")

    lvl_raw = podsumowanie.get("Wymagany_Poziom")
    lvl_txt = str(lvl_raw).strip() if lvl_raw is not None else ""

    if lvl_txt == "":
        lvl = 0
    else:
        lvl_txt = lvl_txt.split("-")[0].strip()
        lvl_digits = "".join(ch for ch in lvl_txt if ch.isdigit())[:2]
        lvl = int(lvl_digits) if lvl_digits != "" else 0

    if jezyk == "EN":
        misja_id = zapewnij_misje_i_pobierz_id(
            silnik=silnik,
            tabela_npc=tabela_npc,
            tabela_misje=tabela_misje,
            url=url,
            tytul=tytul,
            nastepna_misja=nastepna_misja,
            poprzednia_misja=poprzednia_misja,
            lvl=lvl,
            npc_start=npc_start,
            npc_koniec=npc_koniec
        )
    else:
        misja_id = misja_id_pl

    sekcje_do_statusow = [f"Cele_{jezyk}", f"Treść_{jezyk}", f"Postęp_{jezyk}", f"Zakończenie_{jezyk}", f"Nagrody_{jezyk}"]
    wszystkie_wiersze_misje = []

    for segment in sekcje_do_statusow:
        segment_db = mapa_segment.get(segment)
        if segment_db is None:
            continue

        segment_dict = misje_en.get(segment, {})
        if not isinstance(segment_dict, dict) or not segment_dict:
            continue

        if segment == f"Cele_{jezyk}":
            for podsegment, wartosc in segment_dict.items():
                podsegment_db = mapa_podsegment.get(podsegment)
                if podsegment_db is None or not isinstance(wartosc, dict):
                    continue

                for nr_key, tresc in wartosc.items():
                    if tresc is None:
                        continue
                    tresc = str(tresc).strip()
                    if not tresc:
                        continue

                    try:
                        nr = int(str(nr_key).strip())
                    except ValueError:
                        nr = 1

                    wszystkie_wiersze_misje.append({
                        "misja_id": misja_id,
                        "segment": segment_db,
                        "podsegment": podsegment_db,
                        "nr": nr,
                        "status": status,
                        "tresc": tresc
                    })
        else:
            for nr_key, tresc in segment_dict.items():
                if tresc is None:
                    continue
                tresc = str(tresc).strip()
                if not tresc:
                    continue

                try:
                    nr = int(str(nr_key).strip())
                except ValueError:
                    nr = 1

                wszystkie_wiersze_misje.append({
                    "misja_id": misja_id,
                    "segment": segment_db,
                    "podsegment": None,
                    "nr": nr,
                    "status": status,
                    "tresc": tresc
                })

    dodaj_statusy_misji_batch(
        silnik=silnik,
        tabela_misje_statusy=tabela_misje_statusy,
        rekordy=wszystkie_wiersze_misje
    )

    return misja_id

def zaktualizuj_misje_z_wowhead_w_db(
        silnik,
        wynik: dict,
        misja_id: int,
        tabela_misje: str = "dbo.MISJE"
    ) -> None:
    
    wh_id = wynik.get("wowhead_id")
    wh_url = wynik.get("wowhead_url")

    if not wh_id:
        return

    q_update = text(f"""
        UPDATE {tabela_misje}
        SET MISJA_ID_Z_GRY = :id_gra, MISJA_URL_WOWHEAD = :url
        WHERE MISJA_ID_MOJE_PK = :misja_id
    """)

    with silnik.begin() as conn:
        conn.execute(q_update, {
            "id_gra": wh_id,
            "url": wh_url,
            "misja_id": misja_id
        })

def zapisz_dialogi_statusy_do_db(
        silnik,
        wynik: dict,
        misja_id: int,
        tabela_npc: str,
        tabela_npc_statusy: str,
        tabela_dialogi_statusy: str,
        zrodlo: str,
        status: str = "0_ORYGINAŁ",
        jezyk: str = "EN"
    ) -> None:

    mapa_segment = {
        "dymek": "DYMEK",
        "gossip": "GOSSIP"
    }

    dialogi_en = wynik.get(f"Dialogi_{jezyk}", {})
    sequence = dialogi_en.get(f"Gossipy_Dymki_{jezyk}", [])

    if not isinstance(sequence, list) or len(sequence) == 0:
        return

    wszystkie_wiersze_dialogi = []

    for el in sequence:
        typ = (el.get("typ") or "").strip()
        segment_db = mapa_segment.get(typ)
        if segment_db is None:
            continue

        npc_nazwa = (el.get(f"npc_{jezyk.lower()}") or "").strip()
        if npc_nazwa == "":
            npc_nazwa = "Brak Danych"

        npc_id_fk = zapewnij_npc_i_pobierz_id(
            silnik=silnik,
            tabela_npc=tabela_npc,
            tabela_npc_statusy=tabela_npc_statusy,
            nazwa=npc_nazwa,
            zrodlo=zrodlo,
            status="0_ORYGINAŁ"
        )

        nr_bloku_dialogu_raw = el.get("id")
        try:
            nr_bloku_dialogu = int(str(nr_bloku_dialogu_raw).strip())
        except (TypeError, ValueError):
            nr_bloku_dialogu = 1

        wyp = el.get(f"wypowiedzi_{jezyk}") or {}
        if not isinstance(wyp, dict) or len(wyp) == 0:
            continue

        for nr_key, tresc in wyp.items():
            if tresc is None:
                continue
            tresc = str(tresc).strip()
            if tresc == "":
                continue

            try:
                nr_wypowiedzi = int(str(nr_key).strip())
            except ValueError:
                nr_wypowiedzi = 1

            wszystkie_wiersze_dialogi.append({
                "misja_id_fk": misja_id,
                "segment": segment_db,
                "status": status,
                "nr_bloku_dialogu": nr_bloku_dialogu,
                "nr_wypowiedzi": nr_wypowiedzi,
                "npc_id_fk": npc_id_fk,
                "tresc": tresc
            })

    dodaj_statusy_dialogu_batch(
        silnik=silnik,
        tabela_dialogi_statusy=tabela_dialogi_statusy,
        rekordy=wszystkie_wiersze_dialogi
    )

def przefiltruj_dane_misji(dane_wejsciowe, jezyk: str = "EN"):
    sekcja_misje = dane_wejsciowe.get(f"Misje_{jezyk}", {})
    
    nowy_wynik = {
        f"Misje_{jezyk}": {
            f"Podsumowanie_{jezyk}": {
                "Tytuł": sekcja_misje.get(f"Podsumowanie_{jezyk}", {}).get("Tytuł")
            },
            f"Cele_{jezyk}": sekcja_misje.get(f"Cele_{jezyk}"),
            f"Treść_{jezyk}": sekcja_misje.get(f"Treść_{jezyk}"),
            f"Postęp_{jezyk}": sekcja_misje.get(f"Postęp_{jezyk}"),
            f"Zakończenie_{jezyk}": sekcja_misje.get(f"Zakończenie_{jezyk}"),
            f"Nagrody_{jezyk}": sekcja_misje.get(f"Nagrody_{jezyk}")
        },
        f"Dialogi_{jezyk}": dane_wejsciowe.get(f"Dialogi_{jezyk}")
    }
    
    return nowy_wynik

def zapisz_misje_dialogi_ai_do_db(silnik, misja_id, przetlumaczone, status):
    print(f"\n--- [START] Zapis misji ID: {misja_id} | Status: {status} ---")
    
    if status not in ("1_PRZETŁUMACZONO", "2_ZREDAGOWANO"):
        print(f"!!! BŁĄD: Nieprawidłowy status: {status}")
        return

    q_select_npc = text("""SELECT NPC_ID_FK 
                           FROM dbo.NPC_STATUSY 
                           WHERE NAZWA = :nazwa 
                             AND STATUS = '3_ZATWIERDZONO'""")
    
    q_update_tytul = text("""UPDATE dbo.MISJE
                             SET MISJA_TYTUL_PL = :tytul_pl,
                                 STATUS_MISJI = :status
                             WHERE MISJA_ID_MOJE_PK = :misja_id""")
    
    q_insert_misje = text("""
        INSERT INTO dbo.MISJE_STATUSY (MISJA_ID_MOJE_FK, SEGMENT, PODSEGMENT, STATUS, NR, TRESC)
        VALUES (:misja_id, :segment, :podsegment, :status, :nr, :tresc)
    """)
    
    q_insert_dialogi = text("""
        INSERT INTO dbo.DIALOGI_STATUSY (MISJA_ID_MOJE_FK, SEGMENT, STATUS, NR_BLOKU_DIALOGU, NR_WYPOWIEDZI, NPC_ID_FK, TRESC)
        VALUES (:misja_id, :segment, :status, :nr_bloku, :nr_wypowiedzi, :npc_id, :tresc)
    """)

    wszystkie_wiersze_misje = []
    wszystkie_wiersze_dialogi = []
    
    tytul_pl = przetlumaczone["Misje_PL"]["Podsumowanie_PL"].get("Tytuł")
    
    try:
        cele_g = przetlumaczone["Misje_PL"]["Cele_PL"]["Główny"]
        for nr, tresc in cele_g.items():
            wszystkie_wiersze_misje.append({"misja_id": misja_id, "segment": "CEL", "podsegment": "GŁÓWNY_CEL", "status": status, "nr": int(nr), "tresc": tresc})

        cele_p = przetlumaczone["Misje_PL"]["Cele_PL"]["Podrzędny"]
        for nr, tresc in cele_p.items():
            wszystkie_wiersze_misje.append({"misja_id": misja_id, "segment": "CEL", "podsegment": "PODRZĘDNY_CEL", "status": status, "nr": int(nr), "tresc": tresc})

        sekcje = ["Treść_PL", "Postęp_PL", "Zakończenie_PL", "Nagrody_PL"]
        mapa_sekcji = {"Treść_PL": "TREŚĆ", "Postęp_PL": "POSTĘP", "Zakończenie_PL": "ZAKOŃCZENIE", "Nagrody_PL": "NAGRODY"}
        
        for klucz in sekcje:
            slownik = przetlumaczone["Misje_PL"].get(klucz)
            if slownik:
                for nr, tresc in slownik.items():
                    wszystkie_wiersze_misje.append({"misja_id": misja_id, "segment": mapa_sekcji[klucz], "podsegment": None, "status": status, "nr": int(nr), "tresc": tresc})
        
        print(f"-> Przygotowano danych misji: {len(wszystkie_wiersze_misje)} wierszy.")

    except Exception as e:
        print(f"!!! BŁĄD podczas parsowania słownika misji: {e}")
        return

    try:
        with silnik.begin() as conn:
            if tytul_pl:
                conn.execute(q_update_tytul, {"tytul_pl": tytul_pl, "misja_id": misja_id, "status": int(status[:1])})
                print(f"-> Zaktualizowano tytuł na: '{tytul_pl}'")

            if "Dialogi_PL" in przetlumaczone and przetlumaczone["Dialogi_PL"]["Gossipy_Dymki_PL"]:
                print("-> Rozpoczynam mapowanie NPC w dialogach...")
                for blok in przetlumaczone["Dialogi_PL"]["Gossipy_Dymki_PL"]:
                    npc_nazwa = blok.get("npc_pl")
                    npc_id = conn.execute(q_select_npc, {"nazwa": npc_nazwa}).scalar()

                    if npc_id is not None:
                        for nr_wyp, tekst in blok["wypowiedzi_PL"].items():
                            wszystkie_wiersze_dialogi.append({
                                "misja_id": misja_id, "segment": blok["typ"].upper(), "status": status,
                                "nr_bloku": int(blok["id"]), "nr_wypowiedzi": int(nr_wyp), 
                                "npc_id": int(npc_id), "tresc": tekst
                            })
                    else:
                        print(f"   [WARN] POMINIĘTO dialogi dla NPC: '{npc_nazwa}' (Brak ID w bazie lub niezatwierdzony)")

            print(f"-> Przygotowano dialogów: {len(wszystkie_wiersze_dialogi)} wierszy.")

            if wszystkie_wiersze_misje:
                conn.execute(q_insert_misje, wszystkie_wiersze_misje)
            
            if wszystkie_wiersze_dialogi:
                conn.execute(q_insert_dialogi, wszystkie_wiersze_dialogi)
            
            print("-> COMMIT: Dane zostały wysłane do bazy.")

    except Exception as e:
        print(f"\n!!! BŁĄD KRYTYCZNY PODCZAS ZAPISU DO BAZY:\n{e}")
        raise

    print(f"--- [KONIEC] Sukces dla misji ID: {misja_id} ---\n")

def usun_niezredagowane(silnik):
    q_select_drop_misje = text("""
    DROP TABLE IF EXISTS #misje_bez_redakcji;

    WITH statusy_liczby AS (
        SELECT
            MISJA_ID_MOJE_FK,
            CAST(SUBSTRING(STATUS, 1, CHARINDEX('_', STATUS) - 1) AS INT) AS status_liczba
        FROM dbo.MISJE_STATUSY
        GROUP BY MISJA_ID_MOJE_FK, STATUS
    )
    SELECT MISJA_ID_MOJE_FK
    INTO #misje_bez_redakcji
    FROM statusy_liczby
    GROUP BY MISJA_ID_MOJE_FK
    HAVING SUM(status_liczba) = 1
    ;

    DELETE cel
    FROM dbo.MISJE_STATUSY AS cel
    INNER JOIN #misje_bez_redakcji zrodlo 
    ON cel.MISJA_ID_MOJE_FK = zrodlo.MISJA_ID_MOJE_FK
    WHERE cel.STATUS = '1_PRZETŁUMACZONO'
    ;

    DELETE cel
    FROM dbo.DIALOGI_STATUSY AS cel
    INNER JOIN #misje_bez_redakcji zrodlo 
    ON cel.MISJA_ID_MOJE_FK = zrodlo.MISJA_ID_MOJE_FK
    WHERE cel.STATUS = '1_PRZETŁUMACZONO'
    ;

    UPDATE dbo.MISJE
    SET STATUS_MISJI = 0
    WHERE MISJA_ID_MOJE_PK IN (SELECT MISJA_ID_MOJE_FK  FROM #misje_bez_redakcji)
    ;

    DROP TABLE #misje_bez_redakcji;
    """)

    try:
        with silnik.begin() as conn:
            conn.execute(q_select_drop_misje)
    except Exception as e:
        print(f"--- Błąd podczas usuwania misji: {e}")
        raise

def policz_zapisz_wskazniki(silnik, misja: int):
    """
    Przelicza wskazniki a nastepnie robi zapis do tabel:
    a. dbo.MISJE_WSKAZNIKI_ZGODNOSCI (tylko najnowsze dane)
    b. dbo.MISJE (status globalny dla danej misji)
    """
    q_delete = text("""
        DELETE FROM dbo.MISJE_WSKAZNIKI_ZGODNOSCI
        WHERE MISJA_ID_MOJE_FK = :m
        ;
    """) # mnie interesuja tylko wskazniki dla najnowszych misji, co bylo kiedys w tym przypadku to strata miejsca

    q_insert = text("""
        ;WITH ARCH_LAST AS (
            SELECT
                M.MISJA_ID_Z_GRY,
                M.MISJA_ID_MOJE_PK,
                A.TABELA,
                MAX(CAST(A.DATA_ARCHIWIZACJI AS DATETIME2(0))) AS DATA_ARCHIWIZACJI
            FROM dbo.ARCHIWUM_MISJE_DIALOGI AS A
            INNER JOIN dbo.MISJE AS M
              ON A.MISJA_ID_Z_GRY = M.MISJA_ID_Z_GRY
            WHERE 1=1
              AND A.STATUS = N'0_ORYGINAŁ'
              AND M.MISJA_ID_Z_GRY IS NOT NULL
              AND M.MISJA_ID_Z_GRY <> 123456789
              AND M.MISJA_ID_MOJE_PK = :m
            GROUP BY
                M.MISJA_ID_Z_GRY,
                M.MISJA_ID_MOJE_PK,
                A.TABELA
        ),

        ARCH_NORM AS (
            SELECT
                A.TABELA,
                AL.MISJA_ID_MOJE_PK,
                CASE
                    WHEN A.SEGMENT = N'CEL' AND A.PODSEGMENT = N'GŁÓWNY_CEL' THEN N'GŁÓWNY_CEL'
                    WHEN A.SEGMENT = N'CEL' AND A.PODSEGMENT = N'PODRZĘDNY_CEL' THEN N'PODRZĘDNY_CEL'
                    ELSE A.SEGMENT
                END AS SEGMENT,
                A.TRESC
            FROM dbo.ARCHIWUM_MISJE_DIALOGI AS A
            INNER JOIN ARCH_LAST AS AL
              ON A.MISJA_ID_Z_GRY = AL.MISJA_ID_Z_GRY
             AND A.TABELA = AL.TABELA
             AND CAST(A.DATA_ARCHIWIZACJI AS DATETIME2(0)) = AL.DATA_ARCHIWIZACJI
            WHERE 1=1
              AND A.STATUS = N'0_ORYGINAŁ'
        ),

        TERAZ_MISJE_NORM AS (
            SELECT
                N'MISJE_STATUSY' AS TABELA,
                M.MISJA_ID_MOJE_PK,
                CASE
                    WHEN MS.SEGMENT = N'CEL' AND MS.PODSEGMENT = N'GŁÓWNY_CEL' THEN N'GŁÓWNY_CEL'
                    WHEN MS.SEGMENT = N'CEL' AND MS.PODSEGMENT = N'PODRZĘDNY_CEL' THEN N'PODRZĘDNY_CEL'
                    ELSE MS.SEGMENT
                END AS SEGMENT,
                MS.TRESC
            FROM dbo.MISJE_STATUSY AS MS
            INNER JOIN dbo.MISJE AS M
             ON MS.MISJA_ID_MOJE_FK = M.MISJA_ID_MOJE_PK
            WHERE 1=1
              AND M.MISJA_ID_Z_GRY IS NOT NULL
              AND M.MISJA_ID_Z_GRY <> 123456789
              AND MS.STATUS = N'0_ORYGINAŁ'
              AND M.MISJA_ID_MOJE_PK = :m
        ),

        TERAZ_DIALOGI_NORM AS (
            SELECT
                N'DIALOGI_STATUSY' AS TABELA,
                M.MISJA_ID_MOJE_PK,
                DS.SEGMENT,
                DS.TRESC
            FROM dbo.DIALOGI_STATUSY AS DS
            INNER JOIN dbo.MISJE AS M
              ON DS.MISJA_ID_MOJE_FK = M.MISJA_ID_MOJE_PK
            WHERE 1=1
              AND M.MISJA_ID_Z_GRY IS NOT NULL
              AND M.MISJA_ID_Z_GRY <> 123456789
              AND DS.STATUS = N'0_ORYGINAŁ'
              AND M.MISJA_ID_MOJE_PK = :m
        ),

        TERAZ_NORM AS (
            SELECT *
            FROM TERAZ_MISJE_NORM

            UNION ALL

            SELECT *
            FROM TERAZ_DIALOGI_NORM
        ),

        TERAZ AS (
            SELECT
                T.MISJA_ID_MOJE_PK,
                T.SEGMENT,
                SUM(CAST(LEN(T.TRESC) AS BIGINT)) AS ZNAKI_TERAZ,
                COUNT_BIG(*) AS ZDANIA_TERAZ
            FROM TERAZ_NORM AS T
            GROUP BY
                T.MISJA_ID_MOJE_PK,
                T.SEGMENT
        ),

        ARCHIWUM AS (
            SELECT
                A.MISJA_ID_MOJE_PK,
                A.SEGMENT,
                SUM(CAST(LEN(A.TRESC) AS BIGINT)) AS ZNAKI_ARCH,
                COUNT_BIG(*) AS ZDANIA_ARCH
            FROM ARCH_NORM AS A
            GROUP BY
                A.MISJA_ID_MOJE_PK,
                A.SEGMENT
        ),

        TERAZ_TXT AS (
            SELECT
                T.TABELA,
                T.MISJA_ID_MOJE_PK,
                T.SEGMENT,
                T.TRESC,
                COUNT_BIG(*) AS ILE_TERAZ
            FROM TERAZ_NORM AS T
            GROUP BY
                T.TABELA,
                T.MISJA_ID_MOJE_PK,
                T.SEGMENT,
                T.TRESC
        ),

        ARCH_TXT AS (
            SELECT
                A.TABELA,
                A.MISJA_ID_MOJE_PK,
                A.SEGMENT,
                A.TRESC,
                COUNT_BIG(*) AS ILE_ARCH
            FROM ARCH_NORM AS A
            GROUP BY
                A.TABELA,
                A.MISJA_ID_MOJE_PK,
                A.SEGMENT,
                A.TRESC
        ),

        WSPOLNE AS (
            SELECT
                T.MISJA_ID_MOJE_PK,
                T.SEGMENT,
                SUM(
                    CAST(LEN(T.TRESC) AS BIGINT) *
                    CASE
                        WHEN T.ILE_TERAZ < A.ILE_ARCH THEN T.ILE_TERAZ
                        ELSE A.ILE_ARCH
                    END
                ) AS ZNAKI_WSPOLNE,
                SUM(
                    CASE
                        WHEN T.ILE_TERAZ < A.ILE_ARCH THEN T.ILE_TERAZ
                        ELSE A.ILE_ARCH
                    END
                ) AS ZDANIA_WSPOLNE
            FROM TERAZ_TXT AS T
            INNER JOIN ARCH_TXT AS A
              ON T.TABELA = A.TABELA
             AND T.MISJA_ID_MOJE_PK = A.MISJA_ID_MOJE_PK
             AND T.SEGMENT = A.SEGMENT
             AND T.TRESC = A.TRESC
            GROUP BY
                T.MISJA_ID_MOJE_PK,
                T.SEGMENT
        ),

        BAZA AS (
            SELECT
                T.MISJA_ID_MOJE_PK,
                T.SEGMENT,
                T.ZNAKI_TERAZ,
                COALESCE(A.ZNAKI_ARCH, 0) AS ZNAKI_ARCH,
                COALESCE(W.ZNAKI_WSPOLNE, 0) AS ZNAKI_WSPOLNE,
                T.ZDANIA_TERAZ,
                COALESCE(A.ZDANIA_ARCH, 0) AS ZDANIA_ARCH,
                COALESCE(W.ZDANIA_WSPOLNE, 0) AS ZDANIA_WSPOLNE
            FROM TERAZ AS T
            LEFT JOIN ARCHIWUM AS A
              ON T.MISJA_ID_MOJE_PK = A.MISJA_ID_MOJE_PK
             AND T.SEGMENT = A.SEGMENT
            LEFT JOIN WSPOLNE AS W
              ON T.MISJA_ID_MOJE_PK = W.MISJA_ID_MOJE_PK
             AND T.SEGMENT = W.SEGMENT
        ),

        KPI AS (
            SELECT
                B.MISJA_ID_MOJE_PK,
                B.SEGMENT,

                B.ZNAKI_TERAZ,
                B.ZNAKI_ARCH,
                B.ZNAKI_WSPOLNE,

                CAST(
                    COALESCE(1.0 * B.ZNAKI_WSPOLNE / NULLIF(B.ZNAKI_TERAZ, 0), 0)
                    AS DECIMAL(10, 4)
                ) AS PROC_WSPOLNYCH_ZNAKOW,

                B.ZDANIA_TERAZ,
                B.ZDANIA_ARCH,
                B.ZDANIA_WSPOLNE,

                CAST(
                    COALESCE(1.0 * B.ZDANIA_WSPOLNE / NULLIF(B.ZDANIA_TERAZ, 0), 0)
                    AS DECIMAL(10, 4)
                ) AS PROC_WSPOLNYCH_ZDAN,

                CASE
                    WHEN B.ZNAKI_TERAZ = B.ZNAKI_ARCH THEN N'TYLE SAMO'
                    WHEN B.ZNAKI_TERAZ > B.ZNAKI_ARCH THEN N'WIECEJ TERAZ'
                    ELSE N'MNIEJ TERAZ'
                END AS KIERUNEK_ZMIANY_ZNAKI,

                CASE
                    WHEN B.ZDANIA_TERAZ = B.ZDANIA_ARCH THEN N'TYLE SAMO'
                    WHEN B.ZDANIA_TERAZ > B.ZDANIA_ARCH THEN N'WIECEJ TERAZ'
                    ELSE N'MNIEJ TERAZ'
                END AS KIERUNEK_ZMIANY_LICZBY_ZDAN
            FROM BAZA AS B
        )

        INSERT INTO dbo.MISJE_WSKAZNIKI_ZGODNOSCI (
            MISJA_ID_MOJE_FK,
            SEGMENT,
            WSKAZNIK_ZGODNOSCI,
            PROC_WSPOLNYCH_ZNAKOW,
            PROC_WSPOLNYCH_ZDAN,
            ZNAKI_TERAZ,
            ZNAKI_ARCH,
            ZNAKI_WSPOLNE,
            ZDANIA_TERAZ,
            ZDANIA_ARCH,
            ZDANIA_WSPOLNE,
            KIERUNEK_ZMIANY_ZNAKI,
            KIERUNEK_ZMIANY_LICZBY_ZDAN
        )
        SELECT
            K.MISJA_ID_MOJE_PK AS MISJA_ID_MOJE_FK,
            K.SEGMENT,
            CAST(
                0.7 * K.PROC_WSPOLNYCH_ZNAKOW
            + 0.3 * K.PROC_WSPOLNYCH_ZDAN
                AS DECIMAL(10, 4)
            ) AS WSKAZNIK_ZGODNOSCI,
            K.PROC_WSPOLNYCH_ZNAKOW,
            K.PROC_WSPOLNYCH_ZDAN,
            K.ZNAKI_TERAZ,
            K.ZNAKI_ARCH,
            K.ZNAKI_WSPOLNE,
            K.ZDANIA_TERAZ,
            K.ZDANIA_ARCH,
            K.ZDANIA_WSPOLNE,
            K.KIERUNEK_ZMIANY_ZNAKI,
            K.KIERUNEK_ZMIANY_LICZBY_ZDAN
        FROM KPI AS K;
    """)

    q_update = text("""
        WITH X AS (
    SELECT
        MISJA_ID_MOJE_FK,
        SUM(WSKAZNIK_ZGODNOSCI * ZNAKI_TERAZ) / NULLIF(SUM(ZNAKI_TERAZ), 0) AS WSKAZNIK_GLOBALNY
    FROM dbo.MISJE_WSKAZNIKI_ZGODNOSCI
    WHERE MISJA_ID_MOJE_FK = :m
    GROUP BY
        MISJA_ID_MOJE_FK
    )
    UPDATE M
    SET M.WSKAZNIK_ZGODNOSCI = X.WSKAZNIK_GLOBALNY
    FROM dbo.MISJE AS M
    INNER JOIN X
      ON M.MISJA_ID_MOJE_PK = X.MISJA_ID_MOJE_FK
    WHERE M.MISJA_ID_MOJE_PK = :m;
    """)

    with silnik.begin() as conn:
        conn.execute(q_delete, {"m": misja})
        conn.execute(q_insert, {"m": misja})
        conn.execute(q_update, {"m": misja})
