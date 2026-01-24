from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from moduly.db_core import _czy_duplikat

def zapewnij_misje_i_pobierz_id(
        silnik,
        tabela_npc: str,
        tabela_misje: str,
        url: str,
        tytul: str,
        nastepna_misja: str | None,
        poprzednia_misja: str | None,
        lvl: int | None,
        npc_start: str,
        npc_koniec: str
    ) -> int:

    q_select_npc_id = text(f"""
        SELECT NPC_ID_MOJE_PK
        FROM {tabela_npc}
        WHERE NAZWA = :nazwa
    """)

    q_insert_misja = text(f"""
        INSERT INTO {tabela_misje} (
            MISJA_URL_WIKI, MISJA_TYTUL_EN, MISJA_TYTUL_NASTEPNA_EN, MISJA_TYTUL_POPRZEDNIA_EN, 
            WYMAGANY_LVL, NPC_START_ID, NPC_KONIEC_ID
        )
        OUTPUT inserted.MISJA_ID_MOJE_PK
        VALUES (:url, :tytul, :nastepna, :poprzednia, :lvl, :npc_start_id, :npc_koniec_id);
    """)

    q_select_misja_id = text(f"""
        SELECT MISJA_ID_MOJE_PK
        FROM {tabela_misje}
        WHERE MISJA_URL_WIKI = :url;
    """)

    npc_start = (npc_start or "").strip()
    npc_koniec = (npc_koniec or "").strip()

    try:
        with silnik.begin() as conn:
            if npc_start:
                try:
                    npc_start_id = conn.execute(q_select_npc_id, {"nazwa": npc_start}).scalar_one()
                except Exception:
                    npc_start_id = None 
            else:
                npc_start_id = None

            if npc_koniec:
                try:
                    npc_koniec_id = conn.execute(q_select_npc_id, {"nazwa": npc_koniec}).scalar_one()
                except Exception:
                    npc_koniec_id = None
            else:
                npc_koniec_id = None

            misja_id = conn.execute(
                q_insert_misja,
                {
                    "url": url,
                    "tytul": tytul,
                    "nastepna": nastepna_misja,
                    "poprzednia": poprzednia_misja,
                    "lvl": lvl,
                    "npc_start_id": npc_start_id,
                    "npc_koniec_id": npc_koniec_id
                }
            ).scalar_one()

            return misja_id

    except IntegrityError as e:
        if not _czy_duplikat(e):
            raise
        with silnik.connect() as conn:
            return conn.execute(q_select_misja_id, {"url": url}).scalar_one()


def dodaj_status_misji(
        silnik,
        tabela_misje_statusy: str,
        misja_id: int,
        segment: str,
        podsegment: str | None,
        nr: int,
        status: str,
        tresc: str
    ) -> None:

    q_insert_status = text(f"""
        INSERT INTO {tabela_misje_statusy} (
            MISJA_ID_MOJE_FK, SEGMENT, PODSEGMENT, STATUS, NR, TRESC
        )
        VALUES (:misja_id, :segment, :podsegment, :status, :nr, :tresc);
    """)

    try:
        with silnik.begin() as conn:
            conn.execute(
                q_insert_status,
                {
                    "misja_id": misja_id,
                    "segment": segment,
                    "podsegment": podsegment,
                    "status": status,
                    "nr": nr,
                    "tresc": tresc
                }
            )
    except IntegrityError as e:
        if _czy_duplikat(e):
            return
        raise

def pobierz_liste_id_dla_dodatku(silnik, nazwa_dodatku: str):
    """
    Zwraca listę wszystkich istniejących ID dla danego dodatku (np. [1, 2, 5, 1200, 1201]).
    """
    q = text("""
        SELECT MISJA_ID_MOJE_PK
        FROM dbo.MISJE
        WHERE DODATEK_EN = :dodatek
        ORDER BY MISJA_ID_MOJE_PK
    """)
    
    with silnik.connect() as conn:
        wynik = [row[0] for row in conn.execute(q, {"dodatek": nazwa_dodatku})]
        
    return wynik

def ustaw_id_misji_duble_123456789(silnik):
    """
    Koryguje ID misji z wowheada. Czasami ID się dublują przez podobną nazwę.
    Skrypt koryguje to podstawiając pod niektóre id = 123456789.
    W kolejnych skryptach takie misje nie będą brane pod uwagę, by nie zaśmiecać bazy danych.
    """
    q_select = text("""
    WITH ile_razy_wystepuje AS (
        SELECT
            MISJA_ID_MOJE_PK,
            MISJA_ID_Z_GRY,
            MISJA_URL_WIKI,
            COUNT(MISJA_ID_Z_GRY) OVER (PARTITION BY MISJA_ID_Z_GRY) AS CNT
        FROM dbo.MISJE
        WHERE MISJA_ID_Z_GRY != 123456789
    ),

    cnt_i_dlugosc AS (
        SELECT
            MISJA_ID_MOJE_PK,
            MISJA_ID_Z_GRY,
            MISJA_URL_WIKI,
            CNT,
            LEN(MISJA_URL_WIKI) AS DLUGOSC_URL,
            MIN(LEN(MISJA_URL_WIKI)) OVER (PARTITION BY MISJA_ID_Z_GRY) AS MIN_DLUGOSC
        FROM ile_razy_wystepuje
        WHERE CNT > 1
    )

    SELECT
        MISJA_ID_MOJE_PK,
        CASE
            WHEN CNT = 2 AND DLUGOSC_URL = MIN_DLUGOSC THEN '123456789'
            WHEN CNT = 2 THEN MISJA_ID_Z_GRY
            WHEN CNT >= 3 AND DLUGOSC_URL = MIN_DLUGOSC THEN MISJA_ID_Z_GRY
            WHEN CNT >= 3 THEN '123456789'
            ELSE MISJA_ID_Z_GRY
        END AS MISJA_ID_Z_GRY
    FROM cnt_i_dlugosc
    """)

    q_update = text("""
        UPDATE dbo.MISJE
        SET MISJA_ID_Z_GRY = :MISJA_ID_Z_GRY
        WHERE MISJA_ID_MOJE_PK = :MISJA_ID_MOJE_PK
    """)

    with silnik.begin() as conn:
        w = conn.execute(q_select).mappings().all()
        if len(w) > 0:
            wynik = conn.execute(q_update, w)
            print(f"Zaktualizowano: {wynik.rowcount} wierszy.")
        else:
            print("Brak danych do dodania.")

def ujednolic_tytuly_misji(silnik):
    q_update = text("""
        WITH do_poprawy AS (
            SELECT 
                MISJA_ID_MOJE_PK,
                MISJA_TYTUL_PL,
                FIRST_VALUE(MISJA_TYTUL_PL) OVER (
                    PARTITION BY MISJA_TYTUL_EN 
                    ORDER BY MISJA_ID_MOJE_PK ASC
                ) AS WZORCOWY_TYTUL
            FROM dbo.MISJE
            WHERE 1=1
              AND MISJA_ID_Z_GRY != 123456789
              AND MISJA_TYTUL_PL IS NOT NULL
        )
        UPDATE do_poprawy
        SET MISJA_TYTUL_PL = WZORCOWY_TYTUL
        WHERE MISJA_TYTUL_PL <> WZORCOWY_TYTUL;
    """)

    try:
        print("Rozpoczynam ujednolicanie tytułów...")
        with silnik.begin() as conn:
            wynik = conn.execute(q_update)
            print(f"Sukces. Zaktualizowano tytułów: {wynik.rowcount}")
            
    except Exception as e:
        print(f"Wystąpił błąd podczas aktualizacji: {e}")