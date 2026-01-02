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