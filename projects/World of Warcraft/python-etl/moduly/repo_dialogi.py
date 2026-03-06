from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from moduly.db_core import _czy_duplikat

def pobierz_sql_insert_status_dialogu(tabela_dialogi_statusy: str):
    return text(f"""
        INSERT INTO {tabela_dialogi_statusy} (
            MISJA_ID_MOJE_FK, SEGMENT, STATUS,
            NR_BLOKU_DIALOGU, NR_WYPOWIEDZI, NPC_ID_FK,
            TRESC
        )
        VALUES (
            :misja_id_fk, :segment, :status,
            :nr_bloku_dialogu, :nr_wypowiedzi, :npc_id_fk,
            :tresc
        );
    """)

def dodaj_status_dialogu(
        silnik,
        tabela_dialogi_statusy: str,
        misja_id: int,
        segment: str,
        nr_bloku_dialogu: int,
        nr_wypowiedzi: int,
        npc_id_fk: int,
        status: str,
        tresc: str
    ) -> None:

    q_insert_status = pobierz_sql_insert_status_dialogu(tabela_dialogi_statusy)

    try:
        with silnik.begin() as conn:
            conn.execute(
                q_insert_status,
                {
                    "misja_id_fk": misja_id,
                    "segment": segment,
                    "status": status,
                    "nr_bloku_dialogu": nr_bloku_dialogu,
                    "nr_wypowiedzi": nr_wypowiedzi,
                    "npc_id_fk": npc_id_fk,
                    "tresc": tresc
                }
            )
    except IntegrityError as e:
        if _czy_duplikat(e):
            return
        raise

def dodaj_statusy_dialogu_batch(
        silnik,
        tabela_dialogi_statusy: str,
        rekordy: list[dict]
    ) -> None:

    if not rekordy:
        return

    q_insert_status = pobierz_sql_insert_status_dialogu(tabela_dialogi_statusy)

    try:
        with silnik.begin() as conn:
            conn.execute(q_insert_status, rekordy)
    except IntegrityError as e:
        if not _czy_duplikat(e):
            raise

        for rekord in rekordy:
            dodaj_status_dialogu(
                silnik=silnik,
                tabela_dialogi_statusy=tabela_dialogi_statusy,
                misja_id=rekord["misja_id_fk"],
                segment=rekord["segment"],
                nr_bloku_dialogu=rekord["nr_bloku_dialogu"],
                nr_wypowiedzi=rekord["nr_wypowiedzi"],
                npc_id_fk=rekord["npc_id_fk"],
                status=rekord["status"],
                tresc=rekord["tresc"]
            )
