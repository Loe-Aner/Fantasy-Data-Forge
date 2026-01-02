from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from moduly.db_core import _czy_duplikat

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

    q_insert_status = text(f"""
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