from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from moduly.db_core import _czy_duplikat

def zapisz_npc_i_status_do_db(
        silnik,
        tabela_npc: str,
        tabela_npc_statusy: str,
        nazwa: str,
        zrodlo: str,
        status: str
    ) -> int:

    q_insert_npc = text(f"""
        INSERT INTO {tabela_npc} (NAZWA, ZRODLO_NAZWA)
        OUTPUT inserted.NPC_ID_MOJE_PK
        VALUES (:nazwa, :zrodlo);
    """)

    q_select_id = text(f"""
        SELECT NPC_ID_MOJE_PK
        FROM {tabela_npc}
        WHERE NAZWA = :nazwa;
    """)

    q_insert_status = text(f"""
        INSERT INTO {tabela_npc_statusy} (NPC_ID_FK, STATUS, NAZWA)
        VALUES (:npc_id_fk, :status, :nazwa);
    """)

    try:
        with silnik.begin() as conn:
            npc_id = conn.execute(q_insert_npc, {"nazwa": nazwa, "zrodlo": zrodlo}).scalar_one()
            conn.execute(q_insert_status, {"npc_id_fk": npc_id, "status": status, "nazwa": nazwa})
            return npc_id

    except IntegrityError as e:
        if _czy_duplikat(e):
            with silnik.begin() as conn:
                npc_id = conn.execute(q_select_id, {"nazwa": nazwa}).scalar_one()

                try:
                    conn.execute(q_insert_status, {"npc_id_fk": npc_id, "status": status, "nazwa": nazwa})
                except IntegrityError as e2:
                    if not _czy_duplikat(e2):
                        raise

                return npc_id
        raise


def zapewnij_npc_i_pobierz_id(
        silnik,
        tabela_npc: str,
        tabela_npc_statusy: str,
        nazwa: str,
        zrodlo: str,
        status: str = "0_ORYGINAŁ"
    ) -> int:

    q_select_id = text(f"""
        SELECT NPC_ID_MOJE_PK
        FROM {tabela_npc}
        WHERE NAZWA = :nazwa;
    """)

    q_insert_npc = text(f"""
        INSERT INTO {tabela_npc} (NAZWA, ZRODLO_NAZWA)
        OUTPUT inserted.NPC_ID_MOJE_PK
        VALUES (:nazwa, :zrodlo);
    """)

    q_insert_status = text(f"""
        INSERT INTO {tabela_npc_statusy} (NPC_ID_FK, STATUS, NAZWA)
        VALUES (:npc_id_fk, :status, :nazwa);
    """)

    nazwa = (nazwa or "").strip()
    if nazwa == "":
        raise ValueError("NPC nazwa jest pusta - nie da sie wstawic dialogu bez NPC.")

    try:
        with silnik.begin() as conn:
            npc_id = conn.execute(q_insert_npc, {"nazwa": nazwa, "zrodlo": zrodlo}).scalar_one()

            try:
                conn.execute(q_insert_status, {"npc_id_fk": npc_id, "status": status, "nazwa": nazwa})
            except IntegrityError as e2:
                if not _czy_duplikat(e2):
                    raise

            return npc_id

    except IntegrityError as e:
        if not _czy_duplikat(e):
            raise

        with silnik.begin() as conn:
            npc_id = conn.execute(q_select_id, {"nazwa": nazwa}).scalar_one()

            try:
                conn.execute(q_insert_status, {"npc_id_fk": npc_id, "status": status, "nazwa": nazwa})
            except IntegrityError as e2:
                if not _czy_duplikat(e2):
                    raise

            return npc_id