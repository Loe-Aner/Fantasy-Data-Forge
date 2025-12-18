from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

__all__ = ["czerwony_przycisk", 
           "utworz_engine_do_db", 
           "pobierz_dane_z_db", 
           "zapisz_npc_i_status_do_db", "zapisz_npc_i_status_do_db_z_wyniku"
           ]

def czerwony_przycisk(
        silnik
    ):
    q = text("""
        DROP TABLE IF EXISTS dbo.NPC_STATUSY;
        DROP TABLE IF EXISTS dbo.MISJE_STATUSY
        DROP TABLE IF EXISTS dbo.ZRODLO;
        DROP TABLE IF EXISTS dbo.MISJE;
        DROP TABLE IF EXISTS dbo.NPC;
""")
    
    with silnik.begin() as conn:
        conn.execute(q)

def utworz_engine_do_db(
        sterownik: str = "mssql+pyodbc",
        uzytkownik: str | None = None,
        haslo: str | None = None,
        host: str = "localhost",
        nazwa_db: str = "WoW_PL",
        dbapi: dict | None = None
) -> Engine:
    
    if dbapi is None:
        dbapi = {
            "driver": "ODBC Driver 18 for SQL Server",
            "Trusted_Connection": "yes",
            "TrustServerCertificate": "yes",
        }

    polaczenie = URL.create(
        drivername=sterownik,
        username=uzytkownik,
        password=haslo,
        host=host,
        database=nazwa_db,
        query=dbapi
    )

    silnik = create_engine(polaczenie, echo=False, future=True)
    return silnik

# zastanowic sie nad WHERE i innymi
def pobierz_dane_z_db(
        silnik, 
        tabela: str, 
        kolumny_FROM: list | None = None,
        top: int | None = None
    ):
    t = f"TOP {top}" if top is not None else ""
    kf = ", ".join(kolumny_FROM) if kolumny_FROM is not None else "*"

    q = text(f"""
      SELECT {t}
             {kf}
      FROM {tabela}
    """)
    
    with silnik.connect() as conn:
        wiersze = conn.execute(q).mappings().all()
        return [dict(w) for w in wiersze]

def zapisz_npc_i_status_do_db(
        silnik,
        tabela_npc: str,
        tabela_npc_statusy: str,
        nazwa: str,
        zrodlo: str,
        status: str = "0_ORYGINAŁ"
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
        if e.orig and any("2627" in str(arg) or "2601" in str(arg) for arg in e.orig.args):
            with silnik.begin() as conn:
                npc_id = conn.execute(q_select_id, {"nazwa": nazwa}).scalar_one()

                try:
                    conn.execute(q_insert_status, {"npc_id_fk": npc_id, "status": status, "nazwa": nazwa})
                except IntegrityError as e2:
                    if e2.orig and any("2627" in str(arg) or "2601" in str(arg) for arg in e2.orig.args):
                        pass
                    else:
                        raise

                return npc_id

        raise

def zapisz_npc_i_status_do_db_z_wyniku(
        silnik,
        tabela_npc: str,
        tabela_npc_statusy: str,
        szukaj_wg: list[str],
        wyscrapowana_tresc: dict,
        zrodlo: str,
        status: str = "0_ORYGINAŁ"
    ):
    podsumowanie = wyscrapowana_tresc["Misje_EN"]["Podsumowanie_EN"]

    for klucz in szukaj_wg:
        npc = podsumowanie.get(klucz)

        if npc is None:
            continue

        npc = str(npc).strip()
        if npc == "":
            continue

        zapisz_npc_i_status_do_db(
            silnik=utworz_engine_do_db(),
            tabela_npc=tabela_npc,
            tabela_npc_statusy=tabela_npc_statusy,
            nazwa=npc,
            zrodlo=zrodlo,
            status=status
        )

