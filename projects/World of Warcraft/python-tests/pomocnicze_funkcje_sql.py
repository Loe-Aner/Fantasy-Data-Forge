from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

__all__ = ["czerwony_przycisk", 
           "utworz_engine_do_db", 
           "pobierz_dane_z_db", 
           "zapisz_npc_i_status_do_db", "zapisz_npc_i_status_do_db_z_wyniku",
           "zapisz_misje_i_status_do_db", "zapisz_misje_i_status_do_db_z_wyniku"
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

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError


def zapisz_misje_i_status_do_db(
        silnik,
        tabela_npc: str,
        npc_start: str,
        npc_koniec: str,
        tabela_misje: str,
        tabela_misje_statusy: str,
        tytul: str,
        nastepna_misja: str,
        poprzednia_misja: str,
        lvl: int,
        url: str,
        segment: str,
        podsegment: str | None,
        tresc: str,
        status: str,
        nr: int
    ) -> int:

    q_select_npc_start = text(f"""
        SELECT NPC_ID_MOJE_PK
        FROM {tabela_npc}
        WHERE NAZWA = :npc_start
    """)

    q_select_npc_koniec = text(f"""
        SELECT NPC_ID_MOJE_PK
        FROM {tabela_npc}
        WHERE NAZWA = :npc_koniec
    """)

    q_insert_misja = text(f"""
        INSERT INTO {tabela_misje} (
            MISJA_URL_WIKI, MISJA_TYTUL_EN, MISJA_TYTUL_NASTEPNA_EN, MISJA_TYTUL_POPRZEDNIA_EN,
            WYMAGANY_LVL, NPC_START_ID, NPC_KONIEC_ID
        )
        OUTPUT inserted.MISJA_ID_MOJE_PK
        VALUES (:url, :tytul, :nastepna_misja, :poprzednia_misja, :lvl, :npc_start_id, :npc_koniec_id);
    """)

    q_select_misja_id = text(f"""
        SELECT MISJA_ID_MOJE_PK
        FROM {tabela_misje}
        WHERE MISJA_URL_WIKI = :url;
    """)

    q_insert_status = text(f"""
        INSERT INTO {tabela_misje_statusy} (
            MISJA_ID_MOJE_FK, SEGMENT, PODSEGMENT, STATUS, NR, TRESC
        )
        VALUES (:misja_id_fk, :segment, :podsegment, :status, :nr, :tresc);
    """)

    try:
        with silnik.begin() as conn:
            npc_start_id = conn.execute(q_select_npc_start, {"npc_start": npc_start}).scalar_one()
            npc_koniec_id = conn.execute(q_select_npc_koniec, {"npc_koniec": npc_koniec}).scalar_one()

            misja_id = conn.execute(
                q_insert_misja,
                {
                    "url": url,
                    "tytul": tytul,
                    "nastepna_misja": nastepna_misja,
                    "poprzednia_misja": poprzednia_misja,
                    "lvl": lvl,
                    "npc_start_id": npc_start_id,
                    "npc_koniec_id": npc_koniec_id
                }
            ).scalar_one()

            conn.execute(
                q_insert_status,
                {
                    "misja_id_fk": misja_id,
                    "segment": segment,
                    "podsegment": podsegment,
                    "status": status,
                    "nr": nr,
                    "tresc": tresc
                }
            )
            return misja_id

    except IntegrityError as e:
        if e.orig and any("2627" in str(arg) or "2601" in str(arg) for arg in e.orig.args):
            with silnik.begin() as conn:
                misja_id = conn.execute(q_select_misja_id, {"url": url}).scalar_one()

                try:
                    conn.execute(
                        q_insert_status,
                        {
                            "misja_id_fk": misja_id,
                            "segment": segment,
                            "podsegment": podsegment,
                            "status": status,
                            "nr": nr,
                            "tresc": tresc
                        }
                    )
                except IntegrityError as e2:
                    if e2.orig and any("2627" in str(arg) or "2601" in str(arg) for arg in e2.orig.args):
                        pass
                    else:
                        raise

                return misja_id

        raise

def zapisz_misje_i_status_do_db_z_wyniku(
        silnik,
        wynik: dict,
        tabela_npc: str,
        tabela_misje: str,
        tabela_misje_statusy: str,
        status: str = "0_ORYGINAŁ"
    ) -> None:

    mapa_segment = {
        "Cele_EN": "CEL",
        "Treść_EN": "TREŚĆ",
        "Postęp_EN": "POSTĘP",
        "Zakończenie_EN": "ZAKOŃCZENIE",
        "Nagrody_EN": "NAGRODY"
    }

    mapa_podsegment = {
        "Główny": "GŁÓWNY_CEL",
        "Podrzędny": "PODRZĘDNY_CEL"
    }

    misje_en = wynik.get("Misje_EN", {})
    podsumowanie = misje_en.get("Podsumowanie_EN", {})

    url = wynik.get("Źródło", {}).get("url")
    tytul = podsumowanie.get("Tytuł")
    npc_start = podsumowanie.get("Start_NPC")
    npc_koniec = podsumowanie.get("Koniec_NPC")
    nastepna_misja = podsumowanie.get("Następna_Misja")
    poprzednia_misja = podsumowanie.get("Poprzednia_Misja")

    lvl_raw = podsumowanie.get("Wymagany_Poziom")
    lvl = int(str(lvl_raw).strip()) if lvl_raw is not None and str(lvl_raw).strip() != "" else 0

    sekcje_do_statusow = ["Cele_EN", "Treść_EN", "Postęp_EN", "Zakończenie_EN", "Nagrody_EN"]

    for segment in sekcje_do_statusow:
        segment_db = mapa_segment.get(segment)
        if segment_db is None:
            continue

        segment_dict = misje_en.get(segment, {})
        if not isinstance(segment_dict, dict) or len(segment_dict) == 0:
            continue

        if segment == "Cele_EN":
            for podsegment, wartosc in segment_dict.items():
                podsegment_db = mapa_podsegment.get(podsegment)
                if podsegment_db is None:
                    continue
                if not isinstance(wartosc, dict):
                    continue

                for nr_key, tresc in wartosc.items():
                    if tresc is None:
                        continue
                    tresc = str(tresc).strip()
                    if tresc == "":
                        continue

                    try:
                        nr = int(str(nr_key).strip())
                    except ValueError:
                        nr = 1

                    zapisz_misje_i_status_do_db(
                        silnik=silnik,
                        tabela_npc=tabela_npc,
                        npc_start=npc_start,
                        npc_koniec=npc_koniec,
                        tabela_misje=tabela_misje,
                        tabela_misje_statusy=tabela_misje_statusy,
                        tytul=tytul,
                        nastepna_misja=nastepna_misja,
                        poprzednia_misja=poprzednia_misja,
                        lvl=lvl,
                        url=url,
                        segment=segment_db, 
                        podsegment=podsegment_db,
                        tresc=tresc,
                        status=status,
                        nr=nr
                    )

        else:
            for nr_key, tresc in segment_dict.items():
                if tresc is None:
                    continue
                tresc = str(tresc).strip()
                if tresc == "":
                    continue

                try:
                    nr = int(str(nr_key).strip())
                except ValueError:
                    nr = 1

                zapisz_misje_i_status_do_db(
                    silnik=silnik,
                    tabela_npc=tabela_npc,
                    npc_start=npc_start,
                    npc_koniec=npc_koniec,
                    tabela_misje=tabela_misje,
                    tabela_misje_statusy=tabela_misje_statusy,
                    tytul=tytul,
                    nastepna_misja=nastepna_misja,
                    poprzednia_misja=poprzednia_misja,
                    lvl=lvl,
                    url=url,
                    segment=segment_db,
                    podsegment=None,
                    tresc=tresc,
                    status=status,
                    nr=nr
                )
