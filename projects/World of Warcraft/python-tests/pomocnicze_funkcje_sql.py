from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
import time
from scraper_pomocnicze import wyscrapuj_linki_z_kategorii_z_paginacja

__all__ = [
    "czerwony_przycisk",
    "_czy_duplikat",
    "utworz_engine_do_db",
    "pobierz_dane_z_db",

    "zapisz_npc_i_status_do_db",
    "zapisz_npc_i_status_do_db_z_wyniku",

    "zapewnij_misje_i_pobierz_id",
    "dodaj_status_misji",
    "zapisz_misje_i_statusy_do_db_z_wyniku",

    "zapewnij_npc_i_pobierz_id",
    "dodaj_status_dialogu",
    "zapisz_dialogi_statusy_do_db_z_wyniku",

    "zapisz_zrodlo_do_db",

    "wyscrapuj_kategorie_questow_i_zapisz_linki_do_db",
    "pobierz_linki_do_scrapowania",
    "usun_link_z_kolejki",
    "zapisz_link_do_scrapowania"
]


def czerwony_przycisk(
        silnik
    ):
    q = text("""
        DROP TABLE IF EXISTS dbo.LINKI_DO_SCRAPOWANIA;
        DROP TABLE IF EXISTS dbo.DIALOGI_STATUSY;
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

def _czy_duplikat(e: IntegrityError) -> bool:
    return bool(
        e.orig
        and any("2627" in str(arg) or "2601" in str(arg) for arg in getattr(e.orig, "args", []))
    )

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
            silnik=silnik,
            tabela_npc=tabela_npc,
            tabela_npc_statusy=tabela_npc_statusy,
            nazwa=npc,
            zrodlo=zrodlo,
            status=status
        )


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

    try:
        with silnik.begin() as conn:
            npc_start_id = conn.execute(q_select_npc_id, {"nazwa": (npc_start or "").strip()}).scalar_one()
            npc_koniec_id = conn.execute(q_select_npc_id, {"nazwa": (npc_koniec or "").strip()}).scalar_one()

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
        with silnik.begin() as conn:
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


def zapisz_misje_i_statusy_do_db_z_wyniku(
        silnik,
        wynik: dict,
        tabela_npc: str,
        tabela_misje: str,
        tabela_misje_statusy: str,
        status: str = "0_ORYGINAŁ"
    ) -> int:

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
    tytul = (podsumowanie.get("Tytuł") or "").strip()

    npc_start = (podsumowanie.get("Start_NPC") or "").strip()
    npc_koniec = (podsumowanie.get("Koniec_NPC") or "").strip()

    nastepna_misja = podsumowanie.get("Następna_Misja")
    poprzednia_misja = podsumowanie.get("Poprzednia_Misja")

    lvl_raw = podsumowanie.get("Wymagany_Poziom")
    lvl_txt = str(lvl_raw).strip() if lvl_raw is not None else ""

    if lvl_txt == "":
        lvl = 0
    else:
        lvl_txt = lvl_txt.split("-")[0].strip()
        lvl_digits = "".join(ch for ch in lvl_txt if ch.isdigit())
        lvl = int(lvl_digits) if lvl_digits != "" else 0


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

    sekcje_do_statusow = ["Cele_EN", "Treść_EN", "Postęp_EN", "Zakończenie_EN", "Nagrody_EN"]

    for segment in sekcje_do_statusow:
        segment_db = mapa_segment.get(segment)
        if segment_db is None:
            continue

        segment_dict = misje_en.get(segment, {})
        if not isinstance(segment_dict, dict) or not segment_dict:
            continue

        if segment == "Cele_EN":
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

                    dodaj_status_misji(
                        silnik=silnik,
                        tabela_misje_statusy=tabela_misje_statusy,
                        misja_id=misja_id,
                        segment=segment_db,
                        podsegment=podsegment_db,
                        nr=nr,
                        status=status,
                        tresc=tresc
                    )
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

                dodaj_status_misji(
                    silnik=silnik,
                    tabela_misje_statusy=tabela_misje_statusy,
                    misja_id=misja_id,
                    segment=segment_db,
                    podsegment=None,
                    nr=nr,
                    status=status,
                    tresc=tresc
                )

    return misja_id

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

def zapisz_dialogi_statusy_do_db_z_wyniku(
        silnik,
        wynik: dict,
        misja_id: int,
        tabela_npc: str,
        tabela_npc_statusy: str,
        tabela_dialogi_statusy: str,
        zrodlo: str,
        status: str = "0_ORYGINAŁ"
    ) -> None:

    mapa_segment = {
        "dymek": "DYMEK",
        "gossip": "GOSSIP"
    }

    dialogi_en = wynik.get("Dialogi_EN", {})
    sequence = dialogi_en.get("Gossipy_Dymki_EN", [])

    if not isinstance(sequence, list) or len(sequence) == 0:
        return

    for el in sequence:
        typ = (el.get("typ") or "").strip()
        segment_db = mapa_segment.get(typ)
        if segment_db is None:
            continue

        npc_nazwa = (el.get("npc_en") or "").strip()
        if npc_nazwa == "":
            continue

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
        except Exception:
            nr_bloku_dialogu = 1

        wyp = el.get("wypowiedzi_EN") or {}
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

            dodaj_status_dialogu(
                silnik=silnik,
                tabela_dialogi_statusy=tabela_dialogi_statusy,
                misja_id=misja_id,
                segment=segment_db,
                nr_bloku_dialogu=nr_bloku_dialogu,
                nr_wypowiedzi=nr_wypowiedzi,
                npc_id_fk=npc_id_fk,
                status=status,
                tresc=tresc
            )

def zapisz_zrodlo_do_db(
        silnik,
        tabela_zrodlo: str,
        misja_id: int,
        wynik: dict,
        zrodlo: str
    ) -> int:

    hash_html = wynik.get("Hash_HTML", {}) or {}

    hash_glowny_cel = (hash_html.get("Cele_EN", {}) or {}).get("Główny")
    hash_podrzedny_cel = (hash_html.get("Cele_EN", {}) or {}).get("Podrzędny")

    hash_tresc = hash_html.get("Treść_EN")
    hash_postep = hash_html.get("Postęp_EN")
    hash_zakonczenie = hash_html.get("Zakończenie_EN")
    hash_nagrody = hash_html.get("Nagrody_EN")

    hash_dymki = (hash_html.get("Dialogi_EN", {}) or {}).get("Dymki_EN")
    hash_gossip = (hash_html.get("Dialogi_EN", {}) or {}).get("Gossipy_EN")

    q_insert = text(f"""
        INSERT INTO {tabela_zrodlo} (
            MISJA_ID_MOJE_FK, ZRODLO_NAZWA,
            HTML_HASH_GLOWNY_CEL, HTML_HASH_PODRZEDNY_CEL,
            HTML_HASH_TRESC, HTML_HASH_POSTEP, HTML_HASH_ZAKONCZENIE, HTML_HASH_NAGRODY,
            HTML_HASH_DYMKI, HTML_HASH_GOSSIP
        )
        OUTPUT inserted.TECH_ID
        VALUES (
            :misja_id_fk, :zrodlo_nazwa,
            :hash_glowny_cel, :hash_podrzedny_cel,
            :hash_tresc, :hash_postep, :hash_zakonczenie, :hash_nagrody,
            :hash_dymki, :hash_gossip
        );
    """)

    with silnik.begin() as conn:
        tech_id = conn.execute(
            q_insert,
            {
                "misja_id_fk": misja_id,
                "zrodlo_nazwa": zrodlo,
                "hash_glowny_cel": hash_glowny_cel,
                "hash_podrzedny_cel": hash_podrzedny_cel,
                "hash_tresc": hash_tresc,
                "hash_postep": hash_postep,
                "hash_zakonczenie": hash_zakonczenie,
                "hash_nagrody": hash_nagrody,
                "hash_dymki": hash_dymki,
                "hash_gossip": hash_gossip
            }
        ).scalar_one()

        return tech_id

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError


def zapisz_link_do_scrapowania(
        silnik,
        url: str,
        zrodlo: str
    ) -> None:

    q = text("""
        INSERT INTO dbo.LINKI_DO_SCRAPOWANIA (URL, ZRODLO_NAZWA)
        VALUES (:url, :zrodlo)
    """)

    try:
        with silnik.begin() as conn:
            conn.execute(q, {"url": url, "zrodlo": zrodlo})
    except IntegrityError as e:
        if "2627" in str(e) or "2601" in str(e):
            pass
        else:
            raise

    
# def zapisz_linki_do_db(
#         silnik,
#         tresc,
#         zrodlo: str
#     ):
#     """
#     Przekazać tylko pierwszą misję w danym ciągu z wiki.
#     """

#     q_insert_url = text("""
#         INSERT INTO dbo.LINKI_DO_SCRAPOWANIA (URL, ZRODLO_NAZWA)
#         VALUES (:url, :zrodlo)
#     """)

#     with silnik.begin() as conn:
#         for url in wyscrapuj_linki_z_progression_wiki(tresc):
#             if not url:
#                 continue
#             try:
#                 conn.execute(q_insert_url, {"url": url, "zrodlo": zrodlo})
#             except IntegrityError as e:
#                 if not _czy_duplikat(e):
#                     raise

def wyscrapuj_kategorie_questow_i_zapisz_linki_do_db(
        silnik,
        kategorie_urls: list[str],
        zrodlo: str = "wiki",
        sleep_s: int = 1,
        printuj_paginacje: bool = True
    ) -> None:
    """
    Dla każdej kategorii:
    - zbiera linki questów (z paginacją),
    - zapisuje do dbo.LINKI_DO_SCRAPOWANIA,
    - printuje co dodano / co pominięto (duplikat).
    """

    q_insert_url = text("""
        INSERT INTO dbo.LINKI_DO_SCRAPOWANIA (URL, ZRODLO_NAZWA)
        VALUES (:url, :zrodlo)
    """)

    for kat_url in kategorie_urls:
        print(f"\n=== KATEGORIA: {kat_url} ===")

        linki = wyscrapuj_linki_z_kategorii_z_paginacja(
            kategoria_url=kat_url,
            sleep_s=sleep_s,
            printuj_paginacje=printuj_paginacje
        )

        print(f"    Zebrano linków: {len(linki)}")

        with silnik.begin() as conn:
            for i, url in enumerate(linki, start=1):
                if not url:
                    continue

                try:
                    conn.execute(q_insert_url, {"url": url, "zrodlo": zrodlo})
                    print(f"    [{i}/{len(linki)}] + DODANO: {url}")
                except IntegrityError as e:
                    if _czy_duplikat(e):
                        print(f"    [{i}/{len(linki)}] - POMINIĘTO (duplikat): {url}")
                    else:
                        raise

        time.sleep(sleep_s)

def pobierz_linki_do_scrapowania(silnik):
    q = text("""
        SELECT URL
        FROM dbo.LINKI_DO_SCRAPOWANIA
        WHERE ZRODLO_NAZWA = N'wiki'
        ORDER BY TECH_ID
    """)

    with silnik.connect() as conn:
        return [row[0] for row in conn.execute(q).all()]
    
def usun_link_z_kolejki(silnik, url: str) -> None:
    q = text("""
        DELETE FROM dbo.LINKI_DO_SCRAPOWANIA
        WHERE URL = :url
    """)
    with silnik.begin() as conn:
        conn.execute(q, {"url": url})