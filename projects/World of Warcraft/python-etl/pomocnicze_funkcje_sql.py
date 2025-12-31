import os
import json
import time

from dotenv import load_dotenv
from google import genai
from google.genai import types
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import IntegrityError
import pandas as pd

from scraper_pomocnicze import wyscrapuj_linki_z_kategorii_z_paginacja
from scraper_wiki_main import parsuj_misje_z_url

__all__ = [
    "_czy_duplikat",
    "utworz_engine_do_db",

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
    "zapisz_link_do_scrapowania",
    
    "roznice_hashe",
    "hash_typ_lista",
    "roznice_hashe_usun_rekordy_z_db",
    "hashuj_kategorie_i_zapisz_zrodlo",

    "aktualizuj_misje_z_excela",

    "pobierz_przetworz_zapisz_batch_lista",
    "pobierz_liste_id_dla_dodatku",

    "slowa_kluczowe_do_db",
    "mapowanie_misji_do_db",

    "zapisz_npc_i_status_przetlumaczony_do_db"
]


def czerwony_przycisk(
        silnik
    ):
    q = text("""
        DROP TABLE IF EXISTS dbo.MISJE_SLOWA_KLUCZOWE;
        DROP TABLE IF EXISTS dbo.MISJE_SLOWA;
        
        DROP TABLE IF EXISTS dbo.DIALOGI_STATUSY;
        DROP TABLE IF EXISTS dbo.MISJE_STATUSY;
        DROP TABLE IF EXISTS dbo.NPC_STATUSY;
        DROP TABLE IF EXISTS dbo.ZRODLO;
        
        DROP TABLE IF EXISTS dbo.MISJE;
        
        DROP TABLE IF EXISTS dbo.NPC;
        DROP TABLE IF EXISTS dbo.SLOWA_KLUCZOWE;
        DROP TABLE IF EXISTS dbo.LINKI_DO_SCRAPOWANIA;
""")
    
    with silnik.begin() as conn:
        conn.execute(q)
        print("Baza danych... już nie istnieje. Zadowolony jesteś z siebie?")

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
        lvl_digits = "".join(ch for ch in lvl_txt if ch.isdigit())[:2]
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
        if _czy_duplikat(e):
            return
        raise


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
            sleep_s=0,
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

def roznice_hashe (
        silnik,
        dozwolone_kolumny: list
        ):

    hash_roznice = {}

    for hash_typ in dozwolone_kolumny:
        with silnik.connect() as conn:
            roznice = text(f"""
                WITH dwa_ostatnie AS (
                    SELECT
                        MISJA_ID_MOJE_FK,
                        {hash_typ},
                        DATA_WYSCRAPOWANIA,
                        ROW_NUMBER() OVER (
                            PARTITION BY MISJA_ID_MOJE_FK
                            ORDER BY DATA_WYSCRAPOWANIA DESC
                        ) AS rnk
                    FROM dbo.ZRODLO
                    WHERE ZRODLO_NAZWA = N'wiki'
                ),
                porownanie AS (
                    SELECT
                        MISJA_ID_MOJE_FK,
                        {hash_typ} AS NAJN,
                        LAG({hash_typ}) OVER (
                            PARTITION BY MISJA_ID_MOJE_FK
                            ORDER BY DATA_WYSCRAPOWANIA DESC
                        ) AS POPRZ,
                        rnk
                    FROM dwa_ostatnie
                    WHERE rnk <= 2
                )
                SELECT
                    MISJA_ID_MOJE_FK
                FROM porownanie
                WHERE 1=1
                AND POPRZ IS NOT NULL
                AND NAJN <> POPRZ;
                """)
            r = conn.execute(roznice).scalars().all()
            hash_roznice[hash_typ] = r
    return hash_roznice

def hash_typ_lista():
    hash_typ_lista = ["HTML_HASH_GLOWNY_CEL", "HTML_HASH_PODRZEDNY_CEL", "HTML_HASH_TRESC", "HTML_HASH_POSTEP", "HTML_HASH_ZAKONCZENIE",
                    "HTML_HASH_NAGRODY", "HTML_HASH_DYMKI", "HTML_HASH_GOSSIP"]
    return hash_typ_lista

def roznice_hashe_usun_rekordy_z_db(
        silnik,
        zrodlo_insert_url: str
    ):

    print("▶ Szukam różnic w hashach...")
    hash_slownik = roznice_hashe(
        silnik=silnik,
        dozwolone_kolumny=hash_typ_lista()
    )

    misje = set()
    for hash_typ, lista_id in hash_slownik.items():
        print(f"  • {hash_typ}: {len(lista_id)} misji")
        misje.update(lista_id)

    print(f"\n▶ Łącznie misji do czyszczenia: {len(misje)}")
    print(f"▶ ID misji: {sorted(misje)}\n")

    for m in misje:
        print(f"=== Czyszczę MISJA_ID = {m} ===")

        tabele_do_skanowania = [
                    "dbo.MISJE_SLOWA_KLUCZOWE",
                    "dbo.DIALOGI_STATUSY",
                    "dbo.MISJE_STATUSY",
                    "dbo.ZRODLO",
                    "dbo.MISJE"
                ]

        q_select_url = text("""
            SELECT MISJA_URL_WIKI
            FROM dbo.MISJE
            WHERE MISJA_ID_MOJE_PK = :misja_id
        """)

        q_insert_url = text("""
            INSERT INTO dbo.LINKI_DO_SCRAPOWANIA (URL, ZRODLO_NAZWA)
            VALUES (:url, :zrodlo)
        """)

        with silnik.begin() as conn:
            misja_url = conn.execute(q_select_url, {"misja_id": m}).scalar_one()
            print(f"    + wrzucam do LINKI_DO_SCRAPOWANIA: {misja_url}")

            try:
                conn.execute(q_insert_url, {"url": misja_url, "zrodlo": zrodlo_insert_url})
            except IntegrityError as e:
                if _czy_duplikat(e):
                    print("      - już było w kolejce (duplikat)")
                else:
                    raise

            for tabela in tabele_do_skanowania:
                kolumna = "MISJA_ID_MOJE_PK" if tabela == "dbo.MISJE" else "MISJA_ID_MOJE_FK"

                q_delete_id = text(f"""
                    DELETE FROM {tabela}
                    WHERE {kolumna} = :m
                """)

                wynik_out = conn.execute(q_delete_id, {"m": m})
                print(f"    - {tabela}: usunięto {wynik_out.rowcount} wierszy")

        print(f"=== Zakończono MISJA_ID = {m} ===\n")

def hashuj_kategorie_i_zapisz_zrodlo(
        silnik,
        kategorie: list[str],
        zrodlo: str,
        sleep_s: int = 0,
        tabela_misje: str = "dbo.MISJE",
        tabela_zrodlo: str = "dbo.ZRODLO"
    ) -> None:

    q_select_misja_id = text(f"""
        SELECT MISJA_ID_MOJE_PK
        FROM {tabela_misje}
        WHERE MISJA_URL_WIKI = :url
    """)

    for kat_i, kat_url in enumerate(kategorie, start=1):
        print(f"\n=== KATEGORIA [{kat_i}/{len(kategorie)}]: {kat_url} ===")

        questy = wyscrapuj_linki_z_kategorii_z_paginacja(
            kategoria_url=kat_url,
            sleep_s=sleep_s
        )

        print(f"Znaleziono {len(questy)} questów")

        for i, url in enumerate(questy, start=1):
            print(f"  [{i}/{len(questy)}] Hashuję: {url}")

            try:
                wynik = parsuj_misje_z_url(url)
                misja_url = wynik.get("Źródło", {}).get("url")

                with silnik.connect() as conn:
                    row = conn.execute(q_select_misja_id, {"url": misja_url}).first()

                if not row:
                    print("    → brak w MISJE, dodaję do LINKI_DO_SCRAPOWANIA")
                    zapisz_link_do_scrapowania(
                        silnik=silnik,
                        url=misja_url,
                        zrodlo=zrodlo
                    )
                    time.sleep(sleep_s)
                    continue

                misja_id = row[0]

                zapisz_zrodlo_do_db(
                    silnik=silnik,
                    tabela_zrodlo=tabela_zrodlo,
                    misja_id=misja_id,
                    wynik=wynik,
                    zrodlo=zrodlo
                )

                print("    + zapisano hashe do ZRODLO")

            except Exception as e:
                print(f"    ! BŁĄD: {e}")

            time.sleep(sleep_s)


def aktualizuj_misje_z_excela(df, silnik, chunk_size=10_000):
    df = df.dropna(how="all").copy()
    df["NAZWA_LINII_FABULARNEJ_EN"] = df["NAZWA_LINII_FABULARNEJ_EN"].fillna("NoData")

    q = text("SELECT MISJA_TYTUL_EN FROM dbo.MISJE;")
    with silnik.connect() as conn:
        tytuly_db = {row[0] for row in conn.execute(q).all()}

    excel_total = len(df)
    df = df[df["MISJA_TYTUL_EN"].isin(tytuly_db)]
    match_total = len(df)

    if match_total == 0:
        print(f"UPDATE MISJE: 0 dopasowań (Excel={excel_total}, DB={len(tytuly_db)})")
        return

    u = text("""
    UPDATE dbo.MISJE
    SET MISJA_ID_Z_GRY            = :misja_id_z_gry,
        MISJA_URL_WOWHEAD         = :misja_url_wowhead,
        NAZWA_LINII_FABULARNEJ_EN = :nazwa_linii_fabularnej_en,
        KONTYNENT_EN              = :kontynent_en,
        KRAINA_EN                 = :kraina_en,
        DODATEK_EN                = :dodatek_en,
        KONTYNENT_PL              = :kontynent_pl,
        KRAINA_PL                 = :kraina_pl,
        DODATEK_PL                = :dodatek_pl,
        DODANO_W_PATCHU           = :dodano_w_patchu,
        DATA_UPDATE               = SYSDATETIME()
    WHERE MISJA_TYTUL_EN = :misja_tytul_en
    """)

    parametry = [
        {
            "misja_id_z_gry": int(r["MISJA_ID_Z_GRY"]) if pd.notna(r["MISJA_ID_Z_GRY"]) else None,
            "misja_url_wowhead": r["MISJA_URL_WOWHEAD"],
            "nazwa_linii_fabularnej_en": r["NAZWA_LINII_FABULARNEJ_EN"],
            "kontynent_en": r["KONTYNENT_EN"],
            "kraina_en": r["KRAINA_EN_FINAL"],
            "dodatek_en": r["DODATEK_EN"],
            "kontynent_pl": r["KONTYNENT_PL"],
            "kraina_pl": r["KRAINA_PL"],
            "dodatek_pl": r["DODATEK_PL"],
            "dodano_w_patchu": r["DODANO_W_PATCHU"],
            "misja_tytul_en": r["MISJA_TYTUL_EN"],
        }
        for r in df.to_dict("records")
    ]

    total = len(parametry)
    chunks = (total + chunk_size - 1) // chunk_size

    with silnik.begin() as conn:
        for i in range(0, total, chunk_size):
            conn.execute(u, parametry[i:i + chunk_size])

    print(f"UPDATE MISJE: Excel={excel_total}, dopasowane_do_DB={match_total}, wysłane={total}, batche={chunks}")

def zaladuj_api_i_klienta(
        nazwa_api: str
    ):
    load_dotenv()
    API_KEY = os.environ.get(nazwa_api)

    if not API_KEY:
        raise ValueError("BRAK KLUCZA!")
    else:
        print("KLUCZ ZWARTY I GOTOWY!")
        return genai.Client(api_key=API_KEY)

def pobierz_przetworz_zapisz_batch_lista(
        silnik, 
        lista_id_batch, 
        nazwa_dodatku,
        folder_zapisz: str = r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\surowe\slowa_kluczowe_batche"
    ):
    
    min_b = min(lista_id_batch)
    max_b = max(lista_id_batch)
    nazwa_pliku = f"batch_{min_b}_{max_b}.csv"
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
    """).bindparams(bindparam('lista_id', expanding=True))

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

    instrukcja = """
    You are an expert World of Warcraft Translator and Lorewalker.
    Analyze the provided quest texts.
    
    TASK:
    1. Extract Proper Nouns (Names, Locations, Organizations, Items).
    2. Provide a Polish translation for each extracted term based on WoW context.
       - Names (Jaina): Keep original or standard Polish equivalent if exists.
       - Items/Objects (Twilight's Blade): Translate to Polish (Ostrze Zmierzchu).
       - Locations (Stormwind): Use official PL localization (Wichrogród) or keep English if untranslatable.
    3. Assign a Category: NPC, LOCATION, ITEM, ORG, OTHER.

    CRITICAL OUTPUT RULES:
    - Return ONLY a JSON list of objects.
    - Structure:
      [
        {
          "quest_id": 123,
          "extracted": [
             {"en": "Jaina Proudmoore", "pl": "Jaina Proudmoore", "type": "NPC"},
             {"en": "Dalaran", "pl": "Dalaran", "type": "LOCATION"},
             {"en": "Strange Key", "pl": "Dziwny Klucz", "type": "ITEM"}
          ]
        }
      ]
    - Return "extracted": [] if nothing found.
    - Do not skip any Quest ID.
    """

    try:
        odpowiedz = klient.models.generate_content(
            model="gemini-3-flash-preview",
            contents=json.dumps(wsad_dla_geminisia),
            config={
                "system_instruction": instrukcja,
                "response_mime_type": "application/json"
            }
        )
        
        wynik_lista = json.loads(odpowiedz.text)

        df = pd.DataFrame(wynik_lista)
        df_exploded = df.explode("extracted")
        df_exploded = df_exploded.dropna(subset=["extracted"])

        if df_exploded.empty:
            print(f"Batch {min_b}-{max_b} przetworzony, ale nie znaleziono słów kluczowych.")
            return None

        dane_szczegolowe = df_exploded["extracted"].apply(pd.Series)
        
        df_final = pd.concat([df_exploded["quest_id"], dane_szczegolowe], axis=1)
        df_final.to_csv(pelna_sciezka, index=False, encoding="utf-8-sig", sep=";")
        
        print(f"Zapisano: {nazwa_pliku} (Ilość wierszy: {len(df_final)})")
        time.sleep(2) 
        return pelna_sciezka
                
    except Exception as e:
        print(f"Błąd w batchu {min_b}-{max_b}: {e}")
        return None
    

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


def slowa_kluczowe_do_db(
    plik_do_otwarcia=r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\slowa_kluczowe.xlsx",
    silnik=utworz_engine_do_db()
):

    plik_slowa_kluczowe = pd.read_excel(
        plik_do_otwarcia, 
        sheet_name="do_tabeli_slowa_kluczowe", 
        usecols=["SLOWO_EN", "SLOWO_PL", "KATEGORIA"]
    )
    
    with silnik.begin() as conn:
        q_select_sk = text("SELECT SLOWO_EN, SLOWO_PL FROM dbo.SLOWA_KLUCZOWE")
        
        q_insert_sk = text("""
            INSERT INTO dbo.SLOWA_KLUCZOWE (SLOWO_EN, SLOWO_PL, KATEGORIA)
            VALUES (:slowo_en, :slowo_pl, :kategoria)
        """)
        
        q_update_sk = text("""
            UPDATE dbo.SLOWA_KLUCZOWE
            SET SLOWO_PL = :slowo_pl
            WHERE SLOWO_EN = :slowo_en
        """)

        db_records = conn.execute(q_select_sk).all()
        
        mapa_db = {row[0]: row[1] for row in db_records}
        zestaw_en_db = set(mapa_db.keys())

        lista_do_wstawienia = []
        lista_do_aktualizacji = []

        for slowo_en, slowo_pl, kategoria in plik_slowa_kluczowe.values:
            if slowo_en not in zestaw_en_db:
                lista_do_wstawienia.append({
                    "slowo_en": slowo_en, 
                    "slowo_pl": slowo_pl, 
                    "kategoria": kategoria
                })
            elif mapa_db[slowo_en] != slowo_pl:
                lista_do_aktualizacji.append({
                    "slowo_en": slowo_en, 
                    "slowo_pl": slowo_pl
                })

        if lista_do_wstawienia:
            print(f"Wykryto {len(lista_do_wstawienia)} nowych słów. Dodaję...")
            conn.execute(q_insert_sk, lista_do_wstawienia)
            print("Zakończono dodawanie.")
        else:
            print("Brak nowych słów do dodania.")

        if lista_do_aktualizacji:
            print(f"Wykryto {len(lista_do_aktualizacji)} zmienionych tłumaczeń. Aktualizuję...")
            conn.execute(q_update_sk, lista_do_aktualizacji)
            print("Zakończono aktualizację.")
        else:
            print("Brak tłumaczeń wymagających aktualizacji.")


def mapowanie_misji_do_db(
    plik_do_otwarcia=r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\slowa_kluczowe.xlsx",
    silnik=utworz_engine_do_db()
):

    plik_lacznika = pd.read_excel(
        plik_do_otwarcia,
        sheet_name="do_tabeli_misje_slowa_kluczowe",
        usecols=["MISJA_ID_MOJE_FK", "SLOWO_EN"]
    )

    with silnik.begin() as conn:
        q_mapowanie_slow = text("SELECT SLOWO_EN, SLOWO_ID_PK " \
        "                        FROM dbo.SLOWA_KLUCZOWE")
        q_dostepne_misje = text("SELECT MISJA_ID_MOJE_PK " \
        "                        FROM dbo.MISJE")
        q_select_lacznik = text("SELECT MISJA_ID_MOJE_FK, SLOWO_ID " \
        "                        FROM dbo.MISJE_SLOWA_KLUCZOWE")
        
        q_insert_lacznik = text("""
            INSERT INTO dbo.MISJE_SLOWA_KLUCZOWE (MISJA_ID_MOJE_FK, SLOWO_ID)
            VALUES (:misja_id, :slowo_id)
        """)
        
        mapa_slow_sql = dict(conn.execute(q_mapowanie_slow).all())
        dostepne_misje_sql = set(conn.execute(q_dostepne_misje).scalars().all())
        istniejace_pary_sql = set(conn.execute(q_select_lacznik).all())

        plik_lacznika["SLOWO_ID"] = plik_lacznika["SLOWO_EN"].map(mapa_slow_sql)
        
        odrzucone_brak_slowa = plik_lacznika[plik_lacznika["SLOWO_ID"].isna()].copy()
        odrzucone_brak_slowa["PRZYCZYNA"] = "Brak słowa w DB"
        
        plik_lacznika = plik_lacznika.dropna(subset=["SLOWO_ID"])
        plik_lacznika["SLOWO_ID"] = plik_lacznika["SLOWO_ID"].astype("int64")

        # Walidacja Misji
        maska_poprawne_misje = plik_lacznika["MISJA_ID_MOJE_FK"].isin(dostepne_misje_sql)
        odrzucone_brak_misji = plik_lacznika[~maska_poprawne_misje].copy()
        odrzucone_brak_misji["PRZYCZYNA"] = "Brak ID misji w DB"

        plik_lacznika = plik_lacznika[maska_poprawne_misje]

        # Walidacja Duplikatów
        pary_z_excela = set(zip(plik_lacznika["MISJA_ID_MOJE_FK"], plik_lacznika["SLOWO_ID"]))
        
        do_wgrania_set = pary_z_excela - istniejace_pary_sql
        juz_w_bazie_set = pary_z_excela.intersection(istniejace_pary_sql)

        maska_juz_w_bazie = plik_lacznika[["MISJA_ID_MOJE_FK", "SLOWO_ID"]].apply(tuple, axis=1).isin(juz_w_bazie_set)
        odrzucone_duplikaty_db = plik_lacznika[maska_juz_w_bazie].copy()
        odrzucone_duplikaty_db["PRZYCZYNA"] = "Relacja już istnieje w DB"

        dane_do_insertu = [
            {"misja_id": m_id, "slowo_id": s_id}
            for m_id, s_id in do_wgrania_set
        ]

        if dane_do_insertu:
            print(f"Rozpoczynam dodawanie {len(dane_do_insertu)} nowych powiązań...")
            conn.execute(q_insert_lacznik, dane_do_insertu)
            print(f"✅ Sukces: Dodano {len(dane_do_insertu)} powiązań.")
        else:
            print("Brak nowych powiązań do dodania.")

    # Raportowanie
    raport_odrzuconych = pd.concat([
        odrzucone_brak_slowa,
        odrzucone_brak_misji,
        odrzucone_duplikaty_db
    ], ignore_index=True)

    if not raport_odrzuconych.empty:
        print("\n⚠️  Raport odrzuconych rekordów:")
        grupy = raport_odrzuconych.groupby("PRZYCZYNA")
        for przyczyna, grupa in grupy:
            print(f"🔴 {przyczyna}: {len(grupa)} szt.")
            if "Brak" in przyczyna:
                kolumna_info = "SLOWO_EN" if "słowa" in przyczyna else "MISJA_ID_MOJE_FK"
                print(f"   Przykłady: {grupa[kolumna_info].unique()}")
    else:
        print("\n🎉 Wszystkie rekordy z Excela są poprawne i trafiły do bazy (lub już tam były).")


def zapisz_npc_i_status_przetlumaczony_do_db(
    silnik, 
    plik_do_otwarcia=r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\npc.xlsx",
    rozmiar_partii=10000
):
    status_docelowy = "3_ZATWIERDZONO"

    df = pd.read_excel(
        plik_do_otwarcia, 
        sheet_name="surowe", 
        usecols=["NPC_ID_MOJE_PK", "NAZWA", "NAZWA_PL_FINAL", "PLEC", "RASA", "KLASA", "TYTUL"]
    )

    q_pobierz_juz_zatwierdzone = text("""
        SELECT NPC_ID_FK 
        FROM dbo.NPC_STATUSY 
        WHERE STATUS = :status
    """)

    with silnik.connect() as conn:
        df_istniejace = pd.read_sql(q_pobierz_juz_zatwierdzone, conn, params={"status": status_docelowy})

    zestaw_juz_zatwierdzonych_id = set(df_istniejace["NPC_ID_FK"].astype(int))

    maska_juz_sa_zatwierdzone = df["NPC_ID_MOJE_PK"].isin(zestaw_juz_zatwierdzonych_id)
    
    df_do_update_statusy = df[maska_juz_sa_zatwierdzone]
    df_do_insert_statusy = df[~maska_juz_sa_zatwierdzone]

    q_insert_statusy = text("""
        INSERT INTO dbo.NPC_STATUSY (NPC_ID_FK, STATUS, NAZWA)
        VALUES (:npc_id, :status, :nazwa_pl)
    """)

    q_update_statusy = text("""
        UPDATE dbo.NPC_STATUSY
        SET NAZWA = :nazwa_pl
        WHERE NPC_ID_FK = :npc_id 
          AND STATUS = :status
    """)

    q_update_npc_glowne = text("""
        UPDATE dbo.NPC
        SET PLEC = :plec, 
            RASA = :rasa,
            KLASA = :klasa,
            TYTUL = :tytul
        WHERE NAZWA = :nazwa_eng
    """)

    parametry_insert_statusy = [
        {
            "npc_id": int(r["NPC_ID_MOJE_PK"]),
            "status": status_docelowy,
            "nazwa_pl": r["NAZWA_PL_FINAL"]
        }
        for r in df_do_insert_statusy.to_dict("records")
    ]

    parametry_update_statusy = [
        {
            "npc_id": int(r["NPC_ID_MOJE_PK"]),
            "status": status_docelowy,
            "nazwa_pl": r["NAZWA_PL_FINAL"]
        }
        for r in df_do_update_statusy.to_dict("records")
    ]

    df_do_aktualizacji_npc = df[df["PLEC"].notna() & (df["PLEC"] != "")]
    parametry_update_npc_glowne = [
        {
            "nazwa_eng": r["NAZWA"],
            "plec": r["PLEC"],
            "rasa": r["RASA"],
            "klasa": r["KLASA"],
            "tytul": r["TYTUL"]
        }
        for r in df_do_aktualizacji_npc.to_dict("records")
    ]

    l_ins_stat = len(parametry_insert_statusy)
    l_upd_stat = len(parametry_update_statusy)
    l_upd_npc = len(parametry_update_npc_glowne)

    with silnik.begin() as conn:
        for i in range(0, l_ins_stat, rozmiar_partii):
            conn.execute(q_insert_statusy, parametry_insert_statusy[i:i + rozmiar_partii])
            
        for i in range(0, l_upd_stat, rozmiar_partii):
            conn.execute(q_update_statusy, parametry_update_statusy[i:i + rozmiar_partii])

        for i in range(0, l_upd_npc, rozmiar_partii):
            conn.execute(q_update_npc_glowne, parametry_update_npc_glowne[i:i + rozmiar_partii])

    print(f"PROCES ZAKONCZONY. Excel={len(df)}")
    print(f"Tabela STATUSY -> INSERT (Nowe wiersze): {l_ins_stat}")
    print(f"Tabela STATUSY -> UPDATE (Istniejące zatwierdzone): {l_upd_stat}")
    print(f"Tabela NPC -> UPDATE po nazwie: {l_upd_npc}")