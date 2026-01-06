from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from moduly.db_core import _czy_duplikat

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

    q_select_data = text("""
        SELECT m.MISJA_URL_WIKI, z.HTML_SKOMPRESOWANY
        FROM dbo.MISJE m
        LEFT JOIN dbo.ZRODLO z ON z.MISJA_ID_MOJE_FK = m.MISJA_ID_MOJE_PK
        WHERE m.MISJA_ID_MOJE_PK = :misja_id
        ORDER BY z.DATA_WYSCRAPOWANIA DESC
    """)

    for m in misje:
        print(f"=== Czyszczę MISJA_ID = {m} ===")

        tabele_do_skanowania = [
                    "dbo.MISJE_SLOWA_KLUCZOWE",
                    "dbo.DIALOGI_STATUSY",
                    "dbo.MISJE_STATUSY",
                    "dbo.ZRODLO",
                    "dbo.MISJE"
                ]

        with silnik.begin() as conn:
            row = conn.execute(q_select_data, {"misja_id": m}).first()
            if not row:
                print(f"    ! Brak misji ID={m} w bazie, pomijam.")
                continue
            
            misja_url = row[0]
            html_blob = row[1]

            print(f"    + Przenoszę do kolejki: {misja_url} (Cache: {'TAK' if html_blob else 'NIE'})")

            q_insert_kolejka = text("""
                INSERT INTO dbo.LINKI_DO_SCRAPOWANIA (URL, ZRODLO_NAZWA, HTML_SKOMPRESOWANY)
                VALUES (:url, :zrodlo, :html)
            """)
            
            try:
                conn.execute(q_insert_kolejka, {
                    "url": misja_url, 
                    "zrodlo": zrodlo_insert_url,
                    "html": html_blob
                })
            except IntegrityError as e:
                if _czy_duplikat(e):
                    print("      - już było w kolejce (duplikat)")
                else:
                    raise

            for tabela in tabele_do_skanowania:
                kolumna = "MISJA_ID_MOJE_PK" if tabela == "dbo.MISJE" else "MISJA_ID_MOJE_FK"
                q_delete_id = text(f"DELETE FROM {tabela} WHERE {kolumna} = :m")
                wynik_out = conn.execute(q_delete_id, {"m": m})
                print(f"    - {tabela}: usunięto {wynik_out.rowcount} wierszy")

        print(f"=== Zakończono MISJA_ID = {m} ===\n")