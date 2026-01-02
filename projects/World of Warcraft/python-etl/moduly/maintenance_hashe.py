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