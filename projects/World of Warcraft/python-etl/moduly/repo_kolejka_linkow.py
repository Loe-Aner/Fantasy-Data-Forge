from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from moduly.db_core import _czy_duplikat

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