from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from moduly.db_core import _czy_duplikat

def zapisz_link_do_scrapowania(
    silnik,
    url: str,
    zrodlo: str,
    html_skompresowany: str | None = None
) -> None:
    q = text("""
        INSERT INTO dbo.LINKI_DO_SCRAPOWANIA (URL, ZRODLO_NAZWA, HTML_SKOMPRESOWANY)
        VALUES (:url, :zrodlo, :html)
    """)

    try:
        with silnik.begin() as conn:
            conn.execute(q, {
                "url": url, 
                "zrodlo": zrodlo,
                "html": html_skompresowany
            })
    except IntegrityError as e:
        if _czy_duplikat(e):
            return
        raise

def pobierz_linki_do_scrapowania(silnik) -> list[dict]:
    q = text("""
        SELECT URL, HTML_SKOMPRESOWANY
        FROM dbo.LINKI_DO_SCRAPOWANIA
        WHERE ZRODLO_NAZWA = N'wiki'
        ORDER BY TECH_ID
    """)

    wynik = []
    with silnik.connect() as conn:
        rows = conn.execute(q).all()
        for row in rows:
            wynik.append({
                "url": row[0],
                "html_skompresowany": row[1]
            })
            
    return wynik

def usun_link_z_kolejki(silnik, url: str) -> None:
    q = text("""
        DELETE FROM dbo.LINKI_DO_SCRAPOWANIA
        WHERE URL = :url
    """)
    with silnik.begin() as conn:
        conn.execute(q, {"url": url})