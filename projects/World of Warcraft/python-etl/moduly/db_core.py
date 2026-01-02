from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine

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