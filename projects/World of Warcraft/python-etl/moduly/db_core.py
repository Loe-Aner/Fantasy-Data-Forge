import os

from dotenv import load_dotenv
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine

load_dotenv()

def pobierz_pierwsza_wartosc(*wartosci):
    for wartosc in wartosci:
        if wartosc not in (None, ""):
            return wartosc
    return None

def pobierz_wartosc_env(nazwa: str, tryb: str) -> str | None:
    klucze = []

    if tryb in {"lokalnie", "local"}:
        klucze.append(f"DB_LOCAL_{nazwa}")
    elif tryb == "azure":
        klucze.append(f"DB_AZURE_{nazwa}")

    klucze.append(f"DB_{nazwa}")

    for klucz in klucze:
        wartosc = os.getenv(klucz)
        if wartosc not in (None, ""):
            return wartosc

    return None

def normalizuj_bool_na_yes_no(wartosc) -> str | None:
    if wartosc in (None, ""):
        return None

    if isinstance(wartosc, bool):
        return "yes" if wartosc else "no"

    wartosc_znormalizowana = str(wartosc).strip().lower()

    if wartosc_znormalizowana in {"1", "true", "yes", "y", "on"}:
        return "yes"

    if wartosc_znormalizowana in {"0", "false", "no", "n", "off"}:
        return "no"

    return str(wartosc)

def utworz_engine_do_db(
        tryb: str | None = None,
        sterownik: str | None = None,
        uzytkownik: str | None = None,
        haslo: str | None = None,
        host: str | None = None,
        port: int | None = None,
        nazwa_db: str | None = None,
        dbapi: dict | None = None
) -> Engine:

    tryb = str(pobierz_pierwsza_wartosc(tryb, os.getenv("DB_TARGET"), "azure")).strip().lower()

    if tryb not in {"lokalnie", "local", "azure"}:
        raise ValueError("Nieznany tryb bazy. Uzyj \"lokalnie\" albo \"azure\".")

    host = pobierz_pierwsza_wartosc(
        host,
        pobierz_wartosc_env("HOST", tryb),
        "localhost" if tryb in {"lokalnie", "local"} else None
    )

    if tryb == "azure" and not host:
        raise ValueError("Brak hosta dla Azure. Ustaw DB_AZURE_HOST albo DB_HOST.")

    sterownik = pobierz_pierwsza_wartosc(sterownik, pobierz_wartosc_env("DRIVERNAME", tryb), "mssql+pyodbc")
    uzytkownik = pobierz_pierwsza_wartosc(uzytkownik, pobierz_wartosc_env("USER", tryb))
    haslo = pobierz_pierwsza_wartosc(haslo, pobierz_wartosc_env("PASSWORD", tryb))
    nazwa_db = pobierz_pierwsza_wartosc(nazwa_db, pobierz_wartosc_env("NAME", tryb), "WoW_PL")

    port_env = pobierz_pierwsza_wartosc(port, pobierz_wartosc_env("PORT", tryb))
    port = int(port_env) if port_env not in (None, "") else None

    if dbapi is None:
        lokalne_hosty = {"localhost", "127.0.0.1", ".", "(local)"}
        czy_polaczenie_lokalne = str(host).strip().lower() in lokalne_hosty

        dbapi = {
            "driver": pobierz_pierwsza_wartosc(pobierz_wartosc_env("ODBC_DRIVER", tryb), "ODBC Driver 18 for SQL Server")
        }

        zaufane = normalizuj_bool_na_yes_no(pobierz_wartosc_env("TRUSTED_CONNECTION", tryb))
        if zaufane is not None:
            dbapi["Trusted_Connection"] = zaufane
        elif not uzytkownik and not haslo:
            dbapi["Trusted_Connection"] = "yes"

        szyfrowanie = normalizuj_bool_na_yes_no(pobierz_wartosc_env("ENCRYPT", tryb))
        dbapi["Encrypt"] = szyfrowanie if szyfrowanie else ("yes" if not czy_polaczenie_lokalne else "no")

        certyfikat = normalizuj_bool_na_yes_no(pobierz_wartosc_env("TRUST_SERVER_CERTIFICATE", tryb))
        dbapi["TrustServerCertificate"] = certyfikat if certyfikat else ("yes" if czy_polaczenie_lokalne else "no")

    polaczenie = URL.create(
        drivername=sterownik,
        username=uzytkownik,
        password=haslo,
        host=host,
        port=port,
        database=nazwa_db,
        query=dbapi
    )

    return create_engine(polaczenie, echo=False, future=True)

def _czy_duplikat(e: IntegrityError) -> bool:
    if not e.orig:
        return False
    argumenty = getattr(e.orig, "args", [])
    return any(kod in str(arg) for arg in argumenty for kod in ("2627", "2601"))
