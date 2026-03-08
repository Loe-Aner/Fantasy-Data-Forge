import os
import time

from dotenv import load_dotenv
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
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


KODY_BLEDOW_AZURE_PRZEJSCIOWE = (
    "40613",
    "40197",
    "40501",
    "49918",
    "49919",
    "49920",
    "10928",
    "10929",
)

FRAZY_BLEDOW_AZURE_PRZEJSCIOWE = (
    "database is not currently available",
    "please retry the connection later",
    "service is currently busy",
)


def czy_azure_blad_przejsciowy(blad: Exception) -> bool:
    fragmenty = []

    for zrodlo in (blad, getattr(blad, "orig", None)):
        if not zrodlo:
            continue

        fragmenty.append(str(zrodlo))

        argumenty = getattr(zrodlo, "args", [])
        fragmenty.extend(str(argument) for argument in argumenty if argument is not None)

    opis_bledu = " | ".join(fragmenty).lower()

    if any(kod in opis_bledu for kod in KODY_BLEDOW_AZURE_PRZEJSCIOWE):
        return True

    return any(fraza in opis_bledu for fraza in FRAZY_BLEDOW_AZURE_PRZEJSCIOWE)


def dodaj_auto_wybudzanie_azure(
    silnik: Engine,
    tryb: str,
    zapytanie_wybudzajace: str,
    opoznienie_s: int = 10,
    maks_liczba_prob: int | None = None
) -> Engine:
    if tryb != "azure":
        return silnik

    oryginalne_connect = silnik.connect

    def connect_z_wybudzeniem(*args, **kwargs):
        proba = 0

        while True:
            proba += 1

            try:
                conn = oryginalne_connect(*args, **kwargs)

                try:
                    conn.exec_driver_sql(zapytanie_wybudzajace)
                    if conn.in_transaction():
                        conn.rollback()
                    return conn
                except (OperationalError, DBAPIError):
                    conn.close()
                    raise
                except Exception:
                    conn.close()
                    raise

            except (OperationalError, DBAPIError) as e:
                if not czy_azure_blad_przejsciowy(e):
                    raise

                if maks_liczba_prob is not None and proba >= maks_liczba_prob:
                    raise

                print(
                    f"--- Azure SQL jeszcze śpi... "
                    f"Próba {proba} nieudana. "
                    f"Kolejna próba za {opoznienie_s} s.\n{e}"
                )
                time.sleep(opoznienie_s)

    silnik.connect = connect_z_wybudzeniem
    return silnik


def utworz_engine_do_db(
    tryb: str | None = None,
    sterownik: str | None = None,
    uzytkownik: str | None = None,
    haslo: str | None = None,
    host: str | None = None,
    port: int | None = None,
    nazwa_db: str | None = None,
    dbapi: dict | None = None,
    zapytanie_wybudzajace: str = "SELECT TOP 1 MISJA_ID_MOJE_FK FROM dbo.MISJE_STATUSY",
    opoznienie_wybudzania_s: int = 10,
    maks_liczba_prob_wybudzania: int | None = None,
    timeout_polaczenia_s: int | None = None
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
            "driver": pobierz_pierwsza_wartosc(
                pobierz_wartosc_env("ODBC_DRIVER", tryb),
                "ODBC Driver 18 for SQL Server"
            )
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

    connect_args = {}
    if timeout_polaczenia_s is None:
        timeout_polaczenia_s = 60 if tryb == "azure" else None

    if timeout_polaczenia_s is not None:
        connect_args["timeout"] = int(timeout_polaczenia_s)

    silnik = create_engine(
        polaczenie,
        echo=False,
        future=True,
        pool_pre_ping=(tryb == "azure"),
        connect_args=connect_args
    )

    silnik = dodaj_auto_wybudzanie_azure(
        silnik=silnik,
        tryb=tryb,
        zapytanie_wybudzajace=zapytanie_wybudzajace,
        opoznienie_s=opoznienie_wybudzania_s,
        maks_liczba_prob=maks_liczba_prob_wybudzania
    )

    return silnik


def _czy_duplikat(e: IntegrityError) -> bool:
    if not e.orig:
        return False
    argumenty = getattr(e.orig, "args", [])
    return any(kod in str(arg) for arg in argumenty for kod in ("2627", "2601"))
