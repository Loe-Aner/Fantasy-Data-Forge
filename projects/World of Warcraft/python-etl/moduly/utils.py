from sqlalchemy import text
import pandas as pd

def generuj_hash_djb2(tekst):

    tekst = str(tekst)
    
    if not tekst:
        return None
    
    tekst = tekst.lower()

    hash_val = 5381
    hash_val_2 = 0
    
    for znak in tekst:
        kod_znaku = ord(znak)
        
        hash_val = (hash_val * 33 + kod_znaku) & 0xFFFFFFFF
        hash_val_2 = (hash_val_2 * 65599 + kod_znaku) & 0xFFFFFFFF
        
    return f"{hash_val:08x}{hash_val_2:08x}"

# if __name__ == "__main__":
#     przyklady = [
#         ("Apocalyptic threats have taken many forms in Azeroth's history, but today we face Xal'atath and her Twilight's Blade.", "6b7431ab582fdb80")
#     ]

#     print("\n" + "=" * 50)
#     print(f"{'HASH Z LUA':<18} | {'HASH Z PYTHONA':<18} | {'WYNIK'}")
#     print("=" * 50)

#     for tekst, hash_lua in przyklady:
#         hash_python = generuj_hash_djb2(tekst)
        
#         if hash_python == hash_lua:
#             czy_zgodne = "OK"
#         else:
#             czy_zgodne = "BŁĄD"
            
#         print(f"{hash_lua:<18} | {hash_python:<18} | {czy_zgodne}")

#     print("=" * 50 + "\n")

def sklej_warunki_w_WHERE(
    kraina: str | None = None, 
    fabula: str | None = None, 
    dodatek: str | None = None,
    id_misji: int | None = None
):
    if id_misji is not None:
        return "AND m.MISJA_ID_MOJE_PK = :id_misji"

    czesci_warunku = []
    
    if kraina is not None:
        czesci_warunku.append("AND m.KRAINA_EN = :kraina_en")
        
    if fabula is not None:
        czesci_warunku.append("AND m.NAZWA_LINII_FABULARNEJ_EN = :fabula_en")

    if dodatek is not None:
        czesci_warunku.append("AND m.DODATEK_EN = :dodatek_en")
    
    if czesci_warunku:
        return "\n        ".join(czesci_warunku)

    raise ValueError("Nie podano żadnych parametrów filtrowania (ID, Kraina, Fabuła lub Dodatek).")

def usun_stare_daty_pokaz_zmiany(silnik):
    """
    Zostawia tylko dwie najnowsze daty.
    Pokazuje dataframe ze zmianami, tzn. w której grupie zostały dodane/usunięte misje.
    """
    q_delete_stare = text("""
    WITH UNIKALNE_DATY AS (
        SELECT DISTINCT
            DATA_STATUS
        FROM dbo.MISJE_ZMIANY_WIKI
    ),

    RANKING_DAT AS (
        SELECT
            DATA_STATUS,
            ROW_NUMBER() OVER (ORDER BY DATA_STATUS DESC) AS RNK
        FROM UNIKALNE_DATY
    ),

    DO_WYRZUCENIA AS (
        SELECT
            DATA_STATUS
        FROM RANKING_DAT
        WHERE RNK >= 3
    )

    DELETE FROM dbo.MISJE_ZMIANY_WIKI
    WHERE DATA_STATUS IN (
        SELECT DATA_STATUS
        FROM DO_WYRZUCENIA
    );
    """)

    q_select_roznice = text("""
    WITH UNIKALNE_DATY AS (
        SELECT DISTINCT
            DATA_STATUS
        FROM dbo.MISJE_ZMIANY_WIKI
    ),

    RANKING_DAT AS (
        SELECT
            DATA_STATUS,
            ROW_NUMBER() OVER (ORDER BY DATA_STATUS DESC) AS RNK
        FROM UNIKALNE_DATY
    ),

    DANE AS (
        SELECT
            MZW.ZAKRES,
            MZW.LICZBA_MISJI,
            MZW.DATA_STATUS,
            RD.RNK
        FROM dbo.MISJE_ZMIANY_WIKI AS MZW
        INNER JOIN RANKING_DAT AS RD
            ON MZW.DATA_STATUS = RD.DATA_STATUS
        WHERE RD.RNK IN (1, 2)
    ),

    WYNIK AS (
        SELECT
            ZAKRES,
            MAX(CASE WHEN RNK = 2 THEN DATA_STATUS END) AS DATA_POPRZEDNIA,
            MAX(CASE WHEN RNK = 1 THEN DATA_STATUS END) AS DATA_NAJNOWSZA,
            MAX(CASE WHEN RNK = 2 THEN LICZBA_MISJI END) AS LICZBA_MISJI_POPRZEDNIA,
            MAX(CASE WHEN RNK = 1 THEN LICZBA_MISJI END) AS LICZBA_MISJI_NAJNOWSZA,
            MAX(CASE WHEN RNK = 1 THEN LICZBA_MISJI END)
                - MAX(CASE WHEN RNK = 2 THEN LICZBA_MISJI END) AS ROZNICA
        FROM DANE
        GROUP BY ZAKRES
    )

    SELECT
        ZAKRES,
        DATA_POPRZEDNIA,
        DATA_NAJNOWSZA,
        LICZBA_MISJI_POPRZEDNIA,
        LICZBA_MISJI_NAJNOWSZA,
        ROZNICA
    FROM WYNIK
    WHERE ROZNICA <> 0
    ORDER BY ZAKRES;
    """)

    try:
        with silnik.begin() as conn:
            conn.execute(q_delete_stare)
            wynik = pd.read_sql_query(q_select_roznice, conn)
        return wynik

    except Exception as e:
        print(f"--- Błąd podczas odczytywania danych: {e}")
        return pd.DataFrame()