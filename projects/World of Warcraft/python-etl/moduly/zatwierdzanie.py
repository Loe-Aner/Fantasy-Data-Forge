from sqlalchemy import text, bindparam
from sqlalchemy.exc import IntegrityError, DBAPIError, SQLAlchemyError
import pandas as pd

from moduly.utils import sklej_warunki_w_WHERE
from moduly.utils import generuj_hash_djb2

def pobierz_wiersze_archiwum_do_excela(conn, misje):
    if not misje:
        return pd.DataFrame(columns=[
            "MISJA_ID", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "NR_WYP", "TRESC"
        ])

    q_select_archiwum = text("""
        WITH archiwum AS (
            SELECT
                m.MISJA_ID_MOJE_PK AS MISJA_ID,
                CASE
                    WHEN a.SEGMENT = N'TYTUŁ' THEN N'TYTUL'
                    ELSE a.SEGMENT
                END AS SEGMENT,
                COALESCE(a.PODSEGMENT, N'') AS PODSEGMENT,
                a.NR AS NR_BLOKU,
                COALESCE(a.NR_WYPOWIEDZI, 1) AS NR_WYP,
                a.TRESC,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        m.MISJA_ID_MOJE_PK,
                        a.TABELA,
                        a.SEGMENT,
                        COALESCE(a.PODSEGMENT, N''),
                        a.NR,
                        COALESCE(a.NR_WYPOWIEDZI, 1)
                    ORDER BY
                        CASE
                            WHEN a.STATUS = N'3_ZATWIERDZONO' THEN 0
                            WHEN a.STATUS = N'2_ZREDAGOWANO' THEN 1
                            ELSE 2
                        END,
                        a.DATA_ARCHIWIZACJI DESC,
                        a.TECH_ID DESC
                ) AS rnk
            FROM dbo.ARCHIWUM_MISJE_DIALOGI AS a
            INNER JOIN dbo.MISJE AS m
              ON a.MISJA_ID_Z_GRY = m.MISJA_ID_Z_GRY
            WHERE 1=1
              AND m.MISJA_ID_MOJE_PK IN :misje_id
              AND m.MISJA_ID_Z_GRY IS NOT NULL
              AND m.MISJA_ID_Z_GRY <> 123456789
              AND a.STATUS IN (N'3_ZATWIERDZONO', N'2_ZREDAGOWANO')
        )
        SELECT
            MISJA_ID,
            SEGMENT,
            PODSEGMENT,
            NR_BLOKU,
            NR_WYP,
            TRESC
        FROM archiwum
        WHERE rnk = 1
        ;
    """).bindparams(bindparam("misje_id", expanding=True))

    archiwum = conn.execute(q_select_archiwum, {"misje_id": misje}).mappings().all()
    return pd.DataFrame(archiwum)

def zbuduj_wiersze_archiwum(df_wzorzec, df_archiwum):
    if df_wzorzec.empty:
        return pd.DataFrame(columns=[
            "MISJA_ID", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "NR_WYP",
            "STATUS", "TRESC", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"
        ])

    df_wynik = (
        df_wzorzec.copy()
        .assign(STATUS="4_ARCHIWUM", TRESC="")
    )

    if df_archiwum.empty:
        return df_wynik

    df_wynik = df_wynik.assign(
        PODSEGMENT_KLUCZ=lambda x: x["PODSEGMENT"].fillna(""),
        NR_BLOKU_KLUCZ=lambda x: pd.to_numeric(x["NR_BLOKU"], errors="coerce"),
        NR_WYP_KLUCZ=lambda x: pd.to_numeric(x["NR_WYP"], errors="coerce").fillna(1)
    )

    df_archiwum = (
        df_archiwum.copy()
        .assign(
            PODSEGMENT_KLUCZ=lambda x: x["PODSEGMENT"].fillna(""),
            NR_BLOKU_KLUCZ=lambda x: pd.to_numeric(x["NR_BLOKU"], errors="coerce"),
            NR_WYP_KLUCZ=lambda x: pd.to_numeric(x["NR_WYP"], errors="coerce").fillna(1)
        )
        .rename(columns={"TRESC": "TRESC_ARCHIWUM"})
    )

    df_wynik = (
        df_wynik
        .merge(
            df_archiwum[[
                "MISJA_ID", "SEGMENT", "PODSEGMENT_KLUCZ",
                "NR_BLOKU_KLUCZ", "NR_WYP_KLUCZ", "TRESC_ARCHIWUM"
            ]],
            on=["MISJA_ID", "SEGMENT", "PODSEGMENT_KLUCZ", "NR_BLOKU_KLUCZ", "NR_WYP_KLUCZ"],
            how="left"
        )
        .assign(TRESC=lambda x: x["TRESC_ARCHIWUM"].fillna(""))
        .drop(columns=["PODSEGMENT_KLUCZ", "NR_BLOKU_KLUCZ", "NR_WYP_KLUCZ", "TRESC_ARCHIWUM"])
    )

    return df_wynik

def stworz_excele_do_zatwierdzenia_tlumaczen(silnik, kraina = None, fabula = None, dodatek = None, sciezka = None):

    warunki_sql = sklej_warunki_w_WHERE(kraina, fabula, dodatek)

    q_select_misje_id = text(f"""
        SELECT m.MISJA_ID_MOJE_PK
        FROM dbo.MISJE AS m
        WHERE 1=1
          AND m.MISJA_ID_MOJE_PK <> 123456789
          AND (
            m.WSKAZNIK_ZGODNOSCI <= 0.70000
            OR m.WSKAZNIK_ZGODNOSCI IS NULL
        )
          AND m.STATUS_MISJI = 2
 
        {warunki_sql}

        ORDER BY m.MISJA_ID_MOJE_PK ASC
        ;
    """)

    q_select_tytul = text("""
        SELECT 
            m.MISJA_ID_MOJE_PK, 
            m.MISJA_TYTUL_EN, 
            m.MISJA_TYTUL_PL, 
            m.NAZWA_LINII_FABULARNEJ_EN, 
            m.NAZWA_LINII_FABULARNEJ_PL,
            ns1.NAZWA AS NAZWA_NPC_START,
            ns2.NAZWA AS NAZWA_NPC_KONIEC
        FROM dbo.MISJE AS m
        INNER JOIN dbo.NPC_STATUSY AS ns1
           ON ns1.NPC_ID_FK = m.NPC_START_ID
          AND ns1.STATUS = '3_ZATWIERDZONO'
        LEFT JOIN dbo.NPC_STATUSY AS ns2
          ON ns2.NPC_ID_FK = m.NPC_KONIEC_ID
         AND ns2.STATUS = '3_ZATWIERDZONO'
        WHERE MISJA_ID_MOJE_PK IN :misje_id
    ;
    """).bindparams(bindparam("misje_id", expanding=True))

    q_select_misje_dialogi = text("""
        SELECT 
        ds.MISJA_ID_MOJE_FK, ds.SEGMENT, ds.STATUS, ds.NR_BLOKU_DIALOGU, ds.NR_WYPOWIEDZI, ds.TRESC, ns.NAZWA AS NAZWA_NPC_START
        FROM [dbo].[DIALOGI_STATUSY] AS ds
        LEFT JOIN dbo.NPC_STATUSY AS ns
          ON ns.NPC_ID_FK = ds.NPC_ID_FK
         AND ns.STATUS = '3_ZATWIERDZONO'
        WHERE MISJA_ID_MOJE_FK IN :misje_id
    """).bindparams(bindparam("misje_id", expanding=True))

    q_select_misje_tresci = text("""
        SELECT MISJA_ID_MOJE_FK AS MISJA_ID, SEGMENT, PODSEGMENT, STATUS, NR AS NR_BLOKU, TRESC
        FROM dbo.MISJE_STATUSY
        WHERE MISJA_ID_MOJE_FK IN :misje_id
    """).bindparams(bindparam("misje_id", expanding=True))
    
    parametry = {
        "kraina_en": kraina, 
        "fabula_en": fabula, 
        "dodatek_en": dodatek
    }

    with silnik.connect() as conn:
        misje = conn.execute(q_select_misje_id, parametry).scalars().all()
        if not misje:
            print("Brak misji spełniających podane filtry.")
            return pd.DataFrame(columns=[
                "MISJA_ID", "SEGMENT", "PODSEGMENT", "ID_SEGMENTU", "NR_BLOKU",
                "NR_WYP", "STATUS", "TRESC", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"
            ])
        tytuly = conn.execute(q_select_tytul, {"misje_id": misje}).mappings().all()
        misje_dialogi = conn.execute(q_select_misje_dialogi, {"misje_id": misje}).mappings().all()
        misje_tresci = conn.execute(q_select_misje_tresci, {"misje_id": misje}).mappings().all()

    df_tytuly = pd.DataFrame(tytuly)
    df_misje_dialogi = pd.DataFrame(misje_dialogi)
    df_misje_tresci = pd.DataFrame(misje_tresci)

    mapa_npc_start = df_tytuly.set_index("MISJA_ID_MOJE_PK")["NAZWA_NPC_START"].to_dict()
    mapa_npc_koniec = df_tytuly.set_index("MISJA_ID_MOJE_PK")["NAZWA_NPC_KONIEC"].to_dict()

    nowe_naglowki = {
        "MISJA_ID_MOJE_PK": "MISJA_ID",
        "MISJA_ID_MOJE_FK": "MISJA_ID",
        "value": "TRESC",
        "NR_WYPOWIEDZI": "NR_WYP",
        "NR_BLOKU_DIALOGU": "NR_BLOKU"
    }
    
    nowe_wiersze = {
        "MISJA_TYTUL_EN": "0_ORYGINAŁ",
        "MISJA_TYTUL_PL": "3_ZATWIERDZONO"
    }

    mapping_segmentow = {
        "TYTUL": 1, "CEL": 2, "TREŚĆ": 3, "POSTĘP": 4, "ZAKOŃCZENIE": 5, "NAGRODY": 6, "DYMEK": 7, "GOSSIP": 8
    }
    
    kolejnosc_kolumn_koniec = [
        "MISJA_ID", "SEGMENT", "PODSEGMENT", "ID_SEGMENTU", "NR_BLOKU", "NR_WYP", "STATUS", "TRESC", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"
    ]

    df_tytuly = (
        df_tytuly.melt(
            id_vars=["MISJA_ID_MOJE_PK", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"], 
            value_vars=["MISJA_TYTUL_EN", "MISJA_TYTUL_PL"], 
            var_name="STATUS"
        )
        .rename(columns=nowe_naglowki)
        .replace(nowe_wiersze)
        .assign(SEGMENT = "TYTUL", PODSEGMENT = "", NR_BLOKU = 1, NR_WYP = 1)
    )
    
    df_misje_dialogi = (
        df_misje_dialogi
        .rename(columns=nowe_naglowki)
        .assign(
            PODSEGMENT = "",
            NAZWA_NPC_KONIEC = lambda x: x["MISJA_ID"].map(mapa_npc_koniec)
        )
    )

    df_misje_dialogi_zatwierdzone = (
        df_misje_dialogi[["MISJA_ID", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "NR_WYP", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"]]
        .drop_duplicates()
        .assign(STATUS="3_ZATWIERDZONO", TRESC="")
    )

    df_misje_tresci = (
        df_misje_tresci
        .rename(columns=nowe_naglowki)
        .assign(
            NR_WYP = 1,
            NAZWA_NPC_START = lambda x: x["MISJA_ID"].map(mapa_npc_start),
            NAZWA_NPC_KONIEC = lambda x: x["MISJA_ID"].map(mapa_npc_koniec)
        )
    )

    df_misje_tresci_zatwierdzone = (
        df_misje_tresci[["MISJA_ID", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "NR_WYP", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"]]
        .drop_duplicates()
        .assign(STATUS="3_ZATWIERDZONO", TRESC="")
    )

    df_polaczone = (
            pd.concat([
                df_tytuly, 
                df_misje_dialogi, df_misje_dialogi_zatwierdzone, 
                df_misje_tresci, df_misje_tresci_zatwierdzone
            ])
            .assign(
                ID_SEGMENTU = lambda x: x["SEGMENT"].map(mapping_segmentow)
            )
            .sort_values(by=["MISJA_ID", "ID_SEGMENTU", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "NR_WYP", "STATUS"])
            .reset_index(drop=True)
            [kolejnosc_kolumn_koniec]
        )

    tagi_zmiana = {
        "<Name>":  r"{imie}", 
        "<Class>": r"{klasa=wolacz}",
        "<Race>":  r"{rasa=wolacz}",
        # nwm na ile ponizsze potrzebne sa, ale dodaje w razie w
        "<name>":  r"{imie}", 
        "<class>": r"{klasa=wolacz}",
        "<race>":  r"{rasa=wolacz}"
    }
    df_polaczone["TRESC"] = df_polaczone["TRESC"].str.replace(tagi_zmiana, regex=False)
    
    with pd.ExcelWriter(sciezka, engine="xlsxwriter") as zapis:
            df_polaczone.to_excel(zapis, sheet_name="Tlumaczenia", index=False)
            
            arkusz = zapis.sheets["Tlumaczenia"]
            
            arkusz.freeze_panes(1, 0)
            
            format_bazowy = zapis.book.add_format({
                "bg_color": "black",
                "font_color": "white",
                "align": "center",
                "valign": "vcenter"
            })
            
            format_zawijania = zapis.book.add_format({
                "bg_color": "black",
                "font_color": "white",
                "align": "center",
                "valign": "vcenter",
                "text_wrap": True
            })

            format_zatwierdzone = zapis.book.add_format({
                "bg_color": "#404040",
                "font_color": "white"
            })
            
            arkusz.set_column("A:XFD", None, format_bazowy)
            
            arkusz.set_column("A:G", 15, format_bazowy)
            arkusz.set_column("H:H", 80, format_zawijania)
            arkusz.set_column("I:J", 25, format_bazowy)

            maks_wiersz = len(df_polaczone) + 1
            zakres_formatowania = f"A2:J{maks_wiersz}"

            arkusz.conditional_format(zakres_formatowania, {
                "type": "formula",
                "criteria": '=$G2="3_ZATWIERDZONO"',
                "format": format_zatwierdzone
            })

    return df_polaczone

def stworz_excele_do_recznych_tlumaczen(silnik, kraina = None, fabula = None, dodatek = None, sciezka = None):

    warunki_sql = sklej_warunki_w_WHERE(kraina, fabula, dodatek)

    q_select_misje_id = text(f"""
        SELECT m.MISJA_ID_MOJE_PK
        FROM dbo.MISJE AS m
        WHERE 1=1
          AND m.MISJA_ID_MOJE_PK <> 123456789
          AND m.WSKAZNIK_ZGODNOSCI > 0.70000
          AND (m.STATUS_MISJI IS NULL OR m.STATUS_MISJI <> 3)

        {warunki_sql}

        ORDER BY m.MISJA_ID_MOJE_PK ASC
        ;
    """)

    q_select_tytul = text("""
        SELECT
            m.MISJA_ID_MOJE_PK,
            m.MISJA_TYTUL_EN,
            m.MISJA_TYTUL_PL,
            m.NAZWA_LINII_FABULARNEJ_EN,
            m.NAZWA_LINII_FABULARNEJ_PL,
            ns1.NAZWA AS NAZWA_NPC_START,
            ns2.NAZWA AS NAZWA_NPC_KONIEC
        FROM dbo.MISJE AS m
        INNER JOIN dbo.NPC_STATUSY AS ns1
           ON ns1.NPC_ID_FK = m.NPC_START_ID
          AND ns1.STATUS = '3_ZATWIERDZONO'
        LEFT JOIN dbo.NPC_STATUSY AS ns2
          ON ns2.NPC_ID_FK = m.NPC_KONIEC_ID
         AND ns2.STATUS = '3_ZATWIERDZONO'
        WHERE MISJA_ID_MOJE_PK IN :misje_id
    ;
    """).bindparams(bindparam("misje_id", expanding=True))

    q_select_misje_dialogi = text("""
        SELECT
        ds.MISJA_ID_MOJE_FK, ds.SEGMENT, ds.STATUS, ds.NR_BLOKU_DIALOGU, ds.NR_WYPOWIEDZI, ds.TRESC, ns.NAZWA AS NAZWA_NPC_START
        FROM [dbo].[DIALOGI_STATUSY] AS ds
        LEFT JOIN dbo.NPC_STATUSY AS ns
          ON ns.NPC_ID_FK = ds.NPC_ID_FK
         AND ns.STATUS = '3_ZATWIERDZONO'
        WHERE MISJA_ID_MOJE_FK IN :misje_id
    """).bindparams(bindparam("misje_id", expanding=True))

    q_select_misje_tresci = text("""
        SELECT MISJA_ID_MOJE_FK AS MISJA_ID, SEGMENT, PODSEGMENT, STATUS, NR AS NR_BLOKU, TRESC
        FROM dbo.MISJE_STATUSY
        WHERE MISJA_ID_MOJE_FK IN :misje_id
    """).bindparams(bindparam("misje_id", expanding=True))

    parametry = {
        "kraina_en": kraina,
        "fabula_en": fabula,
        "dodatek_en": dodatek
    }

    with silnik.connect() as conn:
        misje = conn.execute(q_select_misje_id, parametry).scalars().all()
        if not misje:
            print("Brak misji spełniających podane filtry.")
            return pd.DataFrame(columns=[
                "MISJA_ID", "SEGMENT", "PODSEGMENT", "ID_SEGMENTU", "NR_BLOKU",
                "NR_WYP", "STATUS", "TRESC", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"
            ])
        tytuly = conn.execute(q_select_tytul, {"misje_id": misje}).mappings().all()
        misje_dialogi = conn.execute(q_select_misje_dialogi, {"misje_id": misje}).mappings().all()
        misje_tresci = conn.execute(q_select_misje_tresci, {"misje_id": misje}).mappings().all()
        archiwum = pobierz_wiersze_archiwum_do_excela(conn, misje)

    df_tytuly = pd.DataFrame(tytuly)
    df_misje_dialogi = pd.DataFrame(misje_dialogi)
    df_misje_tresci = pd.DataFrame(misje_tresci)

    mapa_npc_start = df_tytuly.set_index("MISJA_ID_MOJE_PK")["NAZWA_NPC_START"].to_dict()
    mapa_npc_koniec = df_tytuly.set_index("MISJA_ID_MOJE_PK")["NAZWA_NPC_KONIEC"].to_dict()

    nowe_naglowki = {
        "MISJA_ID_MOJE_PK": "MISJA_ID",
        "MISJA_ID_MOJE_FK": "MISJA_ID",
        "value": "TRESC",
        "NR_WYPOWIEDZI": "NR_WYP",
        "NR_BLOKU_DIALOGU": "NR_BLOKU"
    }

    nowe_wiersze = {
        "MISJA_TYTUL_EN": "0_ORYGINAŁ",
        "MISJA_TYTUL_PL": "3_ZATWIERDZONO"
    }

    mapping_segmentow = {
        "TYTUL": 1, "CEL": 2, "TREŚĆ": 3, "POSTĘP": 4, "ZAKOŃCZENIE": 5, "NAGRODY": 6, "DYMEK": 7, "GOSSIP": 8
    }

    kolejnosc_kolumn_koniec = [
        "MISJA_ID", "SEGMENT", "PODSEGMENT", "ID_SEGMENTU", "NR_BLOKU", "NR_WYP", "STATUS", "TRESC", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"
    ]

    df_tytuly = (
        df_tytuly.melt(
            id_vars=["MISJA_ID_MOJE_PK", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"],
            value_vars=["MISJA_TYTUL_EN", "MISJA_TYTUL_PL"],
            var_name="STATUS"
        )
        .rename(columns=nowe_naglowki)
        .replace(nowe_wiersze)
        .assign(SEGMENT = "TYTUL", PODSEGMENT = "", NR_BLOKU = 1, NR_WYP = 1)
    )

    df_misje_dialogi = (
        df_misje_dialogi
        .rename(columns=nowe_naglowki)
        .assign(
            PODSEGMENT = "",
            NAZWA_NPC_KONIEC = lambda x: x["MISJA_ID"].map(mapa_npc_koniec)
        )
    )

    df_misje_dialogi_zatwierdzone = (
        df_misje_dialogi[["MISJA_ID", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "NR_WYP", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"]]
        .drop_duplicates()
        .assign(STATUS="3_ZATWIERDZONO", TRESC="")
    )

    df_misje_tresci = (
        df_misje_tresci
        .rename(columns=nowe_naglowki)
        .assign(
            NR_WYP = 1,
            NAZWA_NPC_START = lambda x: x["MISJA_ID"].map(mapa_npc_start),
            NAZWA_NPC_KONIEC = lambda x: x["MISJA_ID"].map(mapa_npc_koniec)
        )
    )

    df_misje_tresci_zatwierdzone = (
        df_misje_tresci[["MISJA_ID", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "NR_WYP", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"]]
        .drop_duplicates()
        .assign(STATUS="3_ZATWIERDZONO", TRESC="")
    )

    df_tytuly_archiwum = zbuduj_wiersze_archiwum(
        df_tytuly.loc[
            df_tytuly["STATUS"] == "3_ZATWIERDZONO",
            ["MISJA_ID", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "NR_WYP", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"]
        ],
        archiwum
    )

    df_misje_dialogi_archiwum = zbuduj_wiersze_archiwum(
        df_misje_dialogi_zatwierdzone[[
            "MISJA_ID", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "NR_WYP", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"
        ]],
        archiwum
    )

    df_misje_tresci_archiwum = zbuduj_wiersze_archiwum(
        df_misje_tresci_zatwierdzone[[
            "MISJA_ID", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "NR_WYP", "NAZWA_NPC_START", "NAZWA_NPC_KONIEC"
        ]],
        archiwum
    )

    df_polaczone = (
            pd.concat([
                df_tytuly, df_tytuly_archiwum,
                df_misje_dialogi, df_misje_dialogi_zatwierdzone, df_misje_dialogi_archiwum,
                df_misje_tresci, df_misje_tresci_zatwierdzone, df_misje_tresci_archiwum
            ])
            .assign(
                ID_SEGMENTU = lambda x: x["SEGMENT"].map(mapping_segmentow)
            )
            .sort_values(by=["MISJA_ID", "ID_SEGMENTU", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "NR_WYP", "STATUS"])
            .reset_index(drop=True)
            [kolejnosc_kolumn_koniec]
        )

    tagi_zmiana = {
        "<Name>":  r"{imie}",
        "<Class>": r"{klasa=wolacz}",
        "<Race>":  r"{rasa=wolacz}",
        "<name>":  r"{imie}",
        "<class>": r"{klasa=wolacz}",
        "<race>":  r"{rasa=wolacz}"
    }
    df_polaczone["TRESC"] = df_polaczone["TRESC"].str.replace(tagi_zmiana, regex=False)

    with pd.ExcelWriter(sciezka, engine="xlsxwriter") as zapis:
            df_polaczone.to_excel(zapis, sheet_name="Tlumaczenia", index=False)

            arkusz = zapis.sheets["Tlumaczenia"]

            arkusz.freeze_panes(1, 0)

            format_bazowy = zapis.book.add_format({
                "bg_color": "black",
                "font_color": "white",
                "align": "center",
                "valign": "vcenter"
            })

            format_zawijania = zapis.book.add_format({
                "bg_color": "black",
                "font_color": "white",
                "align": "center",
                "valign": "vcenter",
                "text_wrap": True
            })

            format_zatwierdzone = zapis.book.add_format({
                "bg_color": "#404040",
                "font_color": "white"
            })

            format_archiwum = zapis.book.add_format({
                "bg_color": "#17365D",
                "font_color": "white"
            })

            arkusz.set_column("A:XFD", None, format_bazowy)

            arkusz.set_column("A:G", 15, format_bazowy)
            arkusz.set_column("H:H", 80, format_zawijania)
            arkusz.set_column("I:J", 25, format_bazowy)

            maks_wiersz = len(df_polaczone) + 1
            zakres_formatowania = f"A2:J{maks_wiersz}"

            arkusz.conditional_format(zakres_formatowania, {
                "type": "formula",
                "criteria": '=$G2="3_ZATWIERDZONO"',
                "format": format_zatwierdzone
            })

            arkusz.conditional_format(zakres_formatowania, {
                "type": "formula",
                "criteria": '=$G2="4_ARCHIWUM"',
                "format": format_archiwum
            })

    return df_polaczone

def zatwierdz_tlumaczenia(silnik, sciezka):
    df = pd.read_excel(
        sciezka,
        usecols=["MISJA_ID", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "NR_WYP", "STATUS", "TRESC", "NAZWA_NPC_START"]
    )
    tylko_zatwierdzone = df.loc[:, "STATUS"].isin(["0_ORYGINAŁ", "3_ZATWIERDZONO"])
    bez_tytulu = df.loc[:, "SEGMENT"] != "TYTUL"

    df_tytul_en = (
        df.loc[
            (df["SEGMENT"] == "TYTUL") & (df["STATUS"] == "0_ORYGINAŁ"),
            ["MISJA_ID", "TRESC"]
        ]
        .drop_duplicates(subset=["MISJA_ID"])
        .assign(HASH_EN=lambda x: x["TRESC"].apply(generuj_hash_djb2))
    )

    df_zatw = (
        df
        [tylko_zatwierdzone]
        [bez_tytulu]
    )
    kolumny_misje = {
        "MISJA_ID": "MISJA_ID_MOJE_FK",
        "NR_BLOKU": "NR"
    }
    kolumny_dialogi = {
        "MISJA_ID": "MISJA_ID_MOJE_FK",
        "NR_BLOKU": "NR_BLOKU_DIALOGU",
        "NR_WYP": "NR_WYPOWIEDZI",
        "NAZWA_NPC_START": "NPC_ID_FK"
    }

    with silnik.connect() as conn:
        npc = df_zatw["NAZWA_NPC_START"].dropna().unique().tolist()
        q_select_npcid = text("""
            SELECT NAZWA, NPC_ID_FK
            FROM dbo.NPC_STATUSY
            WHERE NAZWA IN :npc_pl
        """).bindparams(bindparam("npc_pl", expanding=True))

        npc_id = conn.execute(q_select_npcid, {"npc_pl": npc}).mappings().all()
        npc_id_slw = {wiersz["NAZWA"]: wiersz["NPC_ID_FK"] for wiersz in npc_id}

    df_misje = df_zatw.loc[:, ["MISJA_ID", "SEGMENT", "PODSEGMENT", "NR_BLOKU", "STATUS", "TRESC"]]
    df_dialogi = df_zatw.loc[:, ["MISJA_ID", "SEGMENT", "NR_BLOKU", "NR_WYP", "STATUS", "NAZWA_NPC_START", "TRESC"]]
    bez_dialogow = ~df_misje.loc[:, "SEGMENT"].isin(["DYMEK", "GOSSIP"])

    df_misje_baza = (
        df_misje
        [bez_dialogow]
        .rename(columns=kolumny_misje)
        .sort_values(by=["MISJA_ID_MOJE_FK", "SEGMENT", "PODSEGMENT", "NR", "STATUS"])
    )

    df_oryginal = (
        df_misje_baza
        [df_misje_baza["STATUS"] == "0_ORYGINAŁ"]
        .reset_index(drop=True)
        .assign(HASH_EN=lambda x: x["TRESC"].apply(generuj_hash_djb2))
    )

    df_zatwierdzono = (
        df_misje_baza
        [df_misje_baza["STATUS"] == "3_ZATWIERDZONO"]
        .reset_index(drop=True)
    )

    if len(df_oryginal) != len(df_zatwierdzono):
        print(f"--- BŁĄD: Rozjazd liczby wierszy MISJE (0_ORYGINAŁ={len(df_oryginal)} vs 3_ZATWIERDZONO={len(df_zatwierdzono)})")
        return

    df_misje_final = df_zatwierdzono.copy()
    df_misje_final["HASH_EN"] = df_oryginal["HASH_EN"].to_numpy()

    liczba_misji = int(len(df_misje_final))
    liczba_dialogow = int((df_dialogi[~bez_dialogow]["STATUS"] == "3_ZATWIERDZONO").sum())

    try:
        (
            df_misje_final
            .to_sql(schema="dbo", name="MISJE_STATUSY", con=silnik, if_exists="append", index=False)
        )
        print(f"Przerzucono do bazy misje: {liczba_misji}/{liczba_misji}.")

        with silnik.begin() as conn:
            misje_id = df_misje_final["MISJA_ID_MOJE_FK"].unique().tolist()

            q_update_status = text("""
                UPDATE dbo.MISJE
                SET STATUS_MISJI = 3
                WHERE MISJA_ID_MOJE_PK IN :misje_id
            """).bindparams(bindparam("misje_id", expanding=True))

            conn.execute(q_update_status, {"misje_id": misje_id})
            print(f"Dodano status dla misji: {len(misje_id)}/{len(misje_id)}")

            q_update_hash_tytul = text("""
                UPDATE dbo.MISJE
                SET HASH_EN = :hash_en
                WHERE MISJA_ID_MOJE_PK = :misja_id
            """)

            parametry_hash = [
                {"misja_id": int(w["MISJA_ID"]), "hash_en": w["HASH_EN"]}
                for w in df_tytul_en.loc[df_tytul_en["MISJA_ID"].isin(misje_id), ["MISJA_ID", "HASH_EN"]].to_dict("records")
                if w["HASH_EN"] is not None
            ]

            if parametry_hash:
                conn.execute(q_update_hash_tytul, parametry_hash)

    except IntegrityError as e:
        print(f"--- BŁĄD integralności danych przy misjach: {e}")

    except DBAPIError as e:
        print(f"--- BŁĄD DB/API przy misjach: {e}")

    except SQLAlchemyError as e:
        print(f"--- BŁĄD SQLAlchemy przy misjach: {e}")

    df_dialogi_baza = (
        df_dialogi
        [~bez_dialogow]
        .rename(columns=kolumny_dialogi)
        .sort_values(by=["MISJA_ID_MOJE_FK", "SEGMENT", "NR_BLOKU_DIALOGU", "NR_WYPOWIEDZI", "STATUS"])
    )

    df_dialogi_oryginal = (
        df_dialogi_baza
        [df_dialogi_baza["STATUS"] == "0_ORYGINAŁ"]
        .reset_index(drop=True)
        .assign(HASH_EN=lambda x: x["TRESC"].apply(generuj_hash_djb2))
    )

    df_dialogi_zatwierdzono = (
        df_dialogi_baza
        [df_dialogi_baza["STATUS"] == "3_ZATWIERDZONO"]
        .reset_index(drop=True)
        .assign(NPC_ID_FK=lambda x: x["NPC_ID_FK"].map(npc_id_slw))
    )

    if len(df_dialogi_oryginal) != len(df_dialogi_zatwierdzono):
        print(f"--- BŁĄD: Rozjazd liczby wierszy DIALOGI (0_ORYGINAŁ={len(df_dialogi_oryginal)} vs 3_ZATWIERDZONO={len(df_dialogi_zatwierdzono)})")
        return

    df_dialogi_final = df_dialogi_zatwierdzono.copy()
    df_dialogi_final["HASH_EN"] = df_dialogi_oryginal["HASH_EN"].to_numpy()

    try:
        (
            df_dialogi_final
            .to_sql(schema="dbo", name="DIALOGI_STATUSY", con=silnik, if_exists="append", index=False)
        )
        print(f"Przerzucono do bazy dialogi: {liczba_dialogow}/{liczba_dialogow}.")

    except IntegrityError as e:
        print(f"--- BŁĄD integralności danych przy dialogach: {e}")

    except DBAPIError as e:
        print(f"--- BŁĄD DB/API przy dialogach: {e}")

    except SQLAlchemyError as e:
        print(f"--- BŁĄD SQLAlchemy przy dialogach: {e}")
