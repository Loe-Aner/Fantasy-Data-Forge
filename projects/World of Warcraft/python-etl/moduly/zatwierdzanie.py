from sqlalchemy import text, bindparam
import pandas as pd

from moduly.utils import sklej_warunki_w_WHERE

def stworz_excele_do_zatwierdzenia_tlumaczen(silnik, kraina = None, fabula = None, dodatek = None, sciezka = None):

    warunki_sql = sklej_warunki_w_WHERE(kraina, fabula, dodatek)

    q_select_misje_id = text(f"""
        SELECT m.MISJA_ID_MOJE_PK
        FROM dbo.MISJE AS m
        WHERE 1=1
        AND m.MISJA_ID_MOJE_PK <> 123456789
                        
        {warunki_sql}

        ORDER BY m.MISJA_ID_MOJE_PK ASC
        ;
    """)

    q_select_tytul = text(f"""
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
        LEFT OUTER JOIN dbo.NPC_STATUSY AS ns2
          ON ns2.NPC_ID_FK = m.NPC_KONIEC_ID
         AND ns2.STATUS = '3_ZATWIERDZONO'
        WHERE MISJA_ID_MOJE_PK IN :misje_id
    ;
    """).bindparams(bindparam("misje_id", expanding=True))

    q_select_misje_dialogi = text("""
        SELECT 
        ds.MISJA_ID_MOJE_FK, ds.SEGMENT, ds.STATUS, ds.NR_BLOKU_DIALOGU, ds.NR_WYPOWIEDZI, ds.TRESC, ns.NAZWA AS NAZWA_NPC_START
        FROM [dbo].[DIALOGI_STATUSY] AS ds
        LEFT OUTER JOIN dbo.NPC_STATUSY AS ns
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
        tytuly = conn.execute(q_select_tytul, {"misje_id": misje}).mappings().all()
        misje_dialogi = conn.execute(q_select_misje_dialogi, {"misje_id": misje}).mappings().all()
        misje_tresci = conn.execute(q_select_misje_tresci, {"misje_id": misje}).mappings().all()

    df_tytuly = pd.DataFrame(tytuly)
    df_misje_dialogi = pd.DataFrame(misje_dialogi)
    df_misje_tresci = pd.DataFrame(misje_tresci)

    # Słowniki mapujące NPC z pobranych tytułów
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