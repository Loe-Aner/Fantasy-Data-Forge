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
        SELECT MISJA_ID_MOJE_PK, MISJA_TYTUL_EN, MISJA_TYTUL_PL, NAZWA_LINII_FABULARNEJ_EN, NAZWA_LINII_FABULARNEJ_PL
        FROM dbo.MISJE
        WHERE MISJA_ID_MOJE_PK IN :misje_id
    ;
    """).bindparams(bindparam("misje_id", expanding=True))

    q_select_misje_dialogi = text("""
        SELECT MISJA_ID_MOJE_FK, SEGMENT, STATUS, NR_BLOKU_DIALOGU, NR_WYPOWIEDZI, TRESC
        FROM [dbo].[DIALOGI_STATUSY]
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
        "MISJA_ID", "STATUS", "SEGMENT", "PODSEGMENT", "ID_SEGMENTU", "NR_BLOKU", "NR_WYP", "TRESC"
    ]

    df_tytuly = (
        df_tytuly.melt(
            ["MISJA_ID_MOJE_PK"], 
            value_vars=["MISJA_TYTUL_EN", "MISJA_TYTUL_PL"], 
            var_name="STATUS")
        .rename(columns=nowe_naglowki)
        .replace(nowe_wiersze)
        .assign(SEGMENT = "TYTUL", PODSEGMENT = "", NR_BLOKU = 1, NR_WYP = 1)
        )
    
    df_misje_dialogi = (
        df_misje_dialogi
        .rename(columns=nowe_naglowki)
        .assign(PODSEGMENT = "")
    )

    df_misje_tresci = (
        df_misje_tresci
        .rename(columns=nowe_naglowki)
        .assign(NR_WYP = 1)
    )

    df_polaczone = (
            pd.concat([df_tytuly, df_misje_dialogi, df_misje_tresci])
            .assign(
                ID_SEGMENTU = lambda x: x["SEGMENT"].map(mapping_segmentow)
            )
            .sort_values(by=["MISJA_ID", "STATUS", "ID_SEGMENTU", "NR_BLOKU", "NR_WYP"])
            .reset_index(drop=True)
            [kolejnosc_kolumn_koniec]
        )
    
    with pd.ExcelWriter(sciezka, engine="xlsxwriter") as zapis: # type: ignore
            df_polaczone.to_excel(zapis, sheet_name="Tlumaczenia", index=False)
            
            arkusz = zapis.sheets["Tlumaczenia"]
            
            format_bazowy = zapis.book.add_format({ # type: ignore
                "bg_color": "black",
                "font_color": "white",
                "align": "center",
                "valign": "vcenter"
            })
            
            format_zawijania = zapis.book.add_format({ # type: ignore
                "bg_color": "black",
                "font_color": "white",
                "align": "center",
                "valign": "vcenter",
                "text_wrap": True
            })
            
            arkusz.set_column("A:XFD", None, format_bazowy)
            
            arkusz.set_column("A:G", 15, format_bazowy)
            arkusz.set_column("H:H", 80, format_zawijania)

    return df_polaczone