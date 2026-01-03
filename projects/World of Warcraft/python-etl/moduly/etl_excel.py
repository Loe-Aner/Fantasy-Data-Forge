from sqlalchemy import text
import pandas as pd

from moduly.db_core import utworz_engine_do_db

def aktualizuj_misje_z_excela(df, silnik, chunk_size=1000):
    df = df.dropna(how="all").copy()
    df["NAZWA_LINII_FABULARNEJ_EN"] = df["NAZWA_LINII_FABULARNEJ_EN"].fillna("NoData")

    q = text("SELECT MISJA_TYTUL_EN FROM dbo.MISJE;")
    with silnik.connect() as conn:
        tytuly_db = {row[0] for row in conn.execute(q).all()}

    excel_total = len(df)
    df = df[df["MISJA_TYTUL_EN"].isin(tytuly_db)]
    match_total = len(df)

    if match_total == 0:
        print(f"UPDATE MISJE: 0 dopasowań (Excel={excel_total}, DB={len(tytuly_db)})")
        return

    u = text("""
    UPDATE dbo.MISJE
    SET MISJA_ID_Z_GRY            = :misja_id_z_gry,
        MISJA_URL_WOWHEAD         = :misja_url_wowhead,
        NAZWA_LINII_FABULARNEJ_EN = :nazwa_linii_fabularnej_en,
        KONTYNENT_EN              = :kontynent_en,
        KRAINA_EN                 = :kraina_en,
        DODATEK_EN                = :dodatek_en,
        KONTYNENT_PL              = :kontynent_pl,
        KRAINA_PL                 = :kraina_pl,
        DODATEK_PL                = :dodatek_pl,
        DODANO_W_PATCHU           = :dodano_w_patchu,
        DATA_UPDATE               = SYSDATETIME()
    WHERE MISJA_TYTUL_EN = :misja_tytul_en
    """)

    parametry = [
        {
            "misja_id_z_gry": int(r["MISJA_ID_Z_GRY"]) if pd.notna(r["MISJA_ID_Z_GRY"]) else None,
            "misja_url_wowhead": r["MISJA_URL_WOWHEAD"],
            "nazwa_linii_fabularnej_en": r["NAZWA_LINII_FABULARNEJ_EN"],
            "kontynent_en": r["KONTYNENT_EN"],
            "kraina_en": r["KRAINA_EN_FINAL"],
            "dodatek_en": r["DODATEK_EN"],
            "kontynent_pl": r["KONTYNENT_PL"],
            "kraina_pl": r["KRAINA_PL"],
            "dodatek_pl": r["DODATEK_PL"],
            "dodano_w_patchu": r["DODANO_W_PATCHU"],
            "misja_tytul_en": r["MISJA_TYTUL_EN"],
        }
        for r in df.to_dict("records")
    ]

    total = len(parametry)
    chunks = (total + chunk_size - 1) // chunk_size

    with silnik.begin() as conn:
        for i in range(0, total, chunk_size):
            conn.execute(u, parametry[i:i + chunk_size])

    print(f"UPDATE MISJE: Excel={excel_total}, dopasowane_do_DB={match_total}, wysłane={total}, batche={chunks}")

def aktualizuj_id_misji_wowhead_z_excela(df, silnik, chunk_size=1000):
    excel_total = len(df)
    
    u = text("""
        UPDATE dbo.MISJE
        SET MISJA_ID_Z_GRY      = :misja_id_z_gry_final,
            MISJA_URL_WOWHEAD   = :misja_url_wowhead_final
        WHERE MISJA_ID_MOJE_PK  = :misja_id_moje_pk
    """)

    parametry = [
        {
            "misja_id_z_gry_final": int(r["MISJA_ID_Z_GRY_FINAL"]),
            "misja_url_wowhead_final": r["MISJA_URL_WOWHEAD_FINAL"],
            "misja_id_moje_pk": r["MISJA_ID_MOJE_PK"]
        }
        for r in df.to_dict("records")
    ]

    match_total = len(parametry)
    total = len(parametry)
    chunks = (total + chunk_size - 1) // chunk_size

    with silnik.begin() as conn:
        for i in range(0, total, chunk_size):
            conn.execute(u, parametry[i:i + chunk_size])

    print(f"UPDATE MISJE: Excel={excel_total}, dopasowane_do_DB={match_total}, wysłane={total}, batche={chunks}")

def slowa_kluczowe_do_db(
    plik_do_otwarcia=r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\slowa_kluczowe.xlsx",
    silnik=utworz_engine_do_db()
):

    plik_slowa_kluczowe = pd.read_excel(
        plik_do_otwarcia, 
        sheet_name="do_tabeli_slowa_kluczowe", 
        usecols=["SLOWO_EN", "SLOWO_PL", "KATEGORIA"]
    )
    
    with silnik.begin() as conn:
        q_select_sk = text("SELECT SLOWO_EN, SLOWO_PL FROM dbo.SLOWA_KLUCZOWE")
        
        q_insert_sk = text("""
            INSERT INTO dbo.SLOWA_KLUCZOWE (SLOWO_EN, SLOWO_PL, KATEGORIA)
            VALUES (:slowo_en, :slowo_pl, :kategoria)
        """)
        
        q_update_sk = text("""
            UPDATE dbo.SLOWA_KLUCZOWE
            SET SLOWO_PL = :slowo_pl
            WHERE SLOWO_EN = :slowo_en
        """)

        db_records = conn.execute(q_select_sk).all()
        
        mapa_db = {row[0]: row[1] for row in db_records}
        zestaw_en_db = set(mapa_db.keys())

        lista_do_wstawienia = []
        lista_do_aktualizacji = []

        for slowo_en, slowo_pl, kategoria in plik_slowa_kluczowe.values:
            if slowo_en not in zestaw_en_db:
                lista_do_wstawienia.append({
                    "slowo_en": slowo_en, 
                    "slowo_pl": slowo_pl, 
                    "kategoria": kategoria
                })
            elif mapa_db[slowo_en] != slowo_pl:
                lista_do_aktualizacji.append({
                    "slowo_en": slowo_en, 
                    "slowo_pl": slowo_pl
                })

        if lista_do_wstawienia:
            print(f"Wykryto {len(lista_do_wstawienia)} nowych słów. Dodaję...")
            conn.execute(q_insert_sk, lista_do_wstawienia)
            print("Zakończono dodawanie.")
        else:
            print("Brak nowych słów do dodania.")

        if lista_do_aktualizacji:
            print(f"Wykryto {len(lista_do_aktualizacji)} zmienionych tłumaczeń. Aktualizuję...")
            conn.execute(q_update_sk, lista_do_aktualizacji)
            print("Zakończono aktualizację.")
        else:
            print("Brak tłumaczeń wymagających aktualizacji.")

def mapowanie_misji_do_db(
    plik_do_otwarcia=r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\slowa_kluczowe.xlsx",
    silnik=utworz_engine_do_db()
):

    plik_lacznika = pd.read_excel(
        plik_do_otwarcia,
        sheet_name="do_tabeli_misje_slowa_kluczowe",
        usecols=["MISJA_ID_MOJE_FK", "SLOWO_EN"]
    )

    with silnik.begin() as conn:
        q_mapowanie_slow = text("SELECT SLOWO_EN, SLOWO_ID_PK " \
        "                        FROM dbo.SLOWA_KLUCZOWE")
        q_dostepne_misje = text("SELECT MISJA_ID_MOJE_PK " \
        "                        FROM dbo.MISJE")
        q_select_lacznik = text("SELECT MISJA_ID_MOJE_FK, SLOWO_ID " \
        "                        FROM dbo.MISJE_SLOWA_KLUCZOWE")
        
        q_insert_lacznik = text("""
            INSERT INTO dbo.MISJE_SLOWA_KLUCZOWE (MISJA_ID_MOJE_FK, SLOWO_ID)
            VALUES (:misja_id, :slowo_id)
        """)
        
        mapa_slow_sql = dict(conn.execute(q_mapowanie_slow).all())
        dostepne_misje_sql = set(conn.execute(q_dostepne_misje).scalars().all())
        istniejace_pary_sql = set(conn.execute(q_select_lacznik).all())

        plik_lacznika["SLOWO_ID"] = plik_lacznika["SLOWO_EN"].map(mapa_slow_sql)
        
        odrzucone_brak_slowa = plik_lacznika[plik_lacznika["SLOWO_ID"].isna()].copy()
        odrzucone_brak_slowa["PRZYCZYNA"] = "Brak słowa w DB"
        
        plik_lacznika = plik_lacznika.dropna(subset=["SLOWO_ID"])
        plik_lacznika["SLOWO_ID"] = plik_lacznika["SLOWO_ID"].astype("int64")

        # Walidacja Misji
        maska_poprawne_misje = plik_lacznika["MISJA_ID_MOJE_FK"].isin(dostepne_misje_sql)
        odrzucone_brak_misji = plik_lacznika[~maska_poprawne_misje].copy()
        odrzucone_brak_misji["PRZYCZYNA"] = "Brak ID misji w DB"

        plik_lacznika = plik_lacznika[maska_poprawne_misje]

        # Walidacja Duplikatów
        pary_z_excela = set(zip(plik_lacznika["MISJA_ID_MOJE_FK"], plik_lacznika["SLOWO_ID"]))
        
        do_wgrania_set = pary_z_excela - istniejace_pary_sql
        juz_w_bazie_set = pary_z_excela.intersection(istniejace_pary_sql)

        maska_juz_w_bazie = plik_lacznika[["MISJA_ID_MOJE_FK", "SLOWO_ID"]].apply(tuple, axis=1).isin(juz_w_bazie_set)
        odrzucone_duplikaty_db = plik_lacznika[maska_juz_w_bazie].copy()
        odrzucone_duplikaty_db["PRZYCZYNA"] = "Relacja już istnieje w DB"

        dane_do_insertu = [
            {"misja_id": m_id, "slowo_id": s_id}
            for m_id, s_id in do_wgrania_set
        ]

        if dane_do_insertu:
            print(f"Rozpoczynam dodawanie {len(dane_do_insertu)} nowych powiązań...")
            conn.execute(q_insert_lacznik, dane_do_insertu)
            print(f"✅ Sukces: Dodano {len(dane_do_insertu)} powiązań.")
        else:
            print("Brak nowych powiązań do dodania.")

    # Raportowanie
    raport_odrzuconych = pd.concat([
        odrzucone_brak_slowa,
        odrzucone_brak_misji,
        odrzucone_duplikaty_db
    ], ignore_index=True)

    if not raport_odrzuconych.empty:
        print("\n⚠️  Raport odrzuconych rekordów:")
        grupy = raport_odrzuconych.groupby("PRZYCZYNA")
        for przyczyna, grupa in grupy:
            print(f"🔴 {przyczyna}: {len(grupa)} szt.")
            if "Brak" in przyczyna:
                kolumna_info = "SLOWO_EN" if "słowa" in przyczyna else "MISJA_ID_MOJE_FK"
                print(f"   Przykłady: {grupa[kolumna_info].unique()}")
    else:
        print("\n🎉 Wszystkie rekordy z Excela są poprawne i trafiły do bazy (lub już tam były).")

def zapisz_npc_i_status_przetlumaczony_do_db(
    silnik, 
    plik_do_otwarcia=r"D:\MyProjects_4Fun\projects\World of Warcraft\excel-mappingi\npc.xlsx",
    rozmiar_partii=10000
):
    status_docelowy = "3_ZATWIERDZONO"

    df = pd.read_excel(
        plik_do_otwarcia, 
        sheet_name="surowe", 
        usecols=["NPC_ID_MOJE_PK", "NAZWA", "NAZWA_PL_FINAL", "PLEC", "RASA", "KLASA", "TYTUL"]
    )

    q_pobierz_juz_zatwierdzone = text("""
        SELECT NPC_ID_FK 
        FROM dbo.NPC_STATUSY 
        WHERE STATUS = :status
    """)

    with silnik.connect() as conn:
        df_istniejace = pd.read_sql(q_pobierz_juz_zatwierdzone, conn, params={"status": status_docelowy})

    zestaw_juz_zatwierdzonych_id = set(df_istniejace["NPC_ID_FK"].astype(int))

    maska_juz_sa_zatwierdzone = df["NPC_ID_MOJE_PK"].isin(zestaw_juz_zatwierdzonych_id)
    
    df_do_update_statusy = df[maska_juz_sa_zatwierdzone]
    df_do_insert_statusy = df[~maska_juz_sa_zatwierdzone]

    q_insert_statusy = text("""
        INSERT INTO dbo.NPC_STATUSY (NPC_ID_FK, STATUS, NAZWA)
        VALUES (:npc_id, :status, :nazwa_pl)
    """)

    q_update_statusy = text("""
        UPDATE dbo.NPC_STATUSY
        SET NAZWA = :nazwa_pl
        WHERE NPC_ID_FK = :npc_id 
          AND STATUS = :status
    """)

    q_update_npc_glowne = text("""
        UPDATE dbo.NPC
        SET PLEC = :plec, 
            RASA = :rasa,
            KLASA = :klasa,
            TYTUL = :tytul
        WHERE NAZWA = :nazwa_eng
    """)

    parametry_insert_statusy = [
        {
            "npc_id": int(r["NPC_ID_MOJE_PK"]),
            "status": status_docelowy,
            "nazwa_pl": r["NAZWA_PL_FINAL"]
        }
        for r in df_do_insert_statusy.to_dict("records")
    ]

    parametry_update_statusy = [
        {
            "npc_id": int(r["NPC_ID_MOJE_PK"]),
            "status": status_docelowy,
            "nazwa_pl": r["NAZWA_PL_FINAL"]
        }
        for r in df_do_update_statusy.to_dict("records")
    ]

    df_do_aktualizacji_npc = df[df["PLEC"].notna() & (df["PLEC"] != "")]
    parametry_update_npc_glowne = [
        {
            "nazwa_eng": r["NAZWA"],
            "plec": r["PLEC"],
            "rasa": r["RASA"],
            "klasa": r["KLASA"],
            "tytul": r["TYTUL"]
        }
        for r in df_do_aktualizacji_npc.to_dict("records")
    ]

    l_ins_stat = len(parametry_insert_statusy)
    l_upd_stat = len(parametry_update_statusy)
    l_upd_npc = len(parametry_update_npc_glowne)

    with silnik.begin() as conn:
        for i in range(0, l_ins_stat, rozmiar_partii):
            conn.execute(q_insert_statusy, parametry_insert_statusy[i:i + rozmiar_partii])
            
        for i in range(0, l_upd_stat, rozmiar_partii):
            conn.execute(q_update_statusy, parametry_update_statusy[i:i + rozmiar_partii])

        for i in range(0, l_upd_npc, rozmiar_partii):
            conn.execute(q_update_npc_glowne, parametry_update_npc_glowne[i:i + rozmiar_partii])

    print(f"PROCES ZAKONCZONY. Excel={len(df)}")
    print(f"Tabela STATUSY -> INSERT (Nowe wiersze): {l_ins_stat}")
    print(f"Tabela STATUSY -> UPDATE (Istniejące zatwierdzone): {l_upd_stat}")
    print(f"Tabela NPC -> UPDATE po nazwie: {l_upd_npc}")