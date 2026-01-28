from sqlalchemy import text
import re

from moduly.repo_NPC import (
    zapisz_npc_i_status_do_db,
    zapewnij_npc_i_pobierz_id
)
from moduly.repo_misje import (
    zapewnij_misje_i_pobierz_id,
    dodaj_status_misji
)
from moduly.repo_dialogi import dodaj_status_dialogu

def zapisz_npc_i_status_do_db_z_wyniku(
        silnik,
        tabela_npc: str,
        tabela_npc_statusy: str,
        szukaj_wg: list[str],
        wyscrapowana_tresc: dict,
        zrodlo: str,
        status: str = "0_ORYGINAŁ",
        jezyk: str = "EN"
    ):
    podsumowanie = wyscrapowana_tresc[f"Misje_{jezyk}"][f"Podsumowanie_{jezyk}"]

    for klucz in szukaj_wg:
        npc = podsumowanie.get(klucz)

        if npc is None:
            npc = ""
        else:
            npc = str(npc)

        npc = re.sub(r"\[.*?\]|\(.*?\)", "", npc)
        npc = re.sub(r"\s+", " ", npc).strip()

        if npc == "":
            npc = "Brak Nazwy"

        zapisz_npc_i_status_do_db(
            silnik=silnik,
            tabela_npc=tabela_npc,
            tabela_npc_statusy=tabela_npc_statusy,
            nazwa=npc,
            zrodlo=zrodlo,
            status=status
        )

def zapisz_misje_i_statusy_do_db_z_wyniku(
        silnik,
        wynik: dict,
        tabela_npc: str,
        tabela_misje: str,
        tabela_misje_statusy: str,
        status: str = "0_ORYGINAŁ",
        jezyk: str = "EN",
        misja_id_pl = None
    ) -> int:

    mapa_segment = {
        f"Cele_{jezyk}": "CEL",
        f"Treść_{jezyk}": "TREŚĆ",
        f"Postęp_{jezyk}": "POSTĘP",
        f"Zakończenie_{jezyk}": "ZAKOŃCZENIE",
        f"Nagrody_{jezyk}": "NAGRODY"
    }

    mapa_podsegment = {
        "Główny": "GŁÓWNY_CEL",
        "Podrzędny": "PODRZĘDNY_CEL"
    }

    misje_en = wynik.get(f"Misje_{jezyk}", {})
    podsumowanie = misje_en.get(f"Podsumowanie_{jezyk}", {})

    url = wynik.get("Źródło", {}).get("url")
    tytul = (podsumowanie.get("Tytuł") or "").strip()

    npc_start = (podsumowanie.get("Start_NPC") or "").strip()
    npc_koniec = (podsumowanie.get("Koniec_NPC") or "").strip()

    nastepna_misja = podsumowanie.get("Następna_Misja")
    poprzednia_misja = podsumowanie.get("Poprzednia_Misja")

    lvl_raw = podsumowanie.get("Wymagany_Poziom")
    lvl_txt = str(lvl_raw).strip() if lvl_raw is not None else ""

    if lvl_txt == "":
        lvl = 0
    else:
        lvl_txt = lvl_txt.split("-")[0].strip()
        lvl_digits = "".join(ch for ch in lvl_txt if ch.isdigit())[:2]
        lvl = int(lvl_digits) if lvl_digits != "" else 0

    if jezyk == "EN":
        misja_id = zapewnij_misje_i_pobierz_id(
            silnik=silnik,
            tabela_npc=tabela_npc,
            tabela_misje=tabela_misje,
            url=url,
            tytul=tytul,
            nastepna_misja=nastepna_misja,
            poprzednia_misja=poprzednia_misja,
            lvl=lvl,
            npc_start=npc_start,
            npc_koniec=npc_koniec
        )
    else:
        misja_id = misja_id_pl

    sekcje_do_statusow = [f"Cele_{jezyk}", f"Treść_{jezyk}", f"Postęp_{jezyk}", f"Zakończenie_{jezyk}", f"Nagrody_{jezyk}"]

    for segment in sekcje_do_statusow:
        segment_db = mapa_segment.get(segment)
        if segment_db is None:
            continue

        segment_dict = misje_en.get(segment, {})
        if not isinstance(segment_dict, dict) or not segment_dict:
            continue

        if segment == f"Cele_{jezyk}":
            for podsegment, wartosc in segment_dict.items():
                podsegment_db = mapa_podsegment.get(podsegment)
                if podsegment_db is None or not isinstance(wartosc, dict):
                    continue

                for nr_key, tresc in wartosc.items():
                    if tresc is None:
                        continue
                    tresc = str(tresc).strip()
                    if not tresc:
                        continue

                    try:
                        nr = int(str(nr_key).strip())
                    except ValueError:
                        nr = 1

                    dodaj_status_misji(
                        silnik=silnik,
                        tabela_misje_statusy=tabela_misje_statusy,
                        misja_id=misja_id,
                        segment=segment_db,
                        podsegment=podsegment_db,
                        nr=nr,
                        status=status,
                        tresc=tresc
                    )
        else:
            for nr_key, tresc in segment_dict.items():
                if tresc is None:
                    continue
                tresc = str(tresc).strip()
                if not tresc:
                    continue

                try:
                    nr = int(str(nr_key).strip())
                except ValueError:
                    nr = 1

                dodaj_status_misji(
                    silnik=silnik,
                    tabela_misje_statusy=tabela_misje_statusy,
                    misja_id=misja_id,
                    segment=segment_db,
                    podsegment=None,
                    nr=nr,
                    status=status,
                    tresc=tresc
                )

    return misja_id

def zaktualizuj_misje_z_wowhead_w_db_z_wyniku(
        silnik,
        wynik: dict,
        misja_id: int,
        tabela_misje: str = "dbo.MISJE"
    ) -> None:
    
    wh_id = wynik.get("wowhead_id")
    wh_url = wynik.get("wowhead_url")

    if not wh_id:
        return

    q_update = text(f"""
        UPDATE {tabela_misje}
        SET MISJA_ID_Z_GRY = :id_gra, MISJA_URL_WOWHEAD = :url
        WHERE MISJA_ID_MOJE_PK = :misja_id
    """)

    with silnik.begin() as conn:
        conn.execute(q_update, {
            "id_gra": wh_id,
            "url": wh_url,
            "misja_id": misja_id
        })

def zapisz_dialogi_statusy_do_db_z_wyniku(
        silnik,
        wynik: dict,
        misja_id: int,
        tabela_npc: str,
        tabela_npc_statusy: str,
        tabela_dialogi_statusy: str,
        zrodlo: str,
        status: str = "0_ORYGINAŁ",
        jezyk: str = "EN"
    ) -> None:

    mapa_segment = {
        "dymek": "DYMEK",
        "gossip": "GOSSIP"
    }

    dialogi_en = wynik.get(f"Dialogi_{jezyk}", {})
    sequence = dialogi_en.get(f"Gossipy_Dymki_{jezyk}", [])

    if not isinstance(sequence, list) or len(sequence) == 0:
        return

    for el in sequence:
        typ = (el.get("typ") or "").strip()
        segment_db = mapa_segment.get(typ)
        if segment_db is None:
            continue

        npc_nazwa = (el.get(f"npc_{jezyk.lower()}") or "").strip()
        if npc_nazwa == "":
            npc_nazwa = "Brak Danych"

        npc_id_fk = zapewnij_npc_i_pobierz_id(
            silnik=silnik,
            tabela_npc=tabela_npc,
            tabela_npc_statusy=tabela_npc_statusy,
            nazwa=npc_nazwa,
            zrodlo=zrodlo,
            status="0_ORYGINAŁ"
        )

        nr_bloku_dialogu_raw = el.get("id")
        try:
            nr_bloku_dialogu = int(str(nr_bloku_dialogu_raw).strip())
        except Exception:
            nr_bloku_dialogu = 1

        wyp = el.get(f"wypowiedzi_{jezyk}") or {}
        if not isinstance(wyp, dict) or len(wyp) == 0:
            return

        for nr_key, tresc in wyp.items():
            if tresc is None:
                continue
            tresc = str(tresc).strip()
            if tresc == "":
                continue

            try:
                nr_wypowiedzi = int(str(nr_key).strip())
            except ValueError:
                nr_wypowiedzi = 1

            dodaj_status_dialogu(
                silnik=silnik,
                tabela_dialogi_statusy=tabela_dialogi_statusy,
                misja_id=misja_id,
                segment=segment_db,
                nr_bloku_dialogu=nr_bloku_dialogu,
                nr_wypowiedzi=nr_wypowiedzi,
                npc_id_fk=npc_id_fk,
                status=status,
                tresc=tresc
            )

def przefiltruj_dane_misji(dane_wejsciowe, jezyk: str = "EN"):
    sekcja_misje = dane_wejsciowe.get(f"Misje_{jezyk}", {})
    
    nowy_wynik = {
        f"Misje_{jezyk}": {
            f"Podsumowanie_{jezyk}": {
                "Tytuł": sekcja_misje.get(f"Podsumowanie_{jezyk}", {}).get("Tytuł")
            },
            f"Cele_{jezyk}": sekcja_misje.get(f"Cele_{jezyk}"),
            f"Treść_{jezyk}": sekcja_misje.get(f"Treść_{jezyk}"),
            f"Postęp_{jezyk}": sekcja_misje.get(f"Postęp_{jezyk}"),
            f"Zakończenie_{jezyk}": sekcja_misje.get(f"Zakończenie_{jezyk}"),
            f"Nagrody_{jezyk}": sekcja_misje.get(f"Nagrody_{jezyk}")
        },
        f"Dialogi_{jezyk}": dane_wejsciowe.get(f"Dialogi_{jezyk}")
    }
    
    return nowy_wynik

def zapisz_misje_dialogi_ai_do_db(silnik, misja_id, przetlumaczone, status):
    print(f"\n--- [START] Zapis misji ID: {misja_id} | Status: {status} ---")
    
    if status not in ("1_PRZETŁUMACZONO", "2_ZREDAGOWANO"):
        print(f"!!! BŁĄD: Nieprawidłowy status: {status}")
        return

    q_select_npc = text("SELECT NPC_ID_FK FROM dbo.NPC_STATUSY WHERE NAZWA = :nazwa AND STATUS = '3_ZATWIERDZONO'")
    q_update_tytul = text("UPDATE dbo.MISJE SET MISJA_TYTUL_PL = :tytul_pl WHERE MISJA_ID_MOJE_PK = :misja_id")
    
    q_insert_misje = text("""
        INSERT INTO dbo.MISJE_STATUSY (MISJA_ID_MOJE_FK, SEGMENT, PODSEGMENT, STATUS, NR, TRESC)
        VALUES (:misja_id, :segment, :podsegment, :status, :nr, :tresc)
    """)
    
    q_insert_dialogi = text("""
        INSERT INTO dbo.DIALOGI_STATUSY (MISJA_ID_MOJE_FK, SEGMENT, STATUS, NR_BLOKU_DIALOGU, NR_WYPOWIEDZI, NPC_ID_FK, TRESC)
        VALUES (:misja_id, :segment, :status, :nr_bloku, :nr_wypowiedzi, :npc_id, :tresc)
    """)

    wszystkie_wiersze_misje = []
    wszystkie_wiersze_dialogi = []
    
    tytul_pl = przetlumaczone["Misje_PL"]["Podsumowanie_PL"].get("Tytuł")
    
    try:
        cele_g = przetlumaczone["Misje_PL"]["Cele_PL"]["Główny"]
        for nr, tresc in cele_g.items():
            wszystkie_wiersze_misje.append({"misja_id": misja_id, "segment": "CEL", "podsegment": "GŁÓWNY_CEL", "status": status, "nr": int(nr), "tresc": tresc})

        cele_p = przetlumaczone["Misje_PL"]["Cele_PL"]["Podrzędny"]
        for nr, tresc in cele_p.items():
            wszystkie_wiersze_misje.append({"misja_id": misja_id, "segment": "CEL", "podsegment": "PODRZĘDNY_CEL", "status": status, "nr": int(nr), "tresc": tresc})

        sekcje = ["Treść_PL", "Postęp_PL", "Zakończenie_PL", "Nagrody_PL"]
        mapa_sekcji = {"Treść_PL": "TREŚĆ", "Postęp_PL": "POSTĘP", "Zakończenie_PL": "ZAKOŃCZENIE", "Nagrody_PL": "NAGRODY"}
        
        for klucz in sekcje:
            slownik = przetlumaczone["Misje_PL"].get(klucz)
            if slownik:
                for nr, tresc in slownik.items():
                    wszystkie_wiersze_misje.append({"misja_id": misja_id, "segment": mapa_sekcji[klucz], "podsegment": None, "status": status, "nr": int(nr), "tresc": tresc})
        
        print(f"-> Przygotowano danych misji: {len(wszystkie_wiersze_misje)} wierszy.")

    except Exception as e:
        print(f"!!! BŁĄD podczas parsowania słownika misji: {e}")
        return

    try:
        with silnik.begin() as conn:
            if tytul_pl:
                conn.execute(q_update_tytul, {"tytul_pl": tytul_pl, "misja_id": misja_id})
                print(f"-> Zaktualizowano tytuł na: '{tytul_pl}'")

            if "Dialogi_PL" in przetlumaczone and przetlumaczone["Dialogi_PL"]["Gossipy_Dymki_PL"]:
                print("-> Rozpoczynam mapowanie NPC w dialogach...")
                for blok in przetlumaczone["Dialogi_PL"]["Gossipy_Dymki_PL"]:
                    npc_nazwa = blok.get("npc_pl")
                    npc_id = conn.execute(q_select_npc, {"nazwa": npc_nazwa}).scalar()

                    if npc_id is not None:
                        for nr_wyp, tekst in blok["wypowiedzi_PL"].items():
                            wszystkie_wiersze_dialogi.append({
                                "misja_id": misja_id, "segment": blok["typ"].upper(), "status": status,
                                "nr_bloku": int(blok["id"]), "nr_wypowiedzi": int(nr_wyp), 
                                "npc_id": int(npc_id), "tresc": tekst
                            })
                    else:
                        print(f"   [WARN] POMINIĘTO dialogi dla NPC: '{npc_nazwa}' (Brak ID w bazie lub niezatwierdzony)")

            print(f"-> Przygotowano dialogów: {len(wszystkie_wiersze_dialogi)} wierszy.")

            if wszystkie_wiersze_misje:
                conn.execute(q_insert_misje, wszystkie_wiersze_misje)
            
            if wszystkie_wiersze_dialogi:
                conn.execute(q_insert_dialogi, wszystkie_wiersze_dialogi)
            
            print("-> COMMIT: Dane zostały wysłane do bazy.")

    except Exception as e:
        print(f"\n!!! BŁĄD KRYTYCZNY PODCZAS ZAPISU DO BAZY:\n{e}")
        raise e

    print(f"--- [KONIEC] Sukces dla misji ID: {misja_id} ---\n")