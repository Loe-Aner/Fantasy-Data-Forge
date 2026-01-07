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
        status: str = "0_ORYGINAŁ"
    ):
    podsumowanie = wyscrapowana_tresc["Misje_EN"]["Podsumowanie_EN"]

    for klucz in szukaj_wg:
        npc = podsumowanie.get(klucz)

        if npc is None:
            continue

        npc = str(npc).strip()
        if npc == "":
            continue

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
        status: str = "0_ORYGINAŁ"
    ) -> int:

    mapa_segment = {
        "Cele_EN": "CEL",
        "Treść_EN": "TREŚĆ",
        "Postęp_EN": "POSTĘP",
        "Zakończenie_EN": "ZAKOŃCZENIE",
        "Nagrody_EN": "NAGRODY"
    }

    mapa_podsegment = {
        "Główny": "GŁÓWNY_CEL",
        "Podrzędny": "PODRZĘDNY_CEL"
    }

    misje_en = wynik.get("Misje_EN", {})
    podsumowanie = misje_en.get("Podsumowanie_EN", {})

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

    sekcje_do_statusow = ["Cele_EN", "Treść_EN", "Postęp_EN", "Zakończenie_EN", "Nagrody_EN"]

    for segment in sekcje_do_statusow:
        segment_db = mapa_segment.get(segment)
        if segment_db is None:
            continue

        segment_dict = misje_en.get(segment, {})
        if not isinstance(segment_dict, dict) or not segment_dict:
            continue

        if segment == "Cele_EN":
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


def zapisz_dialogi_statusy_do_db_z_wyniku(
        silnik,
        wynik: dict,
        misja_id: int,
        tabela_npc: str,
        tabela_npc_statusy: str,
        tabela_dialogi_statusy: str,
        zrodlo: str,
        status: str = "0_ORYGINAŁ"
    ) -> None:

    mapa_segment = {
        "dymek": "DYMEK",
        "gossip": "GOSSIP"
    }

    dialogi_en = wynik.get("Dialogi_EN", {})
    sequence = dialogi_en.get("Gossipy_Dymki_EN", [])

    if not isinstance(sequence, list) or len(sequence) == 0:
        return

    for el in sequence:
        typ = (el.get("typ") or "").strip()
        segment_db = mapa_segment.get(typ)
        if segment_db is None:
            continue

        npc_nazwa = (el.get("npc_en") or "").strip()
        if npc_nazwa == "":
            continue

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

        wyp = el.get("wypowiedzi_EN") or {}
        if not isinstance(wyp, dict) or len(wyp) == 0:
            continue

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

def przefiltruj_dane_misji(dane_wejsciowe):
    sekcja_misje = dane_wejsciowe.get("Misje_EN", {})
    
    nowy_wynik = {
        "Misje_EN": {
            "Podsumowanie_EN": {
                "Tytuł": sekcja_misje.get("Podsumowanie_EN", {}).get("Tytuł")
            },
            "Cele_EN": sekcja_misje.get("Cele_EN"),
            "Treść_EN": sekcja_misje.get("Treść_EN"),
            "Postęp_EN": sekcja_misje.get("Postęp_EN"),
            "Zakończenie_EN": sekcja_misje.get("Zakończenie_EN"),
            "Nagrody_EN": sekcja_misje.get("Nagrody_EN")
        },
        "Dialogi_EN": dane_wejsciowe.get("Dialogi_EN")
    }
    
    return nowy_wynik