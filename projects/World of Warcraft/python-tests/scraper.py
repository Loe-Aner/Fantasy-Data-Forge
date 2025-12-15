import requests
from bs4 import BeautifulSoup
import hashlib

def pobierz_soup(url: str) -> BeautifulSoup:
    odpowiedz = requests.get(
        url,
        headers={"User-Agent": "WoW_PolishTranslationProject -> (reachable on your Discord: Loe'Aner)"},
        timeout=30
    )
    odpowiedz.raise_for_status()
    return BeautifulSoup(odpowiedz.text, "html.parser")


def pobierz_tresc(soup: BeautifulSoup):
    return soup.select_one("#bodyContent")


def parsuj_podsumowanie_misji(tresc):
    pods_misji = tresc.find(class_="infobox darktable questbox") if tresc else None
    pods_misji_slownik = {}

    if pods_misji:
        tytul_misji = pods_misji.find(class_="above-header")
        if tytul_misji:
            pods_misji_slownik["Tytuł"] = tytul_misji.get_text()

        for ps in pods_misji.find_all("th"):
            naglowki_t = ps.get_text().strip()
            zawartosc = ps.find_next_sibling()
            if not zawartosc or zawartosc.name != "td":
                continue

            match naglowki_t:
                case "Start":
                    naglowki_t = "Start_NPC"
                case "End":
                    naglowki_t = "Koniec_NPC"
                case "Level":
                    naglowki_t = "Wymagany_Poziom"
                case "Experience":
                    naglowki_t = "Doświadczenie"
                case "Rewards":
                    naglowki_t = "Nagrody"
                case "Previous":
                    naglowki_t = "Poprzednia_Misja"
                case "Next":
                    naglowki_t = "Następna_Misja"

            pods_misji_slownik[naglowki_t] = zawartosc.get_text().strip().replace("\xa0", " ")

    return pods_misji_slownik


def parsuj_cele_misji(tresc):
    cele_misji = tresc.find(id="Objectives") if tresc else None
    cele_misji_slownik = {"Główny": {}, "Podrzędny": {}}

    if cele_misji:
        naglowki = cele_misji.find_parent("h2")
        if naglowki:
            glowny_cel = naglowki.find_next_sibling("p")
            if glowny_cel:
                cele_misji_slownik["Główny"][1] = glowny_cel.get_text().strip()

                lista_celi = glowny_cel.find_next_sibling()
                if lista_celi:
                    podcele = lista_celi.find_all("li")
                    for i, pc in enumerate(podcele, start=1):
                        cele_misji_slownik["Podrzędny"][i] = pc.get_text().strip()

    return cele_misji_slownik


def parsuj_sekcje_paragrafowe(tresc, id_ze_strony):
    sekcja = tresc.find(id=id_ze_strony) if tresc else None
    sekcja_slownik = {}

    if sekcja:
        elem = sekcja.find_parent("h2")
        if not elem:
            return sekcja_slownik

        licznik = 0
        while True:
            elem = elem.find_next_sibling()
            if elem is None or elem.name in ("h2",):
                break
            elif elem.name == "p":
                licznik += 1
                sekcja_slownik[licznik] = elem.get_text().strip()

    return sekcja_slownik


def parsuj_opis(tresc):
    return parsuj_sekcje_paragrafowe(tresc, "Description")


def parsuj_postep(tresc):
    return parsuj_sekcje_paragrafowe(tresc, "Progress")


def parsuj_zakonczenie(tresc):
    return parsuj_sekcje_paragrafowe(tresc, "Completion")


def parsuj_nagrode(tresc):
    return parsuj_sekcje_paragrafowe(tresc, "Rewards")


# def parsuj_gossipy(tresc):
#     gossipy = []
#     if not tresc:
#         return gossipy

#     for g in tresc.find_all(class_="dialogue plainlist"):
#         tytul = g.find(class_="dialogue-title")
#         npc_en = ""
#         if tytul:
#             p = tytul.find("p")
#             if p:
#                 npc_en = p.get_text().strip()

#         teksty = []
#         for p in g.find_all("p"):
#             if tytul and tytul.find("p") == p:
#                 continue
#             t = p.get_text().strip()
#             if t:
#                 teksty.append(t)

#         tekst_en = "\n".join(teksty).replace("\xa0", " ")
#         gossipy.append({"npc_en": npc_en, "tekst_en": tekst_en})

#     return gossipy


# def parsuj_dymki(tresc):
#     dymki = []
#     if not tresc:
#         return dymki

#     for span in tresc.select("span.text-say"):
#         b = span.find("b")
#         if not b:
#             continue

#         prefix = b.get_text().strip().replace("\xa0", " ")
#         npc_en = prefix.replace("says:", "").strip()

#         b.extract()
#         tekst_en = span.get_text().strip().replace("\xa0", " ")

#         dymki.append({"npc_en": npc_en, "tekst_en": tekst_en})

#     return dymki


def parsuj_wspolna_kolejnosc_gossipow_i_dymkow(tresc):
    if not tresc:
        return []

    wynik = []
    licznik = 0

    for el in tresc.descendants:
        if not hasattr(el, "name"):
            continue

        # GOSSIPY
        if el.name == "div" and "dialogue" in (el.get("class") or []):
            tytul = el.find(class_="dialogue-title")
            npc_en = ""
            if tytul:
                p = tytul.find("p")
                if p:
                    npc_en = p.get_text().strip()

            teksty = []
            for p in el.find_all("p"):
                if tytul and tytul.find("p") == p:
                    continue
                t = p.get_text().strip()
                if t:
                    teksty.append(t)

            tekst_en = "\n".join(teksty).replace("\xa0", " ")

            licznik += 1
            wynik.append({
                "id": licznik,
                "typ": "gossip",
                "npc_en": npc_en,
                "tekst_en": tekst_en
            })

        # DYMKI
        elif el.name == "span" and any(
            cls in (el.get("class") or [])
            for cls in ("text-say", "text-bossemote")
        ):
            b = el.find("b")
            if not b:
                continue

            prefix = b.get_text().strip().replace("\xa0", " ")
            npc_en = prefix.replace("says:", "").strip()

            el_copy = BeautifulSoup(str(el), "html.parser").select_one("span")
            b2 = el_copy.find("b")
            b2.extract()
            tekst_en = el_copy.get_text().strip().replace("\xa0", " ")

            licznik += 1
            wynik.append({
                "id": licznik,
                "typ": "dymek",
                "npc_en": npc_en.replace(":", ""),
                "tekst_en": tekst_en
            })

    return wynik

def indeksuj_linie(text):
    linie = [x.strip() for x in text.split("\n")]
    linie = [x for x in linie if x]
    return {i: linia for i, linia in enumerate(linie, start=1)}

def agreguj_kolejne_wypowiedzi(sequence):
    wynik = []
    ostatni = None

    for el in sequence:
        if (
            ostatni
            and el.get("typ") == ostatni.get("typ")
            and el.get("npc_en") == ostatni.get("npc_en")
        ):
            start = len(ostatni["wypowiedzi_EN"]) + 1
            for _, tekst in el["wypowiedzi_EN"].items():
                ostatni["wypowiedzi_EN"][start] = tekst
                start += 1
        else:
            wynik.append(el)
            ostatni = el

    return wynik

def renumeruj_id(sequence):
    for i, el in enumerate(sequence, start=1):
        el["id"] = i
    return sequence

def policz_content_hash(tresc) -> str | None:
    if not tresc:
        return None
    tekst = tresc.get_text(separator="\n").strip()
    return hashlib.sha256(tekst.encode("utf-8")).hexdigest()

def parsuj_misje_z_url(url: str):
    soup = pobierz_soup(url)
    tresc = pobierz_tresc(soup)

    sequence = parsuj_wspolna_kolejnosc_gossipow_i_dymkow(tresc)

    for el in sequence:
        el["wypowiedzi_EN"] = indeksuj_linie(el["tekst_en"])
        del el["tekst_en"]

    sequence = agreguj_kolejne_wypowiedzi(sequence)
    sequence = renumeruj_id(sequence)

    return {
        "Źródło": {
            "url": url
        },
        "Misje_EN": {
            "Podsumowanie_EN": parsuj_podsumowanie_misji(tresc),
            "Cele_EN": parsuj_cele_misji(tresc),
            "Treść_EN": parsuj_opis(tresc),
            "Postęp_EN": parsuj_postep(tresc),
            "Zakończenie_EN": parsuj_zakonczenie(tresc),
            "Nagrody_EN": parsuj_nagrode(tresc)
        },
        "Dialogi_EN": {
            "Gossipy_Dymki_EN": sequence
        },
        "Hash_HTML": policz_content_hash(tresc)
    }

url = "https://warcraft.wiki.gg/wiki/Wrath_Unleashed_(quest)"
wynik = parsuj_misje_z_url(url)

import json
print(json.dumps(wynik, ensure_ascii=False, indent=2))