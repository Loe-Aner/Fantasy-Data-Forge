import asyncio

from scraper_wiki_main import pobierz_soup_async, pobierz_tresc


from scraper_wiki_main import (
    parsuj_podsumowanie_misji,
    parsuj_cele_misji,
    parsuj_opis,
    parsuj_postep,
    parsuj_zakonczenie,
    parsuj_nagrode,
    parsuj_wspolna_kolejnosc_gossipow_i_dymkow,
    indeksuj_linie,
    agreguj_kolejne_wypowiedzi,
    renumeruj_id,
    złącz_cele,
    złącz_slownik_linii,
    złącz_dialogi,
    policz_hash_z_tekstu,
)


async def parsuj_misje_z_url_async(url: str):
    soup = await pobierz_soup_async(url)
    if soup is None:
        return None

    tresc = pobierz_tresc(soup)
    if not tresc:
        return None

    podsumowanie = parsuj_podsumowanie_misji(tresc)
    cele = parsuj_cele_misji(tresc)
    opis = parsuj_opis(tresc)
    postep = parsuj_postep(tresc)
    zakonczenie = parsuj_zakonczenie(tresc)
    nagrody = parsuj_nagrode(tresc)

    sequence = parsuj_wspolna_kolejnosc_gossipow_i_dymkow(tresc)
    for el in sequence:
        el["wypowiedzi_EN"] = indeksuj_linie(el["tekst_en"])
        del el["tekst_en"]

    sequence = agreguj_kolejne_wypowiedzi(sequence)
    sequence = renumeruj_id(sequence)

    cele_zlaczone = złącz_cele(cele)

    hash_sekcji = {
        "Cele_EN": {
            "Główny": policz_hash_z_tekstu(cele_zlaczone["Główny"]),
            "Podrzędny": policz_hash_z_tekstu(cele_zlaczone["Podrzędny"]),
        },
        "Treść_EN": policz_hash_z_tekstu(złącz_slownik_linii(opis)),
        "Postęp_EN": policz_hash_z_tekstu(złącz_slownik_linii(postep)),
        "Zakończenie_EN": policz_hash_z_tekstu(złącz_slownik_linii(zakonczenie)),
        "Nagrody_EN": policz_hash_z_tekstu(złącz_slownik_linii(nagrody)),
        "Dialogi_EN": {
            "Dymki_EN": policz_hash_z_tekstu(złącz_dialogi(sequence, {"dymek"})),
            "Gossipy_EN": policz_hash_z_tekstu(złącz_dialogi(sequence, {"gossip"})),
        },
    }

    return {
        "Źródło": {"url": url},
        "Misje_EN": {
            "Podsumowanie_EN": podsumowanie,
            "Cele_EN": cele,
            "Treść_EN": opis,
            "Postęp_EN": postep,
            "Zakończenie_EN": zakonczenie,
            "Nagrody_EN": nagrody,
        },
        "Dialogi_EN": {"Gossipy_Dymki_EN": sequence},
        "Hash_HTML": hash_sekcji,
    }


async def parsuj_wiele_misji_async(quest_urls: list[str], max_concurrency: int = 5):
    sem = asyncio.Semaphore(max_concurrency)

    async def _one(url: str):
        async with sem:
            return url, await parsuj_misje_z_url_async(url)

    wyniki = await asyncio.gather(*(_one(u) for u in quest_urls))
    return wyniki
