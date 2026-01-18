def wyscrapuj_linki_z_kategorii_wiki(tresc) -> list[str]:
    """
    Wyciąga linki /wiki/... z div.mw-category (kategorie questów) i zwraca pełne URL-e.
    """
    if not tresc:
        return []

    baza_url = "https://warcraft.wiki.gg"
    # albo kraina albo przedzialy poziomow
    kontener = tresc.select_one("div.mw-category-columns") or tresc.select_one("div.mw-category")
    if not kontener:
        return []

    wynik = []
    for a in kontener.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if not href.startswith("/wiki/"):
            continue

        wynik.append(f"{baza_url}{href}")

    return list(dict.fromkeys(wynik))


def wyszukaj_link_nastepnej_strony_kategorii(tresc) -> str | None:
    if not tresc:
        return None

    baza_url = "https://warcraft.wiki.gg"

    kontener = tresc.find(id="mw-pages") or tresc.select_one("div.mw-category")
    if not kontener:
        kontener = tresc

    for a in kontener.find_all("a", href=True):
        txt = a.get_text(" ", strip=True).lower()
        if txt != "next page":
            continue

        href = (a.get("href") or "").strip()
        if not href:
            continue

        if href.startswith("/"):
            return f"{baza_url}{href}"
        if href.startswith("http"):
            return href

    return None