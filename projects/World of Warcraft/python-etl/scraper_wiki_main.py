import asyncio
import atexit
import os
import re
import time
import hashlib
import zlib
import base64
from dataclasses import dataclass
from weakref import WeakKeyDictionary

import httpx
from bs4 import BeautifulSoup
from tenacity import AsyncRetrying, RetryError, retry_if_exception_type, stop_after_attempt, wait_exponential

try:
    from requests_cache import AsyncCachedSession
except Exception:
    AsyncCachedSession = None


USER_AGENT = "WoW_PolishTranslationProject -> (reachable on your Discord: Loe'Aner)"
DEFAULT_TIMEOUT = 30

ENABLE_CACHE = os.getenv("SCRAPER_CACHE_ENABLED", "0").lower() in {"1", "true", "yes"}
CACHE_NAME = os.getenv("SCRAPER_CACHE_NAME", "wow_scraper_cache")
CACHE_EXPIRE = int(os.getenv("SCRAPER_CACHE_EXPIRE", "86400"))

WIKI_MAX_CONCURRENCY = int(os.getenv("WIKI_MAX_CONCURRENCY", "5"))
WOWHEAD_MAX_CONCURRENCY = int(os.getenv("WOWHEAD_MAX_CONCURRENCY", "5"))

WIKI_DELAY = float(os.getenv("WIKI_DELAY_SECONDS", "0.4"))
WOWHEAD_DELAY = float(os.getenv("WOWHEAD_DELAY_SECONDS", "0.4"))


class HostThrottle:
    def __init__(self, min_delay: float):
        self.min_delay = max(0.0, min_delay)
        self._lock = asyncio.Lock()
        self._last_call = 0.0

    async def wait(self) -> None:
        async with self._lock:
            elapsed = time.monotonic() - self._last_call
            remaining = self.min_delay - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)
            self._last_call = time.monotonic()


@dataclass
class _LoopState:
    wiki_semaphore: asyncio.Semaphore
    wowhead_semaphore: asyncio.Semaphore
    throttles: dict


_LOOP_STATES: "WeakKeyDictionary[asyncio.AbstractEventLoop, _LoopState]" = WeakKeyDictionary()

def _get_loop_state() -> _LoopState:
    loop = asyncio.get_running_loop()
    state = _LOOP_STATES.get(loop)
    if state is None:
        state = _LoopState(
            wiki_semaphore=asyncio.Semaphore(WIKI_MAX_CONCURRENCY),
            wowhead_semaphore=asyncio.Semaphore(WOWHEAD_MAX_CONCURRENCY),
            throttles={
                "wiki": HostThrottle(WIKI_DELAY),
                "wowhead": HostThrottle(WOWHEAD_DELAY),
            },
        )
        _LOOP_STATES[loop] = state
    return state


_CLIENT: httpx.AsyncClient | None = None

def _build_client() -> httpx.AsyncClient:
    common_kwargs = {
        "headers": {"User-Agent": USER_AGENT},
        "timeout": DEFAULT_TIMEOUT,
        "follow_redirects": True,
    }

    if ENABLE_CACHE and AsyncCachedSession is not None:
        return AsyncCachedSession(
            cache_name=CACHE_NAME,
            backend="sqlite",
            expire_after=CACHE_EXPIRE,
            allowable_methods=["GET"],
            **common_kwargs,
        )

    return httpx.AsyncClient(**common_kwargs)

def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = _build_client()
    return _CLIENT

async def _aclose_client() -> None:
    global _CLIENT
    if _CLIENT is None:
        return
    client = _CLIENT
    _CLIENT = None
    await client.aclose()


_RUNNER: asyncio.Runner | None = None

def _get_runner() -> asyncio.Runner:
    global _RUNNER
    if _RUNNER is None:
        _RUNNER = asyncio.Runner()
    return _RUNNER

def _shutdown_runner_and_client() -> None:
    global _RUNNER
    if _RUNNER is None:
        return
    try:
        _RUNNER.run(_aclose_client())
    except Exception:
        pass
    try:
        _RUNNER.close()
    finally:
        _RUNNER = None

atexit.register(_shutdown_runner_and_client)


async def _fetch_html(url: str, host: str) -> str:
    client = _get_client()
    state = _get_loop_state()

    semaphore = state.wiki_semaphore if host == "wiki" else state.wowhead_semaphore
    throttle = state.throttles[host]

    async def _do_request() -> httpx.Response:
        async with semaphore:
            await throttle.wait()
            response = await client.get(url)

        if response.status_code in (429, 502, 503):
            raise httpx.HTTPStatusError(
                f"{response.status_code} status for {url}",
                request=response.request,
                response=response,
            )

        response.raise_for_status()
        return response

    try:
        async for attempt in AsyncRetrying(
            sleep=asyncio.sleep,
            stop=stop_after_attempt(6),
            wait=wait_exponential(multiplier=1, min=2, max=60),
            retry=retry_if_exception_type(httpx.HTTPError),
            reraise=True,
        ):
            with attempt:
                response = await _do_request()
                return response.text
    except RetryError as exc:
        print(f"SKIP: nie udało się pobrać {url} po 6 próbach: {exc}")
        raise


async def pobierz_soup_async(url: str, parser: str = "html.parser", host: str | None = None) -> BeautifulSoup | None:
    target_host = host or ("wowhead" if "wowhead" in url else "wiki")
    try:
        html = await _fetch_html(url, target_host)
        return BeautifulSoup(html, parser)
    except (httpx.HTTPError, RetryError) as e:
        print(f"Błąd pobierania {url}: {e}")
        return None


def pobierz_soup(url: str, parser: str = "html.parser", host: str | None = None) -> BeautifulSoup | None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import nest_asyncio
        nest_asyncio.apply()
        
        return loop.run_until_complete(pobierz_soup_async(url, parser=parser, host=host))
    else:
        runner = _get_runner()
        return runner.run(pobierz_soup_async(url, parser=parser, host=host))


def pobierz_tresc(soup: BeautifulSoup):
    return soup.select_one("#bodyContent")


def normalizuj_tekst(tekst: str) -> str:
    if not tekst:
        return ""
    tekst = tekst.replace("\xa0", " ")
    tekst = tekst.replace("\r\n", "\n").replace("\r", "\n")
    linie = [linia.strip() for linia in tekst.split("\n")]
    linie = [linia for linia in linie if linia]
    return "\n".join(linie)


def policz_hash_z_tekstu(tekst: str) -> str | None:
    tekst = normalizuj_tekst(tekst)
    if not tekst:
        return None
    return hashlib.sha256(tekst.encode("utf-8")).hexdigest()


def skompresuj_html(tresc_tag) -> str | None:
    if not tresc_tag:
        return None
    try:
        html_str = str(tresc_tag)
        skompresowane_bajty = zlib.compress(html_str.encode("utf-8"))
        zakodowane_base64 = base64.b64encode(skompresowane_bajty).decode("utf-8")
        return zakodowane_base64
    except Exception as e:
        print(f"Błąd kompresji: {e}")
        return None

def dekompresuj_html(zakodowany_string: str) -> str:
    if not zakodowany_string:
        return ""
    try:
        skompresowane_bajty = base64.b64decode(zakodowany_string)
        html_str = zlib.decompress(skompresowane_bajty).decode("utf-8")
        return html_str
    except Exception as e:
        print(f"Błąd dekompresji: {e}")
        return ""


def złącz_slownik_linii(slownik_linii: dict) -> str:
    if not slownik_linii:
        return ""
    return "\n".join(
        normalizuj_tekst(slownik_linii[k])
        for k in sorted(slownik_linii.keys())
        if slownik_linii.get(k)
    ).strip()


def złącz_cele(cele_slownik: dict) -> dict:
    glowny = złącz_slownik_linii((cele_slownik or {}).get("Główny") or {})
    podrzedny = złącz_slownik_linii((cele_slownik or {}).get("Podrzędny") or {})
    return {"Główny": glowny, "Podrzędny": podrzedny}


def złącz_dialogi(sequence: list, typy: set[str]) -> str:
    if not sequence:
        return ""

    bloki = []
    for el in sequence:
        typ = normalizuj_tekst(el.get("typ", ""))
        if typ not in typy:
            continue

        npc = normalizuj_tekst(el.get("npc_en", ""))
        wyp = el.get("wypowiedzi_EN") or {}
        tekst = złącz_slownik_linii(wyp)
        naglowek = " | ".join([x for x in [typ, npc] if x]).strip()

        if naglowek and tekst:
            bloki.append(f"{naglowek}\n{tekst}")
        elif tekst:
            bloki.append(tekst)
        elif naglowek:
            bloki.append(naglowek)

    return "\n\n".join([b for b in bloki if b]).strip()


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


def wyczysc_tekst_en(tekst: str) -> str:
    if not tekst:
        return ""

    tekst = tekst.replace("\xa0", " ")
    tekst = tekst.replace("(Optional)", "").replace("(provided)", "")
    tekst = re.sub(r"\s*\(\d+\)\s*$", "", tekst)

    return tekst.strip()


def parsuj_wspolna_kolejnosc_gossipow_i_dymkow(tresc):
    if not tresc:
        return []

    wynik = []
    licznik = 0

    for el in tresc.descendants:
        if not hasattr(el, "name"):
            continue

        if el.name == "div" and "dialogue" in (el.get("class") or []):
            tytul = el.find(class_="dialogue-title")
            npc_en = ""
            if tytul:
                p = tytul.find("p")
                if p:
                    npc_en = wyczysc_tekst_en(p.get_text())

            teksty = []
            for p in el.find_all("p"):
                if tytul and tytul.find("p") == p:
                    continue
                t = p.get_text()
                t = wyczysc_tekst_en(t)
                if t:
                    teksty.append(t)

            tekst_en = "\n".join(teksty)

            licznik += 1
            wynik.append({
                "id": licznik,
                "typ": "gossip",
                "npc_en": npc_en,
                "tekst_en": tekst_en
            })

        elif el.name == "span" and any(
            cls in (el.get("class") or [])
            for cls in ("text-say", "text-bossemote", "text-yell")
        ):
            b = el.find("b")
            if not b:
                continue

            prefix = wyczysc_tekst_en(b.get_text())
            npc_en = prefix.replace("says:", "").strip()

            el_copy = BeautifulSoup(str(el), "html.parser").select_one("span")
            b2 = el_copy.find("b")
            b2.extract()

            tekst_en = wyczysc_tekst_en(el_copy.get_text())

            licznik += 1
            wynik.append({
                "id": licznik,
                "typ": "dymek",
                "npc_en": npc_en.replace(":", "").replace("yells", "").strip(),
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


def parsuj_misje_z_url(url: str, html_content: str = None):
    if html_content:
        soup = BeautifulSoup(html_content, "html.parser")
        tresc = pobierz_tresc(soup)
        html_skompresowany = skompresuj_html(tresc)
    else:
        soup = pobierz_soup(url)
        tresc = pobierz_tresc(soup)
        html_skompresowany = skompresuj_html(tresc)

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
            "Podrzędny": policz_hash_z_tekstu(cele_zlaczone["Podrzędny"])
        },
        "Treść_EN": policz_hash_z_tekstu(złącz_slownik_linii(opis)),
        "Postęp_EN": policz_hash_z_tekstu(złącz_slownik_linii(postep)),
        "Zakończenie_EN": policz_hash_z_tekstu(złącz_slownik_linii(zakonczenie)),
        "Nagrody_EN": policz_hash_z_tekstu(złącz_slownik_linii(nagrody)),
        "Dialogi_EN": {
            "Dymki_EN": policz_hash_z_tekstu(złącz_dialogi(sequence, {"dymek"})),
            "Gossipy_EN": policz_hash_z_tekstu(złącz_dialogi(sequence, {"gossip"}))
        }
    }

    return {
        "Źródło": {
            "url": url,
            "html_skompresowany": html_skompresowany 
        },
        "Misje_EN": {
            "Podsumowanie_EN": podsumowanie,
            "Cele_EN": cele,
            "Treść_EN": opis,
            "Postęp_EN": postep,
            "Zakończenie_EN": zakonczenie,
            "Nagrody_EN": nagrody
        },
        "Dialogi_EN": {
            "Gossipy_Dymki_EN": sequence
        },
        "Hash_HTML": hash_sekcji
    }