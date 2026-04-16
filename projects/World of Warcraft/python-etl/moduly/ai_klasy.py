from typing import Literal, TypedDict

from langchain_core.messages import AIMessage


class PodsumowaniePL(TypedDict):
    Tytuł: str


class CelePL(TypedDict):
    Główny: dict[str, str]
    Podrzędny: dict[str, str]


class GossipyDymkiPL(TypedDict):
    id: int
    typ: Literal["gossip", "dymek"]
    npc_pl: str
    wypowiedzi_PL: dict[str, str]


class MisjePL(TypedDict):
    Podsumowanie_PL: PodsumowaniePL
    Cele_PL: CelePL
    Treść_PL: dict[str, str]
    Postęp_PL: dict[str, str]
    Zakończenie_PL: dict[str, str]
    Nagrody_PL: dict[str, str]
    

class DialogiPL(TypedDict):
    Gossipy_Dymki_PL: list[GossipyDymkiPL]


class OdpowiedzTlumacza(TypedDict):
    Misje_PL: MisjePL
    Dialogi_PL: DialogiPL


class WynikTranslatora(TypedDict):
    raw: AIMessage
    parsed: OdpowiedzTlumacza | None
    parsing_error: BaseException | None
