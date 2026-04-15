from typing import Literal, TypedDict


class PodsumowanieEN(TypedDict):
    Tytuł: str


class CeleEN(TypedDict):
    Główny: dict[str, str]
    Podrzędny: dict[str, str]


class GossipyDymkiEN(TypedDict):
    id: int
    typ: Literal["gossip", "dymek"]
    npc_en: str
    wypowiedzi_EN: dict[str, str]


class MisjeEN(TypedDict):
    Podsumowanie_EN: PodsumowanieEN
    Cele_EN: CeleEN
    Treść_EN: dict[str, str]
    Postęp_EN: dict[str, str]
    Zakończenie_EN: dict[str, str]
    Nagrody_EN: dict[str, str]
    

class DialogiEN(TypedDict):
    Gossipy_Dymki_EN: list[GossipyDymkiEN]


class OdpowiedzTlumacza(TypedDict):
    Misje_EN: MisjeEN
    Dialogi_EN: DialogiEN