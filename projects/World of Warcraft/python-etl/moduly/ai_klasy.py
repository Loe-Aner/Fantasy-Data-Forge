from typing import Literal

from langchain_core.messages import AIMessage
from typing_extensions import Annotated, TypedDict


class QuestSummaryPL(TypedDict):
    """Podsumowanie misji."""

    Tytuł: Annotated[str, ..., "Tytuł misji"]


class QuestObjectivesPL(TypedDict):
    """Cele misji."""

    Główny: Annotated[dict[str, str], ..., "Główne cele misji mapowane numerem linii"]
    Podrzędny: Annotated[dict[str, str], ..., "Podrzędne cele misji mapowane numerem linii"]


class DialogueBlockPL(TypedDict):
    """Blok dialogowy misji."""

    id: Annotated[int, ..., "Identyfikator bloku dialogowego"]
    typ: Annotated[Literal["gossip", "dymek"], ..., "Typ bloku dialogowego"]
    npc_pl: Annotated[str, ..., "Polska nazwa NPC wypowiadającego kwestie w tym bloku"]
    wypowiedzi_PL: Annotated[dict[str, str], ..., "Kwestie dialogowe mapowane numerem linii"]


class QuestPL(TypedDict):
    """Polska treść misji."""

    Podsumowanie_PL: Annotated[QuestSummaryPL, ..., "Podsumowanie misji"]
    Cele_PL: Annotated[QuestObjectivesPL, ..., "Cele misji"]
    Treść_PL: Annotated[dict[str, str], ..., "Główna treść misji mapowana numerem linii"]
    Postęp_PL: Annotated[dict[str, str], ..., "Teksty postępu misji mapowane numerem linii"]
    Zakończenie_PL: Annotated[dict[str, str], ..., "Teksty zakończenia misji mapowane numerem linii"]
    Nagrody_PL: Annotated[dict[str, str], ..., "Sekcja nagród mapowana numerem linii"]


class DialoguesPL(TypedDict):
    """Polskie dialogi misji."""

    Gossipy_Dymki_PL: Annotated[list[DialogueBlockPL], ..., "Lista bloków dialogowych"]


class QuestContentResponse(TypedDict):
    """Pełna polska treść misji i dialogów."""

    Misje_PL: Annotated[QuestPL, ..., "Polska treść misji"]
    Dialogi_PL: Annotated[DialoguesPL, ..., "Polskie dialogi misji"]


class QuestContentResult(TypedDict):
    raw: AIMessage
    parsed: QuestContentResponse | None
    parsing_error: BaseException | None
