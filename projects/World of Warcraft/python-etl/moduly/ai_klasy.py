from typing import Literal, TypedDict
from pydantic import BaseModel, Field, ConfigDict

from langchain_core.messages import AIMessage


class StrictBaseModel(BaseModel):
    """
    Bazowa klasa Pydantic dla schematów odpowiedzi AI.
    Dziedziczą po niej poniższe klasy, aby nie powtarzać wspólnej konfiguracji modelu.
    """
    model_config = ConfigDict(extra="forbid")


class QuestSummaryPL(StrictBaseModel):
    Tytuł: str = Field(description="Tytuł misji")


class QuestObjectivesPL(StrictBaseModel):
    Główny: dict[str, str] = Field(description="Główne cele misji")
    Podrzędny: dict[str, str] = Field(description="Podrzędne cele misji")


class DialogueBlockPL(StrictBaseModel):
    id: int = Field(description="Identyfikator bloku dialogowego")
    typ: Literal["gossip", "dymek"] = Field(description="Typ bloku dialogowego")
    npc_pl: str = Field(description="Polska nazwa NPC wypowiadającego kwestie w tym bloku")
    wypowiedzi_PL: dict[str, str] = Field(description="Kwestie dialogowe mapowane numerem linii")


class QuestPL(StrictBaseModel):
    Podsumowanie_PL: QuestSummaryPL = Field(description="Podsumowanie misji")
    Cele_PL: QuestObjectivesPL = Field(description="Cele misji")
    Treść_PL: dict[str, str] = Field(description="Główna treść misji")
    Postęp_PL: dict[str, str] = Field(description="Teksty postępu misji")
    Zakończenie_PL: dict[str, str] = Field(description="Teksty zakończenia misji")
    Nagrody_PL: dict[str, str] = Field(description="Sekcja nagród")
    

class DialoguesPL(StrictBaseModel):
    Gossipy_Dymki_PL: list[DialogueBlockPL] = Field(description="Lista bloków dialogowych")


class QuestContentResponse(StrictBaseModel):
    Misje_PL: QuestPL = Field(description="Polska treść misji")
    Dialogi_PL: DialoguesPL = Field(description="Polskie dialogi misji")


class QuestContentResult(TypedDict):
    raw: AIMessage
    parsed: QuestContentResponse | None
    parsing_error: BaseException | None
