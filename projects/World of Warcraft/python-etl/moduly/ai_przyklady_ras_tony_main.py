from dataclasses import dataclass, field
from typing import List

#================================== NA KONCU DODAC DO KLASY PRZYKLADY ==================================

@dataclass(slots=True)
class RaceStyle:
    race_name: str
    label: str
    short_profile: str
    tone: str
    lexicon: str
    syntax_rhythm: str
    voice: str
    naturalness: str
    avoid: str
    mini_rules: List[str] = field(default_factory=list)
    llm_instruction: str = ""