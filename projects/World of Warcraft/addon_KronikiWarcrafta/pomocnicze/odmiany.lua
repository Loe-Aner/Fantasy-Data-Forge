local nazwaAddonu, prywatna_tabela = ...

local SlownikKlas = {
    -- 1. WARRIOR
    ["WARRIOR"] = {
        ["Male"] = {
            ["mianownik"]  = "Wojownik",     -- Kto? Co?
            ["dopelniacz"] = "Wojownika",    -- Kogo? Czego?
            ["celownik"]   = "Wojownikowi",  -- Komu? Czemu?
            ["biernik"]    = "Wojownika",    -- Kogo? Co? (Widzę)
            ["narzednik"]  = "Wojownikiem",  -- Z kim? Z czym?
            ["miejscownik"]= "Wojowniku",    -- O kim? O czym?
            ["wolacz"]     = "Wojowniku"     -- O...! (Witaj)
        },
        ["Female"] = {
            ["mianownik"]  = "Wojowniczka",
            ["dopelniacz"] = "Wojowniczki",
            ["celownik"]   = "Wojowniczce",
            ["biernik"]    = "Wojowniczkę",
            ["narzednik"]  = "Wojowniczką",
            ["miejscownik"]= "Wojowniczce",
            ["wolacz"]     = "Wojowniczko"
        }
    },

    -- 2. PALADIN
    ["PALADIN"] = {
        ["Male"] = {
            ["mianownik"]  = "Paladyn",
            ["dopelniacz"] = "Paladyna",
            ["celownik"]   = "Paladynowi",
            ["biernik"]    = "Paladyna",
            ["narzednik"]  = "Paladynem",
            ["miejscownik"]= "Paladynie",
            ["wolacz"]     = "Paladynie"
        },
        ["Female"] = {
            ["mianownik"]  = "Paladynka",
            ["dopelniacz"] = "Paladynki",
            ["celownik"]   = "Paladynce",
            ["biernik"]    = "Paladynkę",
            ["narzednik"]  = "Paladynką",
            ["miejscownik"]= "Paladynce",
            ["wolacz"]     = "Paladynko"
        }
    },

    -- 3. HUNTER
    ["HUNTER"] = {
        ["Male"] = {
            ["mianownik"]  = "Łowca",
            ["dopelniacz"] = "Łowcy",
            ["celownik"]   = "Łowcy",
            ["biernik"]    = "Łowcę",
            ["narzednik"]  = "Łowcą",
            ["miejscownik"]= "Łowcy",
            ["wolacz"]     = "Łowco"
        },
        ["Female"] = {
            ["mianownik"]  = "Łowczyni",
            ["dopelniacz"] = "Łowczyni",
            ["celownik"]   = "Łowczyni",
            ["biernik"]    = "Łowczynię",
            ["narzednik"]  = "Łowczynią",
            ["miejscownik"]= "Łowczyni",
            ["wolacz"]     = "Łowczyni"
        }
    },

    -- 4. ROGUE
    ["ROGUE"] = {
        ["Male"] = {
            ["mianownik"]  = "Łotr",
            ["dopelniacz"] = "Łotra",
            ["celownik"]   = "Łotrowi",
            ["biernik"]    = "Łotra",
            ["narzednik"]  = "Łotrem",
            ["miejscownik"]= "Łotrze",
            ["wolacz"]     = "Łotrze"
        },
        ["Female"] = {
            ["mianownik"]  = "Łotrzyca",
            ["dopelniacz"] = "Łotrzycy",
            ["celownik"]   = "Łotrzycy",
            ["biernik"]    = "Łotrzycę",
            ["narzednik"]  = "Łotrzycą",
            ["miejscownik"]= "Łotrzycy",
            ["wolacz"]     = "Łotrzyco"
        }
    },

    -- 5. PRIEST
    ["PRIEST"] = {
        ["Male"] = {
            ["mianownik"]  = "Kapłan",
            ["dopelniacz"] = "Kapłana",
            ["celownik"]   = "Kapłanowi",
            ["biernik"]    = "Kapłana",
            ["narzednik"]  = "Kapłanem",
            ["miejscownik"]= "Kapłanie",
            ["wolacz"]     = "Kapłanie"
        },
        ["Female"] = {
            ["mianownik"]  = "Kapłanka",
            ["dopelniacz"] = "Kapłanki",
            ["celownik"]   = "Kapłance",
            ["biernik"]    = "Kapłankę",
            ["narzednik"]  = "Kapłanką",
            ["miejscownik"]= "Kapłance",
            ["wolacz"]     = "Kapłanko"
        }
    },

    -- 6. DEATH KNIGHT
    ["DEATHKNIGHT"] = {
        ["Male"] = {
            ["mianownik"]  = "Rycerz Śmierci",
            ["dopelniacz"] = "Rycerza Śmierci",
            ["celownik"]   = "Rycerzowi Śmierci",
            ["biernik"]    = "Rycerza Śmierci",
            ["narzednik"]  = "Rycerzem Śmierci",
            ["miejscownik"]= "Rycerzu Śmierci",
            ["wolacz"]     = "Rycerzu Śmierci"
        },
        ["Female"] = {
            ["mianownik"]  = "Rycerz Śmierci",
            ["dopelniacz"] = "Rycerza Śmierci",
            ["celownik"]   = "Rycerzowi Śmierci",
            ["biernik"]    = "Rycerza Śmierci",
            ["narzednik"]  = "Rycerzem Śmierci",
            ["miejscownik"]= "Rycerzu Śmierci",
            ["wolacz"]     = "Rycerzu Śmierci"
        }
    },

    -- 7. SHAMAN
    ["SHAMAN"] = {
        ["Male"] = {
            ["mianownik"]  = "Szaman",
            ["dopelniacz"] = "Szamana",
            ["celownik"]   = "Szamanowi",
            ["biernik"]    = "Szamana",
            ["narzednik"]  = "Szamanem",
            ["miejscownik"]= "Szamanie",
            ["wolacz"]     = "Szamanie"
        },
        ["Female"] = {
            ["mianownik"]  = "Szamanka",
            ["dopelniacz"] = "Szamanki",
            ["celownik"]   = "Szamance",
            ["biernik"]    = "Szamankę",
            ["narzednik"]  = "Szamanką",
            ["miejscownik"]= "Szamance",
            ["wolacz"]     = "Szamanko"
        }
    },

    -- 8. MAGE
    ["MAGE"] = {
        ["Male"] = {
            ["mianownik"]  = "Czarodziej",
            ["dopelniacz"] = "Czarodzieja",
            ["celownik"]   = "Czarodziejowi",
            ["biernik"]    = "Czarodzieja",
            ["narzednik"]  = "Czarodziejem",
            ["miejscownik"]= "Czarodzieju",
            ["wolacz"]     = "Czarodzieju"
        },
        ["Female"] = {
            ["mianownik"]  = "Czarodziejka",
            ["dopelniacz"] = "Czarodziejki",
            ["celownik"]   = "Czarodziejce",
            ["biernik"]    = "Czarodziejkę",
            ["narzednik"]  = "Czarodziejką",
            ["miejscownik"]= "Czarodziejce",
            ["wolacz"]     = "Czarodziejko"
        }
    },

    -- 9. WARLOCK
    ["WARLOCK"] = {
        ["Male"] = {
            ["mianownik"]  = "Czarnoksiężnik",
            ["dopelniacz"] = "Czarnoksiężnika",
            ["celownik"]   = "Czarnoksiężnikowi",
            ["biernik"]    = "Czarnoksiężnika",
            ["narzednik"]  = "Czarnoksiężnikiem",
            ["miejscownik"]= "Czarnoksiężniku",
            ["wolacz"]     = "Czarnoksiężniku"
        },
        ["Female"] = {
            ["mianownik"]  = "Czarnoksiężniczka",
            ["dopelniacz"] = "Czarnoksiężniczki",
            ["celownik"]   = "Czarnoksiężniczce",
            ["biernik"]    = "Czarnoksiężniczkę",
            ["narzednik"]  = "Czarnoksiężniczką",
            ["miejscownik"]= "Czarnoksiężniczce",
            ["wolacz"]     = "Czarnoksiężniczko"
        }
    },

    -- 10. MONK
    ["MONK"] = {
        ["Male"] = {
            ["mianownik"]  = "Mnich",
            ["dopelniacz"] = "Mnicha",
            ["celownik"]   = "Mnichowi",
            ["biernik"]    = "Mnicha",
            ["narzednik"]  = "Mnichem",
            ["miejscownik"]= "Mnichu",
            ["wolacz"]     = "Mnichu"
        },
        ["Female"] = {
            ["mianownik"]  = "Mniszka",
            ["dopelniacz"] = "Mniszki",
            ["celownik"]   = "Mniszce",
            ["biernik"]    = "Mniszkę",
            ["narzednik"]  = "Mniszką",
            ["miejscownik"]= "Mniszce",
            ["wolacz"]     = "Mniszko"
        }
    },

    -- 11. DRUID
    ["DRUID"] = {
        ["Male"] = {
            ["mianownik"]  = "Druid",
            ["dopelniacz"] = "Druida",
            ["celownik"]   = "Druidowi",
            ["biernik"]    = "Druida",
            ["narzednik"]  = "Druidem",
            ["miejscownik"]= "Druidzie",
            ["wolacz"]     = "Druidzie"
        },
        ["Female"] = {
            ["mianownik"]  = "Druidka",
            ["dopelniacz"] = "Druidki",
            ["celownik"]   = "Druidce",
            ["biernik"]    = "Druidkę",
            ["narzednik"]  = "Druidką",
            ["miejscownik"]= "Druidce",
            ["wolacz"]     = "Druidko"
        }
    },

    -- 12. DEMON HUNTER
    ["DEMONHUNTER"] = {
        ["Male"] = {
            ["mianownik"]  = "Łowca Demonów",
            ["dopelniacz"] = "Łowcy Demonów",
            ["celownik"]   = "Łowcy Demonów",
            ["biernik"]    = "Łowcę Demonów",
            ["narzednik"]  = "Łowcą Demonów",
            ["miejscownik"]= "Łowcy Demonów",
            ["wolacz"]     = "Łowco Demonów"
        },
        ["Female"] = {
            ["mianownik"]  = "Łowczyni Demonów",
            ["dopelniacz"] = "Łowczyni Demonów",
            ["celownik"]   = "Łowczyni Demonów",
            ["biernik"]    = "Łowczynię Demonów",
            ["narzednik"]  = "Łowczynią Demonów",
            ["miejscownik"]= "Łowczyni Demonów",
            ["wolacz"]     = "Łowczyni Demonów"
        }
    },
    
    -- 13. ZASTANOWIC SIE
    ["EVOKER"] = {
        ["Male"] = {
            ["mianownik"]  = "Ewoker",
            ["dopelniacz"] = "Ewokera",
            ["celownik"]   = "Ewokerowi",
            ["biernik"]    = "Ewokera",
            ["narzednik"]  = "Ewokerem",
            ["miejscownik"]= "Ewokerze",
            ["wolacz"]     = "Ewokerze"
        },
        ["Female"] = {
            ["mianownik"]  = "Ewokerka",
            ["dopelniacz"] = "Ewokerki",
            ["celownik"]   = "Ewokerce",
            ["biernik"]    = "Ewokerkę",
            ["narzednik"]  = "Ewokerką",
            ["miejscownik"]= "Ewokerce",
            ["wolacz"]     = "Ewokerko"
        }
    }
}