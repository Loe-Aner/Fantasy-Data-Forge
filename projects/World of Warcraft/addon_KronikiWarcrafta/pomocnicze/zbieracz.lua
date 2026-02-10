local nazwaAddonu, prywatna_tabela = ...

-- === 1. DANE GRACZA ===
local ImieGracza = UnitName("player")
-- UnitRace zwraca: nazwa lokalna, nazwa angielska. Biore te druga (angielska) dla bezpieczenstwa
local _, RasaGracza = UnitRace("player") 
local RasaGraczaMala = string.lower(RasaGracza or "")

-- UnitClass zwraca: nazwa lokalna, TAG (angielski). Biore TAG.
local _, KlasaGracza = UnitClass("player") 
-- Klasa (TAG) jest zawsze z duzych (np. WARRIOR), wiec robie formatowanie:
-- Zamieniam np. WARRIOR na Warrior (zeby pasowalo do tekstow w questach)
KlasaGracza = string.upper(string.sub(KlasaGracza, 1, 1)) .. string.lower(string.sub(KlasaGracza, 2))
local KlasaGraczaMala = string.lower(KlasaGracza)


-- === 2. FUNKCJA NORMALIZUJĄCA ===
local function NormalizujTekst(tekst)
    if not tekst or tekst == "" then return "" end

    -- 1. Imie
    tekst = string.gsub(tekst, ImieGracza, "{imie}")

    -- 2. Rasa (np. Human -> {rasa})
    if RasaGracza then
        tekst = string.gsub(tekst, RasaGracza, "{rasa}")
        tekst = string.gsub(tekst, RasaGraczaMala, "{rasa}")
    end

    -- 3. Klasa (np. Warrior -> {klasa})
    if KlasaGracza then
        tekst = string.gsub(tekst, KlasaGracza, "{klasa}")
        tekst = string.gsub(tekst, KlasaGraczaMala, "{klasa}")
    end

    return tekst
end


-- === 3. ZAPIS DO BAZY ===
local function ZapiszPojedynczyTekst(TypTekstu, TekstOryginalny, MisjaID)
    if not TekstOryginalny or TekstOryginalny == "" then 
        return 
    end

    local TekstZnormalizowany = NormalizujTekst(TekstOryginalny)
    local hash_tekstu = prywatna_tabela.GenerujHash(TekstZnormalizowany)
    
    if not hash_tekstu then 
        return 
        end

    local BazaBrakujacych = KronikiDB_Nieprzetlumaczone["ListaMisji"]

    if not BazaBrakujacych[hash_tekstu] then
        BazaBrakujacych[hash_tekstu] = {
            ["MISJA_ID"] = MisjaID,
            ["TYP"] = TypTekstu,
            ["TEKST_ENG"] = TekstZnormalizowany, 
            ["TEKST_RAW"] = TekstOryginalny, 
            ["HASH"] = hash_tekstu
        }
        print("|cff00ccff[Kroniki]|r Nowy znormalizowany wpis: " .. hash_tekstu)
    end
end


-- === 4. GLOWNA FUNKCJA ZBIERACZA ===
prywatna_tabela["ZbierajMisje"] = function (self, event)
    local MisjaID = GetQuestID()

    local TytulMisji = GetTitleText()
    ZapiszPojedynczyTekst("TYTUŁ", TytulMisji, MisjaID)

    if event == "QUEST_DETAIL" then
        local TrescMisji = GetQuestText()
        ZapiszPojedynczyTekst("TREŚĆ", TrescMisji, MisjaID)

        local CelMisji = GetObjectiveText()
        ZapiszPojedynczyTekst("CEL", CelMisji, MisjaID)

    elseif event == "QUEST_PROGRESS" then
        local PostepMisji = GetProgressText()
        ZapiszPojedynczyTekst("POSTĘP", PostepMisji, MisjaID)

    elseif event == "QUEST_COMPLETE" then
        local ZakonczenieMisji = GetRewardText()
        ZapiszPojedynczyTekst("ZAKOŃCZENIE", ZakonczenieMisji, MisjaID)
    end
end