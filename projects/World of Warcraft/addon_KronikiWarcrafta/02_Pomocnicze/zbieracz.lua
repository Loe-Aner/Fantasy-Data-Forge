local nazwaAddonu, prywatna_tabela = ...

-- === 0. CACHE ===
local UnitName = UnitName
local UnitRace = UnitRace
local UnitClass = UnitClass
local UnitSex = UnitSex
local GetQuestID = GetQuestID
local GetTitleText = GetTitleText
local GetQuestText = GetQuestText
local GetObjectiveText = GetObjectiveText
local GetProgressText = GetProgressText
local GetRewardText = GetRewardText
-- Biblioteki Lua i funkcje podstawowe
local string = string
local table = table
local ipairs = ipairs
local print = print

-- === 1. DANE GRACZA ===
local ImieGracza = UnitName("player")
-- UnitRace zwraca: nazwa lokalna, nazwa angielska. Biore te druga (angielska) dla bezpieczenstwa
local _, RasaGracza = UnitRace("player") 
local RasaGraczaMala = string.lower(RasaGracza or "")

-- UnitClass zwraca: nazwa lokalna, TAG (angielski). Biore TAG.
local _, KlasaTag = UnitClass("player")
local KlasaKlucz = KlasaTag
local KlasaGracza = string.upper(string.sub(KlasaTag, 1, 1)) .. string.lower(string.sub(KlasaTag, 2))
local KlasaGraczaMala = string.lower(KlasaTag)


-- === 2. FUNKCJA NORMALIZUJĄCA ===
local function NormalizujTekst(Tekst)
    if not Tekst or Tekst == "" then 
      return "" 
   end

    Tekst = string.match(Tekst, "^%s*(.-)%s*$") -- usuwam niepotrzebne spacje z surowego tekstu

    -- 1. Imie (np. Loe'Aner -> {imie})
    Tekst = string.gsub(Tekst, ImieGracza, "{imie}")

    -- 2. Rasa (np. Human -> {rasa})
    if RasaGracza then
        Tekst = string.gsub(Tekst, RasaGracza, "{rasa}")
        Tekst = string.gsub(Tekst, RasaGraczaMala, "{rasa}")
    end

    -- 3. Klasa (np. Warrior -> {klasa})
    if KlasaGracza then
        Tekst = string.gsub(Tekst, KlasaGracza, "{klasa}")
        Tekst = string.gsub(Tekst, KlasaGraczaMala, "{klasa}")
    end

    return Tekst
end

local function PodzielTekst(TekstOryginalny, sep)
   if sep == nil or TekstOryginalny == nil then
      return {}
   end
   
   local TekstPodzielony = {}
   local Wzorzec = "([^" .. sep .. "]+)"
   
   for str in string.gmatch(TekstOryginalny, Wzorzec) do
      table.insert(TekstPodzielony, str)
   end
   return TekstPodzielony
end

-- === 3. ZAPIS DO BAZY ===
local function ZapiszPojedynczyTekst(TypTekstu, TekstOryginalny, MisjaID)
   if not TekstOryginalny or TekstOryginalny == "" then 
      return ""
   end

   local TekstPodzielony = PodzielTekst(TekstOryginalny, "\r\n\r\n")
   local BazaBrakujacych = KronikiDB_Nieprzetlumaczone["ListaMisji"]

   for _, PojedynczaLinia in ipairs(TekstPodzielony) do
      local TekstZnormalizowany = NormalizujTekst(PojedynczaLinia)
      local HashTekstu = prywatna_tabela.GenerujHash(TekstZnormalizowany)
      
      if HashTekstu then 
         if not BazaBrakujacych[HashTekstu] then
            BazaBrakujacych[HashTekstu] = {
               ["MISJA_ID"] = MisjaID,
               ["TYP"] = TypTekstu,
               ["TEKST_ENG"] = TekstZnormalizowany, 
               ["TEKST_RAW"] = PojedynczaLinia
            }
            print("|cff00ccff[Kroniki]|r Dodano nieprzetłumaczony rekord: " .. HashTekstu)
         end
      end
   end
end

-- === 4. GLOWNA FUNKCJA ZBIERACZA ===
prywatna_tabela["ZbierajMisje"] = function(self, event)
    local MisjaID = GetQuestID()
    if not MisjaID or MisjaID == 0 then return end

    ZapiszPojedynczyTekst("TYTUŁ", GetTitleText(), MisjaID)

    if event == "QUEST_DETAIL" then
        ZapiszPojedynczyTekst("CEL",   GetObjectiveText(), MisjaID)
        ZapiszPojedynczyTekst("TREŚĆ", GetQuestText(), MisjaID)

    elseif event == "QUEST_PROGRESS" then
        ZapiszPojedynczyTekst("POSTĘP", GetProgressText(), MisjaID)

    elseif event == "QUEST_COMPLETE" then
        ZapiszPojedynczyTekst("ZAKOŃCZENIE", GetRewardText(), MisjaID)
    end
end

-- przerzut do globalnej tablicy
prywatna_tabela["NormalizujTekst"] = NormalizujTekst
prywatna_tabela["PodzielTekst"] = PodzielTekst

prywatna_tabela["DaneGracza"] = {
    ["KlasaKlucz"] = KlasaKlucz,
    ["RasaKlucz"]  = string.upper(string.gsub(RasaGracza or "", "%s+", "")),
    ["Plec"]       = UnitSex("player") == 2 and "Male" or "Female"
}