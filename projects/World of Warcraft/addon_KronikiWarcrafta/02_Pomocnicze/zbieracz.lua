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

    Tekst = string.match(Tekst, "^%s*(.-)%s*$")
    Tekst = string.gsub(Tekst, "%f[%a]" .. ImieGracza .. "%f[%A]", "{imie}")

    -- 2. Rasa
    if RasaGracza then
        Tekst = string.gsub(Tekst, "%f[%a]" .. RasaGracza .. "%f[%A]", "{rasa}")
        Tekst = string.gsub(Tekst, "%f[%a]" .. RasaGraczaMala .. "%f[%A]", "{rasa}")
    end

    -- 3. Klasa
    if KlasaGracza then
        Tekst = string.gsub(Tekst, "%f[%a]" .. KlasaGracza .. "%f[%A]", "{klasa}")
        Tekst = string.gsub(Tekst, "%f[%a]" .. KlasaGraczaMala .. "%f[%A]", "{klasa}")
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
local function ZapiszPojedynczyTekst(RodzajTekstu, TypTekstu, TekstOryginalny, MisjaID)
   if not TekstOryginalny or TekstOryginalny == "" then 
      return ""
   end

   local TekstPodzielony = PodzielTekst(TekstOryginalny, "\r\n\r\n")
   local BazaBrakujacychMisji = KronikiDB_Nieprzetlumaczone["ListaMisji"]
   local BazaBrakujacychGossipow = KronikiDB_Nieprzetlumaczone["ListaGossipow"]
   local BazaBrakujacychBubbles = KronikiDB_Nieprzetlumaczone["ListaBubbles"] -- ===========================2do===========================

   local NazwaNPC = UnitName("npc") or "Nieznany"
   local GUID = UnitGUID("npc")
   local npcID
   
   if GUID then
      _, _, _, _, _, npcID = strsplit("-", GUID) -- szosta liczba to id npca
   end
   npcID = npcID or "123456789" -- ta sama flaga jest w mojej DB na 'smieci'

   for _, PojedynczaLinia in ipairs(TekstPodzielony) do

      local TekstZnormalizowany = NormalizujTekst(PojedynczaLinia)
      local HashTekstu = prywatna_tabela.GenerujHash(TekstZnormalizowany)
      local CzyJuzPrzetlumaczone = KronikiDB_Przetlumaczone_0001 and KronikiDB_Przetlumaczone_0001[HashTekstu]
      
      if HashTekstu and not CzyJuzPrzetlumaczone then 
         if RodzajTekstu == "MISJA" then
            if not BazaBrakujacychMisji[HashTekstu] then
               BazaBrakujacychMisji[HashTekstu] = {
                  ["NPC"] = NazwaNPC .. " (" .. npcID .. ")",
                  ["MISJA_ID"] = MisjaID,
                  ["TYP"] = TypTekstu,
                  ["TEKST_ENG"] = TekstZnormalizowany, 
                  ["TEKST_RAW"] = PojedynczaLinia
               }
               print("|cff00ccff[Kroniki]|r Dodano nowy nieprzetłumaczony rekord dla misji: " .. HashTekstu)
            end
         end

         if RodzajTekstu == "GOSSIP" then
            if not BazaBrakujacychGossipow[HashTekstu] then
               BazaBrakujacychGossipow[HashTekstu] = {
                  ["NPC"] = NazwaNPC .. " (" .. npcID .. ")",
                  ["TEKST_ENG"] = TekstZnormalizowany, 
                  ["TEKST_RAW"] = PojedynczaLinia
               }
               print("|cff00ccff[Kroniki]|r Dodano nowy nieprzetłumaczony rekord dla gossipa: " .. HashTekstu)
            end
         end
      end
   end
end

-- === 4. GLOWNA FUNKCJA ZBIERACZA ===
prywatna_tabela["ZbierajMisje"] = function(self, event)
    local MisjaID = GetQuestID()
    if not MisjaID or MisjaID == 0 then return end

    ZapiszPojedynczyTekst("MISJA", "TYTUŁ", GetTitleText(), MisjaID)

    if event == "QUEST_DETAIL" then
        ZapiszPojedynczyTekst("MISJA", "CEL", GetObjectiveText(), MisjaID)
        ZapiszPojedynczyTekst("MISJA", "TREŚĆ", GetQuestText(), MisjaID)

    elseif event == "QUEST_PROGRESS" then
        ZapiszPojedynczyTekst("MISJA", "POSTĘP", GetProgressText(), MisjaID)

    elseif event == "QUEST_COMPLETE" then
        ZapiszPojedynczyTekst("MISJA", "ZAKOŃCZENIE", GetRewardText(), MisjaID)
    end
end

-- prywatna_tabela["ZbierajGossipy"] = function(self, event)
   
--    if event == "GOSSIP_SHOW" then
--       local Gossip = GetGossipText()
--       local WyboryGossip = GetGossipOptions()
--    end
   
-- end

-- przerzut do globalnej tablicy
prywatna_tabela["NormalizujTekst"] = NormalizujTekst
prywatna_tabela["PodzielTekst"] = PodzielTekst

prywatna_tabela["DaneGracza"] = {
    ["KlasaKlucz"] = KlasaKlucz,
    ["RasaKlucz"]  = string.upper(string.gsub(RasaGracza or "", "%s+", "")),
    ["Plec"]       = UnitSex("player") == 2 and "Male" or "Female"
}