local nazwaAddonu, prywatna_tabela = ...

-- === 0. CACHE ===
local UnitName = UnitName
local UnitRace = UnitRace
local UnitClass = UnitClass
local UnitSex = UnitSex
local UnitGUID = UnitGUID
local GetQuestID = GetQuestID
local GetTitleText = GetTitleText
local GetQuestText = GetQuestText
local GetObjectiveText = GetObjectiveText
local GetProgressText = GetProgressText
local GetRewardText = GetRewardText
local QuestMapFrame = QuestMapFrame
local C_GossipInfo = C_GossipInfo
-- Biblioteki Lua i funkcje podstawowe
local string = string
local table = table
local strsplit = strsplit
local ipairs = ipairs
local print = print

-- === 1. DANE GRACZA ===
local ImieGracza = UnitName("player")
-- UnitRace zwraca: nazwa lokalna, nazwa angielska. Biore te druga (angielska) dla bezpieczenstwa
local _, RasaGracza = UnitRace("player") 
local RasaGraczaMala = string.lower(RasaGracza or "")

-- UnitClass zwraca: nazwa lokalna, TAG (angielski). Biore TAG.
local _, KlasaTag = UnitClass("player")
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
local function ZapiszPojedynczyTekst(RodzajTekstu, TypTekstu, TekstOryginalny, DaneDodatkowe)
   if not TekstOryginalny or TekstOryginalny == "" then 
      return ""
   end

   local TekstPodzielony = PodzielTekst(TekstOryginalny, "\r\n\r\n")
   local BazaBrakujacychMisji = KronikiDB_Nieprzetlumaczone["ListaMisji"]
   local BazaBrakujacychGossipow = KronikiDB_Nieprzetlumaczone["ListaGossipow"]
   local BazaBrakujacychDymkow = KronikiDB_Nieprzetlumaczone["ListaDymkow"]

   local NazwaNPC
   local MisjaID = nil
   local npcID = "123456789" -- ta sama flaga jest w mojej DB na 'smieci'

   if RodzajTekstu == "DYMEK" then
      NazwaNPC = DaneDodatkowe
   
   elseif RodzajTekstu == "MISJA" then
      NazwaNPC = UnitName("npc") or "Nieznany"
      MisjaID = DaneDodatkowe

   else
      NazwaNPC = UnitName("npc") or "Nieznany"
   end

   local GUID = UnitGUID("npc")
   if GUID then
      local _, _, _, _, _, idZTargetu = strsplit("-", GUID) -- szosta liczba to ID npca/targetu
      if idZTargetu then 
         npcID = idZTargetu 
         end      
      end

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

         if RodzajTekstu == "DYMEK" then
            if not BazaBrakujacychDymkow[HashTekstu] then
               BazaBrakujacychDymkow[HashTekstu] = {
                  ["NPC"] = NazwaNPC,
                  ["TEKST_ENG"] = TekstZnormalizowany, 
                  ["TEKST_RAW"] = PojedynczaLinia
               }
               print("|cff00ccff[Kroniki]|r Dodano nowy nieprzetłumaczony rekord dla dymku: " .. HashTekstu)
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

prywatna_tabela["ZbierajGossipy"] = function(self, event)
   if event == "GOSSIP_SHOW" then
      local Gossip = C_GossipInfo.GetText()          -- wywolanie z 'C' jest nowsze
      local WyboryGossip = C_GossipInfo.GetOptions() -- UWAGA: zwraca tabele, a nie jak wyzej bezposredni tekst

      if not Gossip and not #WyboryGossip == 0 then
         return
      end

      if Gossip then
         ZapiszPojedynczyTekst("GOSSIP", "Dialog", Gossip, nil)
      end

      for _, PojedynczaLinia in ipairs(WyboryGossip) do
         if PojedynczaLinia["name"] then
            ZapiszPojedynczyTekst("GOSSIP", "Wybory", PojedynczaLinia["name"], nil)
         end
      end
   end
end

prywatna_tabela["ZbierajDymki"] = function(self, event, TrescDymku, NazwaNPC, ...) -- eventy beda w Baza.lua
      if not TrescDymku then
         return
      end
      ZapiszPojedynczyTekst("DYMEK", "Dialog", TrescDymku, NazwaNPC)
   end

prywatna_tabela["ZbierajCelPodrzedny"] = function(QuestID, TekstCelu)
   if QuestID and TekstCelu then
      ZapiszPojedynczyTekst("MISJA", "CelPodrzedny", TekstCelu, QuestID)
   end
end

prywatna_tabela["ZbierajOpisMoba"] = function(TekstMoba)
   if TekstMoba then
      ZapiszPojedynczyTekst("MISJA", "OpisMoba", TekstMoba, nil)
   end
end

prywatna_tabela["ZbierajNazwyKrain"] = function(NazwaKrainy)
   if NazwaKrainy then
      ZapiszPojedynczyTekst("MISJA", "NazwaKrainy", NazwaKrainy, nil)
   end
end

prywatna_tabela["ZbierajTekstTooltipa"] = function(TekstOryginalny)
   if TekstOryginalny then
      ZapiszPojedynczyTekst("MISJA", "Tooltip", TekstOryginalny, nil)
   end
end

prywatna_tabela["ZbierajOpisKampanii"] = function()
   if QuestMapFrame and QuestMapFrame.QuestsFrame and QuestMapFrame.QuestsFrame.CampaignOverview then
      local GlownaRamka = QuestMapFrame.QuestsFrame.CampaignOverview
      
      if GlownaRamka.ScrollFrame and GlownaRamka.ScrollFrame.ScrollChild then
         local DzieckoPrzewijania = GlownaRamka.ScrollFrame.ScrollChild
         local WszystkieRegiony = {DzieckoPrzewijania:GetRegions()}
         
         for _, PojedynczyRegion in ipairs(WszystkieRegiony) do
            if PojedynczyRegion:GetObjectType() == "FontString" then
               local TekstEN = PojedynczyRegion:GetText()
               ZapiszPojedynczyTekst("MISJA", "OpisKampanii", TekstEN, nil)
            end
         end
      end
   end
end

-- przerzut do globalnej tablicy
prywatna_tabela["NormalizujTekst"] = NormalizujTekst
prywatna_tabela["PodzielTekst"] = PodzielTekst