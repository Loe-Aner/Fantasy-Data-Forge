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
local QuestScrollFrame = QuestScrollFrame
local C_GossipInfo = C_GossipInfo
local C_Timer = C_Timer
-- Biblioteki Lua i funkcje podstawowe
local string = string
local table = table
local strsplit = strsplit
local ipairs = ipairs
local print = print
local _G = _G

local function BezpiecznyTrim(Tekst)
   local Sukces, Wynik = pcall(string.match, Tekst, "^%s*(.-)%s*$")
   if not Sukces then
      return nil
   end
   return Wynik
end

local function BezpiecznyLower(Tekst)
   local Sukces, Wynik = pcall(string.lower, Tekst)
   if not Sukces then
      return nil
   end
   return Wynik
end

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

    Tekst = BezpiecznyTrim(Tekst)
    if not Tekst or Tekst == "" then
       return ""
    end

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
   local Sukces, Iterator = pcall(string.gmatch, TekstOryginalny, Wzorzec)
   if not Sukces or not Iterator then
      return {}
   end
   
   for str in Iterator do
      table.insert(TekstPodzielony, str)
   end
   return TekstPodzielony
end

local function CzySensownyTekst(Tekst)
   if not Tekst then
      return false
   end

   Tekst = BezpiecznyTrim(Tekst)
   if not Tekst or Tekst == "" then
      return false
   end

   if Tekst == "-" then
      return false
   end

   return true
end

local function CzyTekstKampanii(Tekst)
   if not CzySensownyTekst(Tekst) then
      return false
   end

   local MalaLitera = BezpiecznyLower(Tekst)
   if not MalaLitera then
      return false
   end

   return string.find(MalaLitera, "campaign", 1, true)
      or string.find(MalaLitera, "chapter", 1, true)
      or string.find(MalaLitera, "storyline", 1, true)
      or string.find(MalaLitera, "kampania", 1, true)
      or string.find(MalaLitera, "rozdzial", 1, true)
      or string.find(MalaLitera, "postep", 1, true)
end

local function ZbierzFontStringiRekurencyjnie(Obiekt, Akumulator, Odwiedzone)
   if not Obiekt then
      return
   end

   if Odwiedzone[Obiekt] then
      return
   end
   Odwiedzone[Obiekt] = true

   if Obiekt.GetRegions then
      local Regiony = { Obiekt:GetRegions() }
      for _, Region in ipairs(Regiony) do
         if Region:GetObjectType() == "FontString" and Region.GetText then
            local Tekst = Region:GetText()
            if CzySensownyTekst(Tekst) then
               table.insert(Akumulator, Tekst)
            end
         end
      end
   end

   if Obiekt.GetChildren then
      local Dzieci = { Obiekt:GetChildren() }
      for _, Dziecko in ipairs(Dzieci) do
         ZbierzFontStringiRekurencyjnie(Dziecko, Akumulator, Odwiedzone)
      end
   end
end

local function CzyWlascicielQuestLogLubMapa(Obiekt)
   local Obecny = Obiekt
   for _ = 1, 8 do
      if not Obecny then
         break
      end

      local Nazwa = Obecny.GetName and Obecny:GetName()
      if Nazwa and (
         string.find(Nazwa, "QuestScrollFrame", 1, true)
         or string.find(Nazwa, "QuestMapFrame", 1, true)
         or string.find(Nazwa, "WorldMapFrame", 1, true)
      ) then
         return true
      end

      Obecny = Obecny.GetParent and Obecny:GetParent() or nil
   end

   return false
end

local function CzyKontekstQuestLogLubMapaWidoczny()
   if QuestScrollFrame and QuestScrollFrame.IsVisible and QuestScrollFrame:IsVisible() then
      return true
   end

   if QuestMapFrame and QuestMapFrame.IsVisible and QuestMapFrame:IsVisible() then
      return true
   end

   return false
end

local function DodajLinieJesliNowa(Akumulator, Unikalne, Tekst)
   if not CzySensownyTekst(Tekst) then
      return
   end

   if Unikalne[Tekst] then
      return
   end

   Unikalne[Tekst] = true
   table.insert(Akumulator, Tekst)
end

local function PobierzLinieTooltipa(TooltipFrame)
   local WszystkieLinie = {}
   local UnikalneLinie = {}

   if TooltipFrame.NumLines and TooltipFrame.GetName then
      local NazwaTooltipa = TooltipFrame:GetName()
      if NazwaTooltipa then
         for i = 1, TooltipFrame:NumLines() do
            local Linia = _G[NazwaTooltipa .. "TextLeft" .. i]
            if Linia and Linia.GetText then
               DodajLinieJesliNowa(WszystkieLinie, UnikalneLinie, Linia:GetText())
            end
         end
      end
   end

   local TekstyRekurencyjne = {}
   ZbierzFontStringiRekurencyjnie(TooltipFrame, TekstyRekurencyjne, {})
   for _, Tekst in ipairs(TekstyRekurencyjne) do
      DodajLinieJesliNowa(WszystkieLinie, UnikalneLinie, Tekst)
   end

   return WszystkieLinie
end

local function CzyToTooltipKampanii(WszystkieLinie)
   for _, Tekst in ipairs(WszystkieLinie) do
      if CzyTekstKampanii(Tekst) then
         return true
      end
   end

   return false
end

local function CzyMaLicznikPostepu(WszystkieLinie)
   for _, Tekst in ipairs(WszystkieLinie) do
      if string.find(Tekst, "%d+/%d+") then
         return true
      end
   end

   return false
end

local ZapiszTekstKampaniiZRozbiciem

local function ZapiszLinieTooltipaKampanii(WszystkieLinie)
   for _, Tekst in ipairs(WszystkieLinie) do
      ZapiszTekstKampaniiZRozbiciem("TooltipPostepKampanii", Tekst)
   end
end

local function PrzetworzTooltipKampaniiQuestLogu(TooltipFrame)
   if not TooltipFrame then
      return
   end

   local Wlasciciel = TooltipFrame.GetOwner and TooltipFrame:GetOwner() or nil
   local CzyPoprawnyWlasciciel = Wlasciciel and CzyWlascicielQuestLogLubMapa(Wlasciciel)
   local WszystkieLinie = PobierzLinieTooltipa(TooltipFrame)

   if #WszystkieLinie == 0 then
      return
   end

   local CzyKampania = CzyToTooltipKampanii(WszystkieLinie)
   if not CzyKampania then
      if not CzyPoprawnyWlasciciel or not CzyKontekstQuestLogLubMapaWidoczny() or not CzyMaLicznikPostepu(WszystkieLinie) then
         return
      end
   end

   ZapiszLinieTooltipaKampanii(WszystkieLinie)
end

local function ZbierzWidoczneRamkiTooltip(Obiekt, Akumulator, Odwiedzone, Glebokosc)
   if not Obiekt or Odwiedzone[Obiekt] or Glebokosc > 8 then
      return
   end

   Odwiedzone[Obiekt] = true

   local CzyWidoczna = Obiekt.IsVisible and Obiekt:IsVisible()
   if CzyWidoczna then
      local Nazwa = Obiekt.GetName and Obiekt:GetName() or nil
      local CzyNazwaTooltip = Nazwa and string.find(Nazwa, "Tooltip", 1, true)
      local CzyTooltipWoW = Obiekt.NumLines and Obiekt.GetOwner
      local StrataRamki = Obiekt.GetFrameStrata and Obiekt:GetFrameStrata() or nil
      local CzyWarstwaTooltip = (StrataRamki == "TOOLTIP")

      if CzyNazwaTooltip or CzyTooltipWoW or CzyWarstwaTooltip then
         table.insert(Akumulator, Obiekt)
      end
   end

   if Obiekt.GetChildren then
      local Dzieci = { Obiekt:GetChildren() }
      for _, Dziecko in ipairs(Dzieci) do
         ZbierzWidoczneRamkiTooltip(Dziecko, Akumulator, Odwiedzone, Glebokosc + 1)
      end
   end
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
               -- print("|cff00ccff[Kroniki]|r Dodano nowy nieprzetłumaczony rekord dla misji: " .. HashTekstu)
            end
         end

         if RodzajTekstu == "GOSSIP" then
            if not BazaBrakujacychGossipow[HashTekstu] then
               BazaBrakujacychGossipow[HashTekstu] = {
                  ["NPC"] = NazwaNPC .. " (" .. npcID .. ")",
                  ["TEKST_ENG"] = TekstZnormalizowany, 
                  ["TEKST_RAW"] = PojedynczaLinia
               }
               -- print("|cff00ccff[Kroniki]|r Dodano nowy nieprzetłumaczony rekord dla gossipa: " .. HashTekstu)
            end
         end

         if RodzajTekstu == "DYMEK" then
            if not BazaBrakujacychDymkow[HashTekstu] then
               BazaBrakujacychDymkow[HashTekstu] = {
                  ["NPC"] = NazwaNPC,
                  ["TEKST_ENG"] = TekstZnormalizowany, 
                  ["TEKST_RAW"] = PojedynczaLinia
               }
               -- print("|cff00ccff[Kroniki]|r Dodano nowy nieprzetłumaczony rekord dla dymku: " .. HashTekstu)
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

ZapiszTekstKampaniiZRozbiciem = function(TypTekstu, TekstOryginalny)
   if not CzySensownyTekst(TekstOryginalny) then
      return
   end

   ZapiszPojedynczyTekst("MISJA", TypTekstu, TekstOryginalny, nil)

   local _, Koncowka = TekstOryginalny:match("^(.-%d+/%d+)%s+(.+)$")
   if Koncowka and Koncowka ~= "" then
      ZapiszPojedynczyTekst("MISJA", TypTekstu .. "_CZESC", Koncowka, nil)
      return
   end

   local _, KoncowkaProsta = TekstOryginalny:match("^(%d+/%d+)%s+(.+)$")
   if KoncowkaProsta and KoncowkaProsta ~= "" then
      ZapiszPojedynczyTekst("MISJA", TypTekstu .. "_CZESC", KoncowkaProsta, nil)
   end
end

local function CzyKandydatBlokuPostepuKampanii(Blok)
   if not Blok then
      return false
   end

   if Blok.Progress then
      return true
   end

   if Blok.GetName then
      local NazwaBloku = Blok:GetName()
      if NazwaBloku and (
         string.find(NazwaBloku, "Campaign", 1, true)
         or string.find(NazwaBloku, "Progress", 1, true)
      ) then
         return true
      end
   end

   return false
end

prywatna_tabela["ZbierajPostepKampaniiQuestLogu"] = function()
   local ZawartoscQuestLogu = QuestScrollFrame and QuestScrollFrame.Contents
   if not ZawartoscQuestLogu then
      return
   end

   local WszystkieBloki = { ZawartoscQuestLogu:GetChildren() }
   for _, Blok in ipairs(WszystkieBloki) do
      if CzyKandydatBlokuPostepuKampanii(Blok) then
         local Teksty = {}
         ZbierzFontStringiRekurencyjnie(Blok, Teksty, {})

         local ToJestSekcjaKampanii = false
         for _, Tekst in ipairs(Teksty) do
            if CzyTekstKampanii(Tekst) then
               ToJestSekcjaKampanii = true
               break
            end
         end

         if ToJestSekcjaKampanii then
            for _, Tekst in ipairs(Teksty) do
               ZapiszTekstKampaniiZRozbiciem("QuestLogPostepKampanii", Tekst)
            end
         end
      end
   end
end

prywatna_tabela["ZbierajTooltipKampaniiQuestLogu"] = function(TooltipFrame)
   if not TooltipFrame then
      return
   end

   PrzetworzTooltipKampaniiQuestLogu(TooltipFrame)
end

prywatna_tabela["ZbierajWidoczneTooltipyQuestLogu"] = function()
   local WorldMapFrame = _G.WorldMapFrame

   local CzyQuestScrollWidoczny = QuestScrollFrame and QuestScrollFrame.IsVisible and QuestScrollFrame:IsVisible()
   local CzyQuestMapWidoczny = QuestMapFrame and QuestMapFrame.IsVisible and QuestMapFrame:IsVisible()
   local CzyWorldMapWidoczny = WorldMapFrame and WorldMapFrame.IsVisible and WorldMapFrame:IsVisible()

   if not (CzyQuestScrollWidoczny or CzyQuestMapWidoczny or CzyWorldMapWidoczny) then
      return
   end

   local Tooltipy = {}
   local Odwiedzone = {}
   ZbierzWidoczneRamkiTooltip(QuestScrollFrame, Tooltipy, Odwiedzone, 1)
   ZbierzWidoczneRamkiTooltip(QuestMapFrame, Tooltipy, Odwiedzone, 1)
   ZbierzWidoczneRamkiTooltip(WorldMapFrame, Tooltipy, Odwiedzone, 1)

   for _, RamkaTooltipa in ipairs(Tooltipy) do
      PrzetworzTooltipKampaniiQuestLogu(RamkaTooltipa)
   end
end

local function UruchomZbieranieWidocznychTooltipow()
   local FunkcjaZbierajaca = prywatna_tabela["ZbierajWidoczneTooltipyQuestLogu"]
   if FunkcjaZbierajaca then
      FunkcjaZbierajaca()
   end
end

prywatna_tabela["ZahaczHoverPostepuKampaniiQuestLogu"] = function()
   local ZawartoscQuestLogu = QuestScrollFrame and QuestScrollFrame.Contents
   if not ZawartoscQuestLogu then
      return
   end

   local WszystkieBloki = { ZawartoscQuestLogu:GetChildren() }
   for _, Blok in ipairs(WszystkieBloki) do
      if CzyKandydatBlokuPostepuKampanii(Blok) and Blok.HookScript and not Blok.KronikiHoverKampaniiHooked then
         Blok.KronikiHoverKampaniiHooked = true
         Blok:HookScript("OnEnter", function()
            if C_Timer and C_Timer.After then
               C_Timer.After(0, UruchomZbieranieWidocznychTooltipow)
               C_Timer.After(0.08, UruchomZbieranieWidocznychTooltipow)
               C_Timer.After(0.18, UruchomZbieranieWidocznychTooltipow)
            else
               UruchomZbieranieWidocznychTooltipow()
            end
         end)
      end
   end
end

-- przerzut do globalnej tablicy
prywatna_tabela["NormalizujTekst"] = NormalizujTekst
prywatna_tabela["PodzielTekst"] = PodzielTekst
