local nazwaAddonu, prywatna_tabela = ...

local ipairs = ipairs
local table = table
local QuestMapFrame = QuestMapFrame
local CampaignQuestObjectiveTracker = CampaignQuestObjectiveTracker
local hooksecurefunc = hooksecurefunc

local NormalizujTekst     = prywatna_tabela["NormalizujTekst"]       -- funkcja z zbieracz.lua
local PodzielTekst        = prywatna_tabela["PodzielTekst"]          -- funkcja z zbieracz.lua
local GenerujHash         = prywatna_tabela["GenerujHash"]           -- funkcja z narzedzia.lua
local FontTresci          = prywatna_tabela["FontTresci"]            -- zmienna globalna z konfiguracja.lua
local DostosujKolorkiFont = prywatna_tabela["DostosujKolorkiFont"]   -- funkcja z narzedzia.lua
local ZbierajCelPodrzedny = prywatna_tabela["ZbierajCelPodrzedny"]   -- funkcja z zbieracz.lua
local ZbierajTekstTooltipa = prywatna_tabela["ZbierajTekstTooltipa"]  -- funkcja z zbieracz.lua

local function PrzetlumaczTekst(Tekst)
   if not Tekst or Tekst == "" then return "" end

   local TekstPodzielony = PodzielTekst(Tekst, "\r\n\r\n") -- jezeli nie ma separatora, przetlumaczy normalnie calosc (dobre dla gossipow i wyborow)
   local Akumulator = {}
   
   local Baza = KronikiDB_Przetlumaczone_0001 or {}

   for _, OryginalnyTekst in ipairs(TekstPodzielony) do
      local TekstZnormalizowany = NormalizujTekst(OryginalnyTekst)
      local HashTekstu = GenerujHash(TekstZnormalizowany)

      local PrzetlumaczonyTekst = Baza[HashTekstu]
      
      if HashTekstu and PrzetlumaczonyTekst then
         table.insert(Akumulator, PrzetlumaczonyTekst)   -- jezeli przetlumaczone, bierz ten tekst
      else
         table.insert(Akumulator, OryginalnyTekst)       -- a jezeli nie, to zwroc oryginal nieznormalizowany
      end
   end
   return table.concat(Akumulator, "\r\n\r\n")
end

-- Funkcja pomocnicza szukająca tekstu w głąb (w dzieciach ramki)
local function SkanujRamke(Obiekt)
   if not Obiekt or not Obiekt.GetRegions then return end

   local Regiony = {Obiekt:GetRegions()}
   
   for _, Region in ipairs(Regiony) do
      if Region:GetObjectType() == "FontString" then
         local TekstOryginalny = Region:GetText()
         
         if TekstOryginalny and TekstOryginalny ~= Region["OstatniTekst"] then 
            local TekstPL = PrzetlumaczTekst(TekstOryginalny)
            
            if TekstPL then
               Region:SetText(TekstPL)
               Region:SetFont(FontTresci, 14)
               
               Region["OstatniTekst"] = TekstPL -- zapamietywany jest przetlumaczony, zeby nie mielic tego samego    
            else
               Region["OstatniTekst"] = TekstOryginalny
            end
         end
      end
   end

   local Dzieci = {Obiekt:GetChildren()}
   for _, Dziecko in ipairs(Dzieci) do
      SkanujRamke(Dziecko) -- rekurencja leci dalej
   end
end

local function TlumaczDymki()
   local CzyInstancja, RodzajInstancji = IsInInstance()
   
   if CzyInstancja and (RodzajInstancji == "party" or RodzajInstancji == "raid" or RodzajInstancji == "pvp" or RodzajInstancji == "arena") then
      return
   end

   local WszystkieDymki = C_ChatBubbles.GetAllChatBubbles()

   for _, Dymek in ipairs(WszystkieDymki) do
      if not Dymek:IsForbidden() then
         SkanujRamke(Dymek)
      end
   end
end

local function TlumaczDymkiCzat(self, event, TekstEN, Autor, ...)
   local TekstPL = PrzetlumaczTekst(TekstEN)
   if TekstPL and TekstPL ~= TekstEN then
      return false, TekstPL, Autor, ... -- false oznacza 'nie blokuj wiadomosci'; zwroc tekst po PL
   else
      return false, TekstEN, Autor, ... -- jezeli ten po PL jest pusty/nil, zwroc oryginal
   end
end

local function TlumaczOpisKampanii()
   if QuestMapFrame and QuestMapFrame.QuestsFrame and QuestMapFrame.QuestsFrame.CampaignOverview then
      local GlownaRamka = QuestMapFrame.QuestsFrame.CampaignOverview
      
      if GlownaRamka.ScrollFrame and GlownaRamka.ScrollFrame.ScrollChild then
         local DzieckoPrzewijania = GlownaRamka.ScrollFrame.ScrollChild
         local WszystkieRegiony = {DzieckoPrzewijania:GetRegions()}
         
         for _, PojedynczyRegion in ipairs(WszystkieRegiony) do
            if PojedynczyRegion:GetObjectType() == "FontString" then
               local TekstEN = PojedynczyRegion:GetText()
               if TekstEN and TekstEN ~= "" then
                  local TekstPL = PrzetlumaczTekst(TekstEN)
                  if TekstPL and TekstPL ~= TekstEN then
                     DostosujKolorkiFont(PojedynczyRegion, TekstPL, FontTresci, 14, nil, nil, nil, true)
                  end
               end
            end
         end
      end
   end
end

local function ZastosujHaczykNaTekst(ObiektTekstowy, MisjaID)
   if ObiektTekstowy.ZahaczonePL then return end
   ObiektTekstowy.ZahaczonePL = true
   
   hooksecurefunc(ObiektTekstowy, "SetText", function(self, OdczytanyTekst)
      if self.TlumaczenieWTle or not OdczytanyTekst or OdczytanyTekst == "" then return end
      
      self.TlumaczenieWTle = true
      
      local Licznik, TrescWlasciwa = OdczytanyTekst:match("^(%d+/%d+)%s+(.+)$")
      
      if not Licznik then
         local Przetlumaczony = PrzetlumaczTekst(OdczytanyTekst)
         if Przetlumaczony and Przetlumaczony ~= OdczytanyTekst then
            self:SetText(Przetlumaczony)
         else
            ZbierajCelPodrzedny(MisjaID, OdczytanyTekst)
         end
      else
         local PrzetlumaczonyCelu = PrzetlumaczTekst(TrescWlasciwa)
         if PrzetlumaczonyCelu and PrzetlumaczonyCelu ~= TrescWlasciwa then
            self:SetText(Licznik .. " " .. PrzetlumaczonyCelu)
         else
            ZbierajCelPodrzedny(MisjaID, TrescWlasciwa)
         end
      end
      
      self.TlumaczenieWTle = false
   end)
   
   local ObecnyTekst = ObiektTekstowy:GetText()
   if ObecnyTekst then
      ObiektTekstowy:SetText(ObecnyTekst)
   end
end

local function TlumaczCelePoPrawejStronie()
   local GlownaRamka = CampaignQuestObjectiveTracker.ContentsFrame
   if not GlownaRamka then return end
   
   local WszystkieBlokiMisji = {GlownaRamka:GetChildren()}
   for _, PojedynczyBlokMisji in ipairs(WszystkieBlokiMisji) do
      local MisjaID = PojedynczyBlokMisji.id or PojedynczyBlokMisji.questID
      
      if PojedynczyBlokMisji.HeaderText then
         ZastosujHaczykNaTekst(PojedynczyBlokMisji.HeaderText, MisjaID)
      end
      
      local ElementyCelu = {PojedynczyBlokMisji:GetChildren()}
      for _, Dziecko in ipairs(ElementyCelu) do
         if Dziecko.Text then
            ZastosujHaczykNaTekst(Dziecko.Text, MisjaID)
         end
         
         local Regiony = {Dziecko:GetRegions()}
         for _, Region in ipairs(Regiony) do
            if Region:GetObjectType() == "FontString" then
               ZastosujHaczykNaTekst(Region, MisjaID)
            end
         end
      end
   end
end

local function PrzetlumaczLinieDymku(PojedynczaLinia)
   local OdczytanyTekst = PojedynczaLinia:GetText()

   if not OdczytanyTekst or OdczytanyTekst == "" then return end

   local Poczatek, TrescWlasciwa = OdczytanyTekst:match("^(.-%d+/%d+)%s+(.+)$")
   if Poczatek then
      local Przetlumaczony = PrzetlumaczTekst(TrescWlasciwa)
      if Przetlumaczony and Przetlumaczony ~= TrescWlasciwa then
         PojedynczaLinia:SetText(Poczatek .. " " .. Przetlumaczony)
      else
         ZbierajTekstTooltipa(TrescWlasciwa)
      end
      return
   end

   local Ikona, TekstZIkona = OdczytanyTekst:match("^(|A.-|a)%s*(.+)$")
   if not Ikona then
      Ikona, TekstZIkona = OdczytanyTekst:match("^(|T.-|t)%s*(.+)$")
   end
   
   if Ikona then
      local Przetlumaczony = PrzetlumaczTekst(TekstZIkona)
      if Przetlumaczony and Przetlumaczony ~= TekstZIkona then
         PojedynczaLinia:SetText(Ikona .. " " .. Przetlumaczony)
      else
         ZbierajTekstTooltipa(TekstZIkona)
      end
      return
   end

   local KolorStart, Srodek, KolorKoniec = OdczytanyTekst:match("^(|c%x%x%x%x%x%x%x%x)(.-)(|r%s*)$")
   if KolorStart then
      local Przetlumaczony = PrzetlumaczTekst(Srodek)
      if Przetlumaczony and Przetlumaczony ~= Srodek then
         PojedynczaLinia:SetText(KolorStart .. Przetlumaczony .. KolorKoniec)
      else
         ZbierajTekstTooltipa(Srodek)
      end
      return
   end

   local PrzetlumaczonyTekst = PrzetlumaczTekst(OdczytanyTekst)
   if PrzetlumaczonyTekst and PrzetlumaczonyTekst ~= OdczytanyTekst then
      PojedynczaLinia:SetText(PrzetlumaczonyTekst)
   else
      if string.len(OdczytanyTekst) > 3 then
         ZbierajTekstTooltipa(OdczytanyTekst)
      end
   end
end

local function TlumaczGlobalnyDymek(self)
   local Wlasciciel = self:GetOwner()
   local CzyToMisja = false
   
   if Wlasciciel then
      local ObecnyRodzic = Wlasciciel
      for i = 1, 5 do
         if not ObecnyRodzic then break end
         
         local NazwaRodzica = ObecnyRodzic:GetName()
         if NazwaRodzica and (string.find(NazwaRodzica, "ObjectiveTracker") or string.find(NazwaRodzica, "QuestMap") or string.find(NazwaRodzica, "QuestScroll")) then
            CzyToMisja = true
            break
         end
         
         ObecnyRodzic = ObecnyRodzic:GetParent()
      end
   end

   if not CzyToMisja then return end

   local NazwaDymku = self:GetName()
   if not NazwaDymku then return end

   local LiczbaLinii = self:NumLines()

   for i = 1, LiczbaLinii do
      local PojedynczaLinia = _G[NazwaDymku .. "TextLeft" .. i]
      if PojedynczaLinia then
         PrzetlumaczLinieDymku(PojedynczaLinia)
      end
   end
end

-- przypisanie do tabeli addonu na koncu
prywatna_tabela["PrzetlumaczTekst"] = PrzetlumaczTekst
prywatna_tabela["SkanujRamke"] = SkanujRamke
prywatna_tabela["TlumaczDymki"] = TlumaczDymki
prywatna_tabela["TlumaczDymkiCzat"] = TlumaczDymkiCzat
prywatna_tabela["TlumaczOpisKampanii"] = TlumaczOpisKampanii
prywatna_tabela["TlumaczCelePoPrawejStronie"] = TlumaczCelePoPrawejStronie
prywatna_tabela["TlumaczGlobalnyDymek"] = TlumaczGlobalnyDymek