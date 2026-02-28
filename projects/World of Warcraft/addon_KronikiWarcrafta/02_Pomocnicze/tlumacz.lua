local nazwaAddonu, prywatna_tabela = ...

local ipairs = ipairs
local table = table
local string = string

local C_Timer = C_Timer
local C_ChatBubbles = C_ChatBubbles
local C_GossipInfo = C_GossipInfo
local C_QuestLog = C_QuestLog
local CampaignQuestObjectiveTracker = CampaignQuestObjectiveTracker
local QuestObjectiveTracker = QuestObjectiveTracker
local GossipFrame = GossipFrame
local GossipGreetingText = GossipGreetingText
local IsInInstance = IsInInstance
local MapQuestInfoRewardsFrame = MapQuestInfoRewardsFrame
local QuestInfoDescriptionHeader = QuestInfoDescriptionHeader
local QuestInfoDescriptionText = QuestInfoDescriptionText
local QuestInfoObjectivesHeader = QuestInfoObjectivesHeader
local QuestInfoObjectivesText = QuestInfoObjectivesText
local QuestInfoRewardText = QuestInfoRewardText
local QuestInfoRewardsFrame = QuestInfoRewardsFrame
local QuestInfoTitleHeader = QuestInfoTitleHeader
local QuestInfoXPFrame = QuestInfoXPFrame
local QuestMapFrame = QuestMapFrame
local QuestNPCModelText = QuestNPCModelText
local QuestProgressText = QuestProgressText
local QuestProgressTitleText = QuestProgressTitleText
local QuestScrollFrame = QuestScrollFrame
local hooksecurefunc = hooksecurefunc

local GetObjectiveText = GetObjectiveText
local GetProgressText = GetProgressText
local GetQuestID = GetQuestID
local GetQuestText = GetQuestText
local GetRewardText = GetRewardText
local GetTitleText = GetTitleText
local QuestMapFrame_GetDetailQuestID = QuestMapFrame_GetDetailQuestID

local NormalizujTekst = prywatna_tabela["NormalizujTekst"]
local PodzielTekst = prywatna_tabela["PodzielTekst"]
local GenerujHash = prywatna_tabela["GenerujHash"]
local FontTytulu = prywatna_tabela["FontTytulu"]
local FontTresci = prywatna_tabela["FontTresci"]
local DostosujKolorkiFont = prywatna_tabela["DostosujKolorkiFont"]
local ZbierajCelPodrzedny = prywatna_tabela["ZbierajCelPodrzedny"]
local ZbierajNazwyKrain = prywatna_tabela["ZbierajNazwyKrain"]
local ZbierajOpisMoba = prywatna_tabela["ZbierajOpisMoba"]
local ZbierajTekstTooltipa = prywatna_tabela["ZbierajTekstTooltipa"]

local function PrzetlumaczTekst(Tekst)
   if not Tekst or Tekst == "" then
      return ""
   end

   local TekstPodzielony = PodzielTekst(Tekst, "\r\n\r\n")
   local Akumulator = {}
   local Baza = KronikiDB_Przetlumaczone_0001 or {}

   for _, OryginalnyTekst in ipairs(TekstPodzielony) do
      local TekstZnormalizowany = NormalizujTekst(OryginalnyTekst)
      local HashTekstu = GenerujHash(TekstZnormalizowany)
      local PrzetlumaczonyTekst = Baza[HashTekstu]

      if HashTekstu and PrzetlumaczonyTekst then
         table.insert(Akumulator, PrzetlumaczonyTekst)
      else
         table.insert(Akumulator, OryginalnyTekst)
      end
   end

   return table.concat(Akumulator, "\r\n\r\n")
end

local function PrzetlumaczJesliInny(Tekst)
   if not Tekst or Tekst == "" then
      return nil
   end

   local Przetlumaczony = PrzetlumaczTekst(Tekst)
   if Przetlumaczony and Przetlumaczony ~= "" and Przetlumaczony ~= Tekst then
      return Przetlumaczony
   end

   return nil
end

local function RozdzielLicznikIOpis(PelnyTekst)
   if not PelnyTekst or PelnyTekst == "" then
      return "", ""
   end

   local Licznik, TrescWlasciwa = PelnyTekst:match("^(%d+/%d+)%s+(.+)$")
   if Licznik then
      return Licznik .. " ", TrescWlasciwa
   end

   return "", PelnyTekst
end

local function CzyTekstKampanii(Tekst)
   if not Tekst or Tekst == "" then
      return false
   end

   local MalaLitera = string.lower(Tekst)
   return string.find(MalaLitera, "campaign", 1, true)
      or string.find(MalaLitera, "chapter", 1, true)
      or string.find(MalaLitera, "storyline", 1, true)
      or string.find(MalaLitera, "kampania", 1, true)
      or string.find(MalaLitera, "rozdzial", 1, true)
      or string.find(MalaLitera, "postep", 1, true)
end

local function PrzetlumaczLinieZLicznikiem(Tekst)
   if not Tekst or Tekst == "" then
      return nil
   end

   local PelneTlumaczenie = PrzetlumaczJesliInny(Tekst)
   if PelneTlumaczenie then
      return PelneTlumaczenie
   end

   local Poczatek, Koncowka = Tekst:match("^(.-%d+/%d+)%s+(.+)$")
   if Poczatek then
      local KoncowkaPL = PrzetlumaczJesliInny(Koncowka)
      if KoncowkaPL then
         return Poczatek .. " " .. KoncowkaPL
      end
   end

   local Licznik, Tresc = RozdzielLicznikIOpis(Tekst)
   if Licznik ~= "" then
      local TrescPL = PrzetlumaczJesliInny(Tresc)
      if TrescPL then
         return Licznik .. TrescPL
      end
   end

   return nil
end

local function PodswietlLicznikNaZloto(Tekst)
   if not Tekst or Tekst == "" then
      return Tekst
   end

   -- Jesli tekst ma juz własne kody kolorow, nie nadpisuj ich.
   if string.find(Tekst, "|c", 1, true) then
      return Tekst
   end

   -- Preferowane: podswietl caly fragment postepu, np. "0/3 Rozdzialow".
   local Prefix, Postep = Tekst:match("^(.-)(%d+/%d+%s+.+)$")
   if Postep then
      return Prefix .. "|cffffff00" .. Postep .. "|r"
   end

   -- Fallback: jezeli jest sam licznik, podswietl sam licznik.
   return Tekst:gsub("(%d+/%d+)", "|cffffff00%1|r")
end

local function CzyWymagaZlotegoPostepu(TekstEN, TekstPL)
   if (not TekstEN and not TekstPL) then
      return false
   end

   local MaLicznik = (TekstEN and string.find(TekstEN, "%d+/%d+"))
      or (TekstPL and string.find(TekstPL, "%d+/%d+"))

   if not MaLicznik then
      return false
   end

   return CzyTekstKampanii(TekstEN) or CzyTekstKampanii(TekstPL)
end

local function PrzetlumaczFontStringiRekurencyjnie(Obiekt, Odwiedzone)
   if not Obiekt or Odwiedzone[Obiekt] then
      return
   end

   Odwiedzone[Obiekt] = true

   if Obiekt.GetRegions then
      local Regiony = { Obiekt:GetRegions() }
      for _, Region in ipairs(Regiony) do
         if Region:GetObjectType() == "FontString" and Region.GetText and Region.SetText then
            local TekstEN = Region:GetText()
            local TekstPL = PrzetlumaczLinieZLicznikiem(TekstEN)
            if TekstPL then
               if CzyWymagaZlotegoPostepu(TekstEN, TekstPL) then
                  TekstPL = PodswietlLicznikNaZloto(TekstPL)
               end
               Region:SetText(TekstPL)
            end
         end
      end
   end

   if Obiekt.GetChildren then
      local Dzieci = { Obiekt:GetChildren() }
      for _, Dziecko in ipairs(Dzieci) do
         PrzetlumaczFontStringiRekurencyjnie(Dziecko, Odwiedzone)
      end
   end
end

local function CzyBlokPostepuKampanii(Blok)
   if not Blok then
      return false
   end

   if Blok.Progress then
      if not Blok.Progress.GetText then
         return true
      end

      local TekstPostepu = Blok.Progress:GetText()
      if not TekstPostepu or TekstPostepu == "" then
         return true
      end

      if CzyTekstKampanii(TekstPostepu) then
         return true
      end
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

local function PrzetlumaczPostepKampaniiQuestLogu()
   local GlownaRamka = QuestScrollFrame and QuestScrollFrame.Contents
   if not GlownaRamka then
      return
   end

   local WszystkieBloki = { GlownaRamka:GetChildren() }
   for _, Blok in ipairs(WszystkieBloki) do
      if CzyBlokPostepuKampanii(Blok) then
         PrzetlumaczFontStringiRekurencyjnie(Blok, {})
      end
   end
end

local function BezpiecznySetText(ElementUI, Tekst, Font, RozmiarFontu, KolorR, KolorG, KolorB, WymusPokazanie)
   if not ElementUI or not Tekst then
      return
   end

   DostosujKolorkiFont(ElementUI, Tekst, Font, RozmiarFontu, KolorR, KolorG, KolorB, WymusPokazanie)
end

local function PobierzIdAktywnejMisji()
   if QuestMapFrame and QuestMapFrame.IsVisible and QuestMapFrame:IsVisible() and QuestMapFrame_GetDetailQuestID then
      local MisjaIDMapy = QuestMapFrame_GetDetailQuestID()
      if MisjaIDMapy and MisjaIDMapy > 0 then
         return MisjaIDMapy
      end
   end

   local MisjaID = GetQuestID and GetQuestID()
   if MisjaID and MisjaID > 0 then
      return MisjaID
   end

   return nil
end

local function PobierzTytulAktywnejMisji()
   if QuestMapFrame and QuestMapFrame.IsVisible and QuestMapFrame:IsVisible() and QuestMapFrame_GetDetailQuestID then
      local MisjaID = QuestMapFrame_GetDetailQuestID()
      if MisjaID and MisjaID > 0 and C_QuestLog and C_QuestLog.GetTitleForQuestID then
         local TytulMapy = C_QuestLog.GetTitleForQuestID(MisjaID)
         if TytulMapy and TytulMapy ~= "" then
            return TytulMapy
         end
      end
   end

   local Tytul = GetTitleText and GetTitleText()
   if Tytul and Tytul ~= "" then
      return Tytul
   end

   if QuestInfoTitleHeader and QuestInfoTitleHeader.GetText then
      return QuestInfoTitleHeader:GetText()
   end

   return nil
end

local function PrzetlumaczCeleQuestInfo(MisjaID)
   local i = 1

   while true do
      local PodrzednyCel = _G["QuestInfoObjective" .. i]
      if not PodrzednyCel then
         break
      end

      if PodrzednyCel:IsVisible() and PodrzednyCel:GetText() then
         local TekstPelny = PodrzednyCel:GetText()
         local Licznik, TrescWlasciwa = RozdzielLicznikIOpis(TekstPelny)
         local Tlumaczenie = PrzetlumaczJesliInny(TrescWlasciwa)

         if Tlumaczenie then
            BezpiecznySetText(PodrzednyCel, Licznik .. Tlumaczenie, FontTresci, 14, 0, 0, 0, true)
         elseif MisjaID then
            ZbierajCelPodrzedny(MisjaID, TrescWlasciwa)
         end
      end

      i = i + 1
   end
end

local TLUMACZENIA_STALYCH_ETYKIET_NAGROD = {
   ["The following will be cast on you:"] = "Na ciebie zostanie rzucone:",
   ["You will be able to choose one of these rewards:"] = "Będziesz w stanie wybrać jedną z poniższych nagród:",
}

local function PrzetlumaczStaleEtykietyWRegionach(Ramka, Font, RozmiarFontu, KolorR, KolorG, KolorB)
   if not Ramka or not Ramka.GetRegions then
      return
   end

   local Regiony = { Ramka:GetRegions() }
   for _, Region in ipairs(Regiony) do
      if Region
         and Region.GetObjectType
         and Region:GetObjectType() == "FontString"
         and Region.GetText then
         local TekstEN = Region:GetText()
         local TekstPL = TLUMACZENIA_STALYCH_ETYKIET_NAGROD[TekstEN]
         if TekstPL then
            BezpiecznySetText(Region, TekstPL, Font, RozmiarFontu, KolorR, KolorG, KolorB, true)
         end
      end
   end
end

local function TlumaczStaleEtykietyMisji()
   BezpiecznySetText(QuestInfoObjectivesHeader, "Cele misji", FontTytulu, 18, 0, 0, 0, true)
   BezpiecznySetText(QuestInfoDescriptionHeader, "Opis", FontTytulu, 18, 0, 0, 0, true)

   if QuestInfoRewardsFrame then
      BezpiecznySetText(QuestInfoRewardsFrame.Header, "Nagrody", FontTytulu, 18, 0, 0, 0, true)
      BezpiecznySetText(QuestInfoRewardsFrame.ItemReceiveText, "Otrzymasz:", FontTresci, 14, 0, 0, 0, true)
      BezpiecznySetText(QuestInfoRewardsFrame.ItemChooseText, "Będziesz w stanie wybrać jedną z poniższych nagród:", FontTresci, 14, 0, 0, 0, true)
      BezpiecznySetText(QuestInfoRewardsFrame.SpellLearnText, "Na ciebie zostanie rzucone:", FontTresci, 14, 0, 0, 0, true)
      BezpiecznySetText(_G.QuestInfoSpellLearnText, "Na ciebie zostanie rzucone:", FontTresci, 14, 0, 0, 0, true)
      BezpiecznySetText(_G.QuestInfoRewardFrameSpellLearnText, "Na ciebie zostanie rzucone:", FontTresci, 14, 0, 0, 0, true)
      BezpiecznySetText(_G.QuestInfoRewardFrameItemChooseText, "Będziesz w stanie wybrać jedną z poniższych nagród:", FontTresci, 14, 0, 0, 0, true)
      PrzetlumaczStaleEtykietyWRegionach(QuestInfoRewardsFrame, FontTresci, 14, 0, 0, 0)
   end

   PrzetlumaczStaleEtykietyWRegionach(_G.QuestInfoRewardFrame, FontTresci, 14, 0, 0, 0)

   if QuestInfoXPFrame then
      BezpiecznySetText(QuestInfoXPFrame.ReceiveText, "Doswiadczenie:", FontTresci, 14, 0, 0, 0, true)
   end

   local RewardsFrameMapy = QuestMapFrame
      and QuestMapFrame.QuestsFrame
      and QuestMapFrame.QuestsFrame.DetailsFrame
      and QuestMapFrame.QuestsFrame.DetailsFrame.RewardsFrameContainer
      and QuestMapFrame.QuestsFrame.DetailsFrame.RewardsFrameContainer.RewardsFrame

   if RewardsFrameMapy and RewardsFrameMapy.Label then
      BezpiecznySetText(RewardsFrameMapy.Label, "Nagrody", FontTytulu, 18, 0.85, 0.77, 0.60, true)
   end

   local TlumaczeniaReceive = {
      ["You will receive:"] = "Otrzymasz:",
      ["You will also receive:"] = "Otrzymasz rowniez:",
   }

   local TlumaczeniaChoose = {
      ["You will receive:"] = "Otrzymasz przedmiot:",
      ["You will receive one of:"] = "Otrzymasz jeden z ponizszych:",
      ["You will be able to choose one of these rewards:"] = "Będziesz w stanie wybrać jedną z poniższych nagród:",
   }

   if MapQuestInfoRewardsFrame and MapQuestInfoRewardsFrame.ItemReceiveText then
      local TekstEN = MapQuestInfoRewardsFrame.ItemReceiveText:GetText()
      local TekstPL = TlumaczeniaReceive[TekstEN]
      if TekstPL then
         BezpiecznySetText(MapQuestInfoRewardsFrame.ItemReceiveText, TekstPL, FontTresci, 11, 0.85, 0.77, 0.60, true)
      end
   end

   if MapQuestInfoRewardsFrame and MapQuestInfoRewardsFrame.ItemChooseText then
      local TekstEN = MapQuestInfoRewardsFrame.ItemChooseText:GetText()
      local TekstPL = TlumaczeniaChoose[TekstEN]
      if TekstPL then
         BezpiecznySetText(MapQuestInfoRewardsFrame.ItemChooseText, TekstPL, FontTresci, 11, 0.85, 0.77, 0.60, true)
      end
   end

   PrzetlumaczStaleEtykietyWRegionach(MapQuestInfoRewardsFrame, FontTresci, 11, 0.85, 0.77, 0.60)
end

local function TlumaczOpisIMainTeksty()
   local OpisOryginal = GetQuestText and GetQuestText()
   if (not OpisOryginal or OpisOryginal == "") and QuestInfoDescriptionText and QuestInfoDescriptionText.GetText then
      OpisOryginal = QuestInfoDescriptionText:GetText()
   end

   if OpisOryginal then
      local OpisPL = PrzetlumaczTekst(OpisOryginal)
      if OpisPL then
         BezpiecznySetText(QuestInfoDescriptionText, OpisPL, FontTresci, 14, 0, 0, 0, true)
      end
   end

   local CelOryginal = GetObjectiveText and GetObjectiveText()
   if (not CelOryginal or CelOryginal == "")
      and QuestInfoObjectivesText
      and QuestInfoObjectivesText.IsVisible
      and QuestInfoObjectivesText:IsVisible() then
      CelOryginal = QuestInfoObjectivesText:GetText()
   end

   if CelOryginal and QuestInfoObjectivesText and QuestInfoObjectivesText.IsVisible and QuestInfoObjectivesText:IsVisible() then
      local CelPL = PrzetlumaczTekst(CelOryginal)
      if CelPL then
         BezpiecznySetText(QuestInfoObjectivesText, CelPL, FontTresci, 14, 0, 0, 0, true)
      end
   end

   if QuestNPCModelText and QuestNPCModelText.GetText then
      local OpisMoba = QuestNPCModelText:GetText()
      if OpisMoba and OpisMoba ~= "" then
         ZbierajOpisMoba(OpisMoba)
         local OpisMobaPL = PrzetlumaczJesliInny(OpisMoba)
         if OpisMobaPL then
            QuestNPCModelText:SetText(OpisMobaPL)
         end
      end
   end

   local PostepOryginal = GetProgressText and GetProgressText()
   if PostepOryginal and QuestProgressText and QuestProgressText.IsVisible and QuestProgressText:IsVisible() then
      local PostepPL = PrzetlumaczTekst(PostepOryginal)
      if PostepPL then
         BezpiecznySetText(QuestProgressText, PostepPL, FontTresci, 14, 0, 0, 0, true)
      end
   end

   if QuestProgressTitleText and QuestProgressTitleText.IsVisible and QuestProgressTitleText:IsVisible() then
      local Tytul = GetTitleText and GetTitleText()
      local TytulPL = PrzetlumaczTekst(Tytul)
      if TytulPL then
         BezpiecznySetText(QuestProgressTitleText, TytulPL, FontTytulu, 18, 0, 0, 0, true)
      end
   end

   local ZakonczenieOryginal = GetRewardText and GetRewardText()
   if ZakonczenieOryginal and QuestInfoRewardText and QuestInfoRewardText.IsVisible and QuestInfoRewardText:IsVisible() then
      local ZakonczeniePL = PrzetlumaczTekst(ZakonczenieOryginal)
      if ZakonczeniePL then
         BezpiecznySetText(QuestInfoRewardText, ZakonczeniePL, FontTresci, 14, 0, 0, 0, true)
      end
   end
end

local function TlumaczGossip()
   local GossipDialog = C_GossipInfo and C_GossipInfo.GetText and C_GossipInfo.GetText()
   if not GossipDialog or not GossipFrame or not GossipFrame.IsVisible or not GossipFrame:IsVisible() then
      return
   end

   local GossipDialogPL = PrzetlumaczTekst(GossipDialog)

   if GossipGreetingText and GossipGreetingText.IsShown and GossipGreetingText:IsShown() then
      BezpiecznySetText(GossipGreetingText, GossipDialogPL, FontTresci, 14, 0, 0, 0, true)
   end

   local ScrollBox = GossipFrame.GreetingPanel and GossipFrame.GreetingPanel.ScrollBox
   if not ScrollBox or not ScrollBox.GetFrames then
      return
   end

   local RamkiWLiscie = ScrollBox:GetFrames()
   if not RamkiWLiscie then
      return
   end

   for _, PojedynczaRamka in ipairs(RamkiWLiscie) do
      if PojedynczaRamka.GreetingText and PojedynczaRamka.GreetingText.GetText then
         local ObecnyTekst = PojedynczaRamka.GreetingText:GetText()
         if ObecnyTekst and ObecnyTekst ~= "" then
            local TekstPL = PrzetlumaczTekst(ObecnyTekst)
            if TekstPL then
               BezpiecznySetText(PojedynczaRamka.GreetingText, TekstPL, FontTresci, 14, 0, 0, 0, true)
            end
         end
      end

      if PojedynczaRamka.GetText and PojedynczaRamka.SetText then
         local TekstPrzycisku = PojedynczaRamka:GetText()
         if TekstPrzycisku and TekstPrzycisku ~= "" then
            local TekstPL = PrzetlumaczTekst(TekstPrzycisku)
            if TekstPL then
               PojedynczaRamka:SetText(TekstPL)

               if PojedynczaRamka.GetFontString then
                  local FontString = PojedynczaRamka:GetFontString()
                  if FontString then
                     FontString:SetFont(FontTresci, 14)
                     FontString:SetTextColor(0, 0, 0)
                  end
               end
            end
         end
      end
   end
end

local function PodmienTekstOknienko()
   local TytulDoTlumaczenia = PobierzTytulAktywnejMisji()
   if TytulDoTlumaczenia then
      local TytulPL = PrzetlumaczTekst(TytulDoTlumaczenia)
      if TytulPL then
         BezpiecznySetText(QuestInfoTitleHeader, TytulPL, FontTytulu, 18, nil, nil, nil, true)
      end
   end

   local MisjaID = PobierzIdAktywnejMisji()
   PrzetlumaczCeleQuestInfo(MisjaID)
   TlumaczStaleEtykietyMisji()
   TlumaczOpisIMainTeksty()
   TlumaczGossip()
end

local function SkanujRamke(Obiekt)
   if not Obiekt or not Obiekt.GetRegions then
      return
   end

   local Regiony = { Obiekt:GetRegions() }
   for _, Region in ipairs(Regiony) do
      if Region:GetObjectType() == "FontString" then
         local TekstOryginalny = Region:GetText()

         if TekstOryginalny and TekstOryginalny ~= Region["OstatniTekst"] then
            local TekstPL = PrzetlumaczJesliInny(TekstOryginalny)
            if TekstPL then
               Region:SetText(TekstPL)
               Region:SetFont(FontTresci, 14)
               Region["OstatniTekst"] = TekstPL
            else
               Region["OstatniTekst"] = TekstOryginalny
            end
         end
      end
   end

   local Dzieci = { Obiekt:GetChildren() }
   for _, Dziecko in ipairs(Dzieci) do
      SkanujRamke(Dziecko)
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
   local TekstPL = PrzetlumaczJesliInny(TekstEN)
   if TekstPL then
      return false, TekstPL, Autor, ...
   end

   return false, TekstEN, Autor, ...
end

local function TlumaczOpisKampanii()
   local CampaignOverview = QuestMapFrame and QuestMapFrame.QuestsFrame and QuestMapFrame.QuestsFrame.CampaignOverview
   if not CampaignOverview or not CampaignOverview.ScrollFrame or not CampaignOverview.ScrollFrame.ScrollChild then
      return
   end

   local DzieckoPrzewijania = CampaignOverview.ScrollFrame.ScrollChild
   local WszystkieRegiony = { DzieckoPrzewijania:GetRegions() }

   for _, PojedynczyRegion in ipairs(WszystkieRegiony) do
      if PojedynczyRegion:GetObjectType() == "FontString" then
         local TekstEN = PojedynczyRegion:GetText()
         local TekstPL = PrzetlumaczJesliInny(TekstEN)
         if TekstPL then
            BezpiecznySetText(PojedynczyRegion, TekstPL, FontTresci, 14, nil, nil, nil, true)
         end
      end
   end
end

local function ZastosujHaczykNaTekst(ObiektTekstowy, MisjaID)
   if not ObiektTekstowy or ObiektTekstowy.ZahaczonePL then
      return
   end

   ObiektTekstowy.ZahaczonePL = true

   hooksecurefunc(ObiektTekstowy, "SetText", function(self, OdczytanyTekst)
      if self.TlumaczenieWTle or not OdczytanyTekst or OdczytanyTekst == "" then
         return
      end

      self.TlumaczenieWTle = true

      local Licznik, TrescWlasciwa = RozdzielLicznikIOpis(OdczytanyTekst)
      local Przetlumaczony = PrzetlumaczJesliInny(TrescWlasciwa)

      if Przetlumaczony then
         self:SetText(Licznik .. Przetlumaczony)
      else
         ZbierajCelPodrzedny(MisjaID, TrescWlasciwa)
      end

      self.TlumaczenieWTle = false
   end)

   local ObecnyTekst = ObiektTekstowy:GetText()
   if ObecnyTekst then
      ObiektTekstowy:SetText(ObecnyTekst)
   end
end

local function DlaKazdegoTrackeraMisji(Funkcja)
   local Trackery = {CampaignQuestObjectiveTracker, QuestObjectiveTracker}

   for _, Tracker in ipairs(Trackery) do
      local GlownaRamka = Tracker and Tracker.ContentsFrame
      if GlownaRamka then
         Funkcja(GlownaRamka)
      end
   end
end

local function TlumaczCelePoPrawejStronie()
   DlaKazdegoTrackeraMisji(function(GlownaRamka)
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
   end)
end

local function TlumaczTrackerKampanii()
   DlaKazdegoTrackeraMisji(function(GlownaRamka)
      local WszystkieDzieci = { GlownaRamka:GetChildren() }
      for _, PojedynczeDziecko in ipairs(WszystkieDzieci) do
         if PojedynczeDziecko.HeaderText and PojedynczeDziecko.HeaderText.GetText then
            local OryginalnyTekst = PojedynczeDziecko.HeaderText:GetText()
            local PrzetlumaczonyTekst = PrzetlumaczJesliInny(OryginalnyTekst)
            if PrzetlumaczonyTekst then
               PojedynczeDziecko.HeaderText:SetText(PrzetlumaczonyTekst)
            end
         end

         local MisjaID = PojedynczeDziecko.questID or PojedynczeDziecko.id
         local ElementyWSrodkuZadania = { PojedynczeDziecko:GetChildren() }
         for _, Element in ipairs(ElementyWSrodkuZadania) do
            if Element.Text and Element.Text.GetText then
               local PelnyTekst = Element.Text:GetText()
               if PelnyTekst then
                  local Licznik, TrescWlasciwa = RozdzielLicznikIOpis(PelnyTekst)
                  local PrzetlumaczonyTekstCelu = PrzetlumaczJesliInny(TrescWlasciwa)

                  if PrzetlumaczonyTekstCelu then
                     Element.Text:SetText(Licznik .. PrzetlumaczonyTekstCelu)
                  elseif MisjaID and MisjaID > 0 then
                     ZbierajCelPodrzedny(MisjaID, TrescWlasciwa)
                  end
               end
            end
         end
      end
   end)
end

local function PodmienTekstLokacji(self, Tekst)
   if self.TrwaTlumaczenie then
      return
   end

   if not Tekst or Tekst == "" then
      return
   end

   ZbierajNazwyKrain(Tekst)

   local PrzetlumaczonyTekst = PrzetlumaczJesliInny(Tekst)
   if not PrzetlumaczonyTekst then
      return
   end

   self.TrwaTlumaczenie = true
   C_Timer.After(0, function()
      self:SetText(PrzetlumaczonyTekst)
      self.TrwaTlumaczenie = false
   end)
end

local function ZapiszBrakujacyTekstQuestLogu(PelnyTekst)
   if not PelnyTekst or PelnyTekst == "" then
      return
   end

   local _, TrescPoLiczniku = PelnyTekst:match("^(.-%d+/%d+)%s+(.+)$")
   if TrescPoLiczniku then
      ZbierajOpisMoba(TrescPoLiczniku)
   else
      ZbierajOpisMoba(PelnyTekst)
   end
end

local function PrzetlumaczElementQuestLogu(ObiektTekstowy)
   if not ObiektTekstowy or not ObiektTekstowy.GetText or not ObiektTekstowy.SetText then
      return
   end

   local PelnyTekst = ObiektTekstowy:GetText()
   if not PelnyTekst or PelnyTekst == "" then
      return
   end

   local Przetlumaczony = PrzetlumaczLinieZLicznikiem(PelnyTekst)

   if Przetlumaczony then
      if CzyWymagaZlotegoPostepu(PelnyTekst, Przetlumaczony) then
         Przetlumaczony = PodswietlLicznikNaZloto(Przetlumaczony)
      end
      ObiektTekstowy:SetText(Przetlumaczony)
   else
      ZapiszBrakujacyTekstQuestLogu(PelnyTekst)
   end
end

local function ZahaczElementQuestLogu(ObiektTekstowy)
   if not ObiektTekstowy or not ObiektTekstowy.SetText or ObiektTekstowy.ZahaczoneQuestLogPL then
      return
   end

   ObiektTekstowy.ZahaczoneQuestLogPL = true

   hooksecurefunc(ObiektTekstowy, "SetText", function(self, NowyTekst)
      if self.TlumaczenieQuestLogWTle or not NowyTekst or NowyTekst == "" then
         return
      end

      self.TlumaczenieQuestLogWTle = true

      local Przetlumaczony = PrzetlumaczLinieZLicznikiem(NowyTekst)
      if Przetlumaczony and Przetlumaczony ~= NowyTekst then
         if CzyWymagaZlotegoPostepu(NowyTekst, Przetlumaczony) then
            Przetlumaczony = PodswietlLicznikNaZloto(Przetlumaczony)
         end
         self:SetText(Przetlumaczony)
      else
         ZapiszBrakujacyTekstQuestLogu(NowyTekst)
      end

      self.TlumaczenieQuestLogWTle = false
   end)
end

local function NalozHaczykiINatychmiastPrzetlumaczQuestLog(Obiekt)
   if not Obiekt then
      return
   end

   if Obiekt.Text then
      ZahaczElementQuestLogu(Obiekt.Text)
      PrzetlumaczElementQuestLogu(Obiekt.Text)
   end

   if Obiekt.GetRegions then
      local Regiony = { Obiekt:GetRegions() }
      for _, Region in ipairs(Regiony) do
         if Region:GetObjectType() == "FontString" then
            ZahaczElementQuestLogu(Region)
            PrzetlumaczElementQuestLogu(Region)
         end
      end
   end
end

local function PrzetlumaczZawartoscQuestLogu()
   local GlownaRamka = QuestScrollFrame and QuestScrollFrame.Contents
   if not GlownaRamka then
      return
   end

   local WszystkieDzieci = { GlownaRamka:GetChildren() }
   for _, PojedynczeDziecko in ipairs(WszystkieDzieci) do
      NalozHaczykiINatychmiastPrzetlumaczQuestLog(PojedynczeDziecko)

      local ElementyWSrodkuZadania = { PojedynczeDziecko:GetChildren() }
      for _, Element in ipairs(ElementyWSrodkuZadania) do
         NalozHaczykiINatychmiastPrzetlumaczQuestLog(Element)

         local ElementyDrugiegoPoziomu = { Element:GetChildren() }
         for _, Element2 in ipairs(ElementyDrugiegoPoziomu) do
            NalozHaczykiINatychmiastPrzetlumaczQuestLog(Element2)
         end
      end
   end

   PrzetlumaczPostepKampaniiQuestLogu()
end

local function PrzetlumaczLinieDymku(PojedynczaLinia)
   local OdczytanyTekst = PojedynczaLinia:GetText()
   if not OdczytanyTekst or OdczytanyTekst == "" then
      return
   end

   local Poczatek, TrescWlasciwa = OdczytanyTekst:match("^(.-%d+/%d+)%s+(.+)$")
   if Poczatek then
      local Przetlumaczony = PrzetlumaczJesliInny(TrescWlasciwa)
      if Przetlumaczony then
         PojedynczaLinia:SetText(Poczatek .. " " .. Przetlumaczony)
      else
         local PelneTlumaczenie = PrzetlumaczJesliInny(OdczytanyTekst)
         if PelneTlumaczenie then
            PojedynczaLinia:SetText(PelneTlumaczenie)
         else
            ZbierajTekstTooltipa(TrescWlasciwa)
         end
      end
      return
   end

   local Ikona, TekstZIkona = OdczytanyTekst:match("^(|A.-|a)%s*(.+)$")
   if not Ikona then
      Ikona, TekstZIkona = OdczytanyTekst:match("^(|T.-|t)%s*(.+)$")
   end

   if Ikona then
      local Przetlumaczony = PrzetlumaczJesliInny(TekstZIkona)
      if Przetlumaczony then
         PojedynczaLinia:SetText(Ikona .. " " .. Przetlumaczony)
      else
         ZbierajTekstTooltipa(TekstZIkona)
      end
      return
   end

   local KolorStart, Srodek, KolorKoniec = OdczytanyTekst:match("^(|c%x%x%x%x%x%x%x%x)(.-)(|r%s*)$")
   if KolorStart then
      local Przetlumaczony = PrzetlumaczJesliInny(Srodek)
      if Przetlumaczony then
         PojedynczaLinia:SetText(KolorStart .. Przetlumaczony .. KolorKoniec)
      else
         ZbierajTekstTooltipa(Srodek)
      end
      return
   end

   local PrzetlumaczonyTekst = PrzetlumaczJesliInny(OdczytanyTekst)
   if PrzetlumaczonyTekst then
      PojedynczaLinia:SetText(PrzetlumaczonyTekst)
   elseif string.len(OdczytanyTekst) > 3 then
      ZbierajTekstTooltipa(OdczytanyTekst)
   end
end

local function TlumaczGlobalnyDymek(self)
   local Wlasciciel = self:GetOwner()
   local CzyToMisja = false

   if Wlasciciel then
      local ObecnyRodzic = Wlasciciel
      for _ = 1, 5 do
         if not ObecnyRodzic then
            break
         end

         local NazwaRodzica = ObecnyRodzic:GetName()
         if NazwaRodzica
            and (string.find(NazwaRodzica, "ObjectiveTracker")
               or string.find(NazwaRodzica, "QuestMap")
               or string.find(NazwaRodzica, "QuestScroll")
               or string.find(NazwaRodzica, "WorldMapFrame")) then
            CzyToMisja = true
            break
         end

         ObecnyRodzic = ObecnyRodzic:GetParent()
      end
   end

   if not CzyToMisja then
      return
   end

   local NazwaDymku = self:GetName()
   if not NazwaDymku then
      return
   end

   local LiczbaLinii = self:NumLines()
   for i = 1, LiczbaLinii do
      local PojedynczaLinia = _G[NazwaDymku .. "TextLeft" .. i]
      if PojedynczaLinia then
         PrzetlumaczLinieDymku(PojedynczaLinia)
      end
   end
end

prywatna_tabela["PrzetlumaczTekst"] = PrzetlumaczTekst
prywatna_tabela["SkanujRamke"] = SkanujRamke
prywatna_tabela["TlumaczDymki"] = TlumaczDymki
prywatna_tabela["TlumaczDymkiCzat"] = TlumaczDymkiCzat
prywatna_tabela["TlumaczOpisKampanii"] = TlumaczOpisKampanii
prywatna_tabela["TlumaczCelePoPrawejStronie"] = TlumaczCelePoPrawejStronie
prywatna_tabela["TlumaczGlobalnyDymek"] = TlumaczGlobalnyDymek
prywatna_tabela["PodmienTekstOknienko"] = PodmienTekstOknienko
prywatna_tabela["TlumaczTrackerKampanii"] = TlumaczTrackerKampanii
prywatna_tabela["PodmienTekstLokacji"] = PodmienTekstLokacji
prywatna_tabela["PrzetlumaczZawartoscQuestLogu"] = PrzetlumaczZawartoscQuestLogu
