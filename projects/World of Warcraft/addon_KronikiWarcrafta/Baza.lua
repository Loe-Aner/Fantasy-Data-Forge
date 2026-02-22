local nazwaAddonu, prywatna_tabela = ...

local C_Timer = C_Timer
local ChatFrame_AddMessageEventFilter = ChatFrame_AddMessageEventFilter
local CreateFrame = CreateFrame
local GameTooltip = GameTooltip
local MinimapZoneText = MinimapZoneText
local ObjectiveTrackerFrame = ObjectiveTrackerFrame
local CampaignQuestObjectiveTracker = CampaignQuestObjectiveTracker
local QuestMapFrame = QuestMapFrame
local QuestObjectiveTracker = QuestObjectiveTracker
local QuestScrollFrame = QuestScrollFrame
local SubZoneTextString = SubZoneTextString
local ZoneTextString = ZoneTextString
local hooksecurefunc = hooksecurefunc
local ipairs = ipairs

local InicjujDB = prywatna_tabela["InicjujDB"]
local ZbierajMisje = prywatna_tabela["ZbierajMisje"]
local ZbierajGossipy = prywatna_tabela["ZbierajGossipy"]
local ZbierajDymki = prywatna_tabela["ZbierajDymki"]
local ZbierajOpisKampanii = prywatna_tabela["ZbierajOpisKampanii"]
local ZbierajPostepKampaniiQuestLogu = prywatna_tabela["ZbierajPostepKampaniiQuestLogu"]
local ZbierajTooltipKampaniiQuestLogu = prywatna_tabela["ZbierajTooltipKampaniiQuestLogu"]
local ZbierajWidoczneTooltipyQuestLogu = prywatna_tabela["ZbierajWidoczneTooltipyQuestLogu"]
local ZahaczHoverPostepuKampaniiQuestLogu = prywatna_tabela["ZahaczHoverPostepuKampaniiQuestLogu"]

local PodmienTekstOknienko = prywatna_tabela["PodmienTekstOknienko"]
local TlumaczDymki = prywatna_tabela["TlumaczDymki"]
local TlumaczDymkiCzat = prywatna_tabela["TlumaczDymkiCzat"]
local TlumaczOpisKampanii = prywatna_tabela["TlumaczOpisKampanii"]
local TlumaczCelePoPrawejStronie = prywatna_tabela["TlumaczCelePoPrawejStronie"]
local TlumaczTrackerKampanii = prywatna_tabela["TlumaczTrackerKampanii"]
local TlumaczGlobalnyDymek = prywatna_tabela["TlumaczGlobalnyDymek"]
local PodmienTekstLokacji = prywatna_tabela["PodmienTekstLokacji"]
local PrzetlumaczZawartoscQuestLogu = prywatna_tabela["PrzetlumaczZawartoscQuestLogu"]

local EVENTY_CZAT_MONSTER = {
   "CHAT_MSG_MONSTER_SAY",
   "CHAT_MSG_MONSTER_YELL",
   "CHAT_MSG_MONSTER_WHISPER",
   "CHAT_MSG_MONSTER_EMOTE",
}

local function UstawNaglowekTrackera(Tracker, Tekst)
   if not Tracker then
      return
   end

   Tracker.headerText = Tekst
   if Tracker.Header and Tracker.Header.Text then
      Tracker.Header.Text:SetText(Tekst)
   end
end

local function OpoznionePodmianaOknienka()
   if PodmienTekstOknienko then
      C_Timer.After(0, PodmienTekstOknienko)
   end
end

local function ObsluzAddonLoaded(self, NazwaZaladowanegoAddonu)
   if NazwaZaladowanegoAddonu ~= nazwaAddonu then
      return
   end

   if InicjujDB then
      InicjujDB(self, "ADDON_LOADED", NazwaZaladowanegoAddonu)
   end

   if TlumaczDymki then
      C_Timer.NewTicker(0.33, TlumaczDymki)
   end

   if ChatFrame_AddMessageEventFilter and TlumaczDymkiCzat then
      for _, EventCzatu in ipairs(EVENTY_CZAT_MONSTER) do
         ChatFrame_AddMessageEventFilter(EventCzatu, TlumaczDymkiCzat)
      end
   end

   UstawNaglowekTrackera(ObjectiveTrackerFrame, "Wszystkie zadania")
   UstawNaglowekTrackera(CampaignQuestObjectiveTracker, "Kampania")
   UstawNaglowekTrackera(QuestObjectiveTracker, "Misje")

   if PodmienTekstOknienko then
      PodmienTekstOknienko()
   end

   self:UnregisterEvent("ADDON_LOADED")
end

local function ObsluzEventMisji(self, event, ...)
   if ZbierajMisje then
      ZbierajMisje(self, event, ...)
   end

   if event == "QUEST_PROGRESS" then
      OpoznionePodmianaOknienka()
   end
end

local function ObsluzEventGossip(self, event, ...)
   if ZbierajGossipy then
      ZbierajGossipy(self, event, ...)
   end

   OpoznionePodmianaOknienka()
end

local function ObsluzEventQuestLog()
   OpoznionePodmianaOknienka()
end

local function ObsluzEventDymki(self, event, ...)
   if ZbierajDymki then
      ZbierajDymki(self, event, ...)
   end
end

local EVENT_HANDLERS = {
   ["ADDON_LOADED"] = function(self, _eventName, ...)
      local NazwaZaladowanegoAddonu = ...
      ObsluzAddonLoaded(self, NazwaZaladowanegoAddonu)
   end,
   ["QUEST_DETAIL"] = ObsluzEventMisji,
   ["QUEST_PROGRESS"] = ObsluzEventMisji,
   ["QUEST_COMPLETE"] = ObsluzEventMisji,
   ["GOSSIP_SHOW"] = ObsluzEventGossip,
   ["QUEST_LOG_UPDATE"] = ObsluzEventQuestLog,
}

for _, EventCzatu in ipairs(EVENTY_CZAT_MONSTER) do
   EVENT_HANDLERS[EventCzatu] = ObsluzEventDymki
end

local function GlownyHandler(self, event, ...)
   local Handler = EVENT_HANDLERS[event]
   if Handler then
      Handler(self, event, ...)
   end
end

local function HookujSzczegolyMisji()
   if not PodmienTekstOknienko then
      return
   end

   hooksecurefunc("QuestInfo_Display", function()
      PodmienTekstOknienko()
   end)

   hooksecurefunc("QuestMapFrame_ShowQuestDetails", function()
      PodmienTekstOknienko()
   end)
end

local function HookujTrackerKampanii()
   if not CampaignQuestObjectiveTracker then
      return
   end

   hooksecurefunc(CampaignQuestObjectiveTracker, "Update", function()
      C_Timer.After(0.1, function()
         if TlumaczTrackerKampanii then
            TlumaczTrackerKampanii()
         end

         if TlumaczCelePoPrawejStronie then
            TlumaczCelePoPrawejStronie()
         end
      end)
   end)
end

local function HookujOpisKampanii()
   local CampaignOverview = QuestMapFrame and QuestMapFrame.QuestsFrame and QuestMapFrame.QuestsFrame.CampaignOverview
   if not CampaignOverview then
      return
   end

   CampaignOverview:HookScript("OnShow", function()
      C_Timer.After(0.1, function()
         if ZbierajOpisKampanii then
            ZbierajOpisKampanii()
         end

         if TlumaczOpisKampanii then
            TlumaczOpisKampanii()
         end

         if ZbierajPostepKampaniiQuestLogu then
            ZbierajPostepKampaniiQuestLogu()
         end

         if ZahaczHoverPostepuKampaniiQuestLogu then
            ZahaczHoverPostepuKampaniiQuestLogu()
         end

         if ZbierajWidoczneTooltipyQuestLogu then
            ZbierajWidoczneTooltipyQuestLogu()
         end
      end)
   end)
end

local function HookujNazwyStref()
   if not PodmienTekstLokacji then
      return
   end

   local EtykietyStref = { MinimapZoneText, ZoneTextString, SubZoneTextString }
   for _, Etykieta in ipairs(EtykietyStref) do
      if Etykieta then
         hooksecurefunc(Etykieta, "SetText", PodmienTekstLokacji)
      end
   end
end

local function HookujTooltip()
   if not GameTooltip then
      return
   end

   GameTooltip:HookScript("OnShow", function(self)
      C_Timer.After(0.05, function()
         if ZbierajTooltipKampaniiQuestLogu then
            ZbierajTooltipKampaniiQuestLogu(self)
         end

         if TlumaczGlobalnyDymek then
            TlumaczGlobalnyDymek(self)
         end
      end)
   end)
end

local function HookujQuestLog()
   if not QuestScrollFrame or (not PrzetlumaczZawartoscQuestLogu and not ZbierajPostepKampaniiQuestLogu) then
      return
   end

   local function OpoznioneTlumaczenieQuestLogu()
      C_Timer.After(0, function()
         if PrzetlumaczZawartoscQuestLogu then
            PrzetlumaczZawartoscQuestLogu()
         end

         if ZbierajPostepKampaniiQuestLogu then
            ZbierajPostepKampaniiQuestLogu()
         end

         if ZahaczHoverPostepuKampaniiQuestLogu then
            ZahaczHoverPostepuKampaniiQuestLogu()
         end

         if ZbierajWidoczneTooltipyQuestLogu then
            ZbierajWidoczneTooltipyQuestLogu()
         end
      end)
   end

   if QuestScrollFrame.Update then
      hooksecurefunc(QuestScrollFrame, "Update", OpoznioneTlumaczenieQuestLogu)
   elseif QuestScrollFrame.update then
      hooksecurefunc(QuestScrollFrame, "update", OpoznioneTlumaczenieQuestLogu)
   end

   QuestScrollFrame:HookScript("OnShow", OpoznioneTlumaczenieQuestLogu)

   if not QuestScrollFrame.KronikiTooltipOnUpdateHooked then
      QuestScrollFrame.KronikiTooltipOnUpdateHooked = true
      QuestScrollFrame.KronikiTooltipOnUpdateElapsed = 0

      QuestScrollFrame:HookScript("OnUpdate", function(self, elapsed)
         if not ZbierajWidoczneTooltipyQuestLogu then
            return
         end

         self.KronikiTooltipOnUpdateElapsed = (self.KronikiTooltipOnUpdateElapsed or 0) + (elapsed or 0)
         if self.KronikiTooltipOnUpdateElapsed < 0.2 then
            return
         end

         self.KronikiTooltipOnUpdateElapsed = 0
         ZbierajWidoczneTooltipyQuestLogu()
      end)
   end
end

local function HookujPierwszyRefreshTrackera()
   local RamkaInicjalizacyjna = CreateFrame("Frame")
   RamkaInicjalizacyjna:RegisterEvent("PLAYER_ENTERING_WORLD")
   RamkaInicjalizacyjna:SetScript("OnEvent", function()
      C_Timer.After(2.5, function()
         if ObjectiveTrackerFrame and ObjectiveTrackerFrame.Update then
            ObjectiveTrackerFrame:Update()
         end
      end)
   end)
end

local ramka = CreateFrame("Frame")
ramka:RegisterEvent("ADDON_LOADED")
ramka:RegisterEvent("QUEST_DETAIL")
ramka:RegisterEvent("QUEST_PROGRESS")
ramka:RegisterEvent("QUEST_COMPLETE")
ramka:RegisterEvent("GOSSIP_SHOW")
ramka:RegisterEvent("QUEST_LOG_UPDATE")

for _, EventCzatu in ipairs(EVENTY_CZAT_MONSTER) do
   ramka:RegisterEvent(EventCzatu)
end

ramka:SetScript("OnEvent", GlownyHandler)

HookujSzczegolyMisji()
HookujTrackerKampanii()
HookujOpisKampanii()
HookujNazwyStref()
HookujTooltip()
HookujQuestLog()
HookujPierwszyRefreshTrackera()
