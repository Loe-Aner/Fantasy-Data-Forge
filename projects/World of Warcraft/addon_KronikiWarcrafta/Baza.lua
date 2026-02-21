local nazwaAddonu, prywatna_tabela = ...

-- === 0. CACHE ===

-- Funkcje Podstawowe WoW API
local CreateFrame = CreateFrame
local hooksecurefunc = hooksecurefunc
local C_Timer = C_Timer

-- Funkcje Questowe
local GetTitleText = GetTitleText
local GetQuestText = GetQuestText
local GetObjectiveText = GetObjectiveText
local GetProgressText = GetProgressText
local GetRewardText = GetRewardText
local QuestNPCModelText = QuestNPCModelText

-- Globalne Ramki UI
local QuestInfoTitleHeader = QuestInfoTitleHeader
local QuestInfoObjectivesHeader = QuestInfoObjectivesHeader
local QuestInfoObjectivesText = QuestInfoObjectivesText
local QuestInfoDescriptionText = QuestInfoDescriptionText
local QuestInfoRewardsFrame = QuestInfoRewardsFrame
local QuestInfoRewardText = QuestInfoRewardText
local QuestProgressText = QuestProgressText
local QuestInfoDescriptionHeader = QuestInfoDescriptionHeader
local QuestProgressTitleText = QuestProgressTitleText
local QuestInfoXPFrame = QuestInfoXPFrame
local ObjectiveTrackerFrame = ObjectiveTrackerFrame
local CampaignQuestObjectiveTracker = CampaignQuestObjectiveTracker
local C_GossipInfo = C_GossipInfo
local GossipGreetingText = GossipGreetingText
local GossipFrame = GossipFrame
local MinimapZoneText = MinimapZoneText
local ZoneTextString = ZoneTextString
local SubZoneTextString = SubZoneTextString
local QuestScrollFrame = QuestScrollFrame
local QuestMapFrame = QuestMapFrame
local MapQuestInfoRewardsFrame = MapQuestInfoRewardsFrame
local QuestObjectiveTracker = QuestObjectiveTracker
local GameTooltip = GameTooltip

-- Funkcje z innych plikow
local InicjujDB = prywatna_tabela["InicjujDB"]
local ZbierajMisje = prywatna_tabela["ZbierajMisje"]
local ZbierajGossipy = prywatna_tabela["ZbierajGossipy"]
local ZbierajDymki = prywatna_tabela["ZbierajDymki"]
local PrzetlumaczTekst = prywatna_tabela["PrzetlumaczTekst"]
local TlumaczDymki = prywatna_tabela["TlumaczDymki"]
local TlumaczDymkiCzat = prywatna_tabela["TlumaczDymkiCzat"]
local ZbierajCelPodrzedny = prywatna_tabela["ZbierajCelPodrzedny"]
local ZbierajOpisMoba = prywatna_tabela["ZbierajOpisMoba"]
local ZbierajNazwyKrain = prywatna_tabela["ZbierajNazwyKrain"]
local DostosujKolorkiFont = prywatna_tabela["DostosujKolorkiFont"]
local FontTytulu = prywatna_tabela["FontTytulu"]
local FontTresci = prywatna_tabela["FontTresci"]
local TlumaczOpisKampanii = prywatna_tabela["TlumaczOpisKampanii"]
local ZbierajOpisKampanii = prywatna_tabela["ZbierajOpisKampanii"]
local TlumaczCelePoPrawejStronie = prywatna_tabela["TlumaczCelePoPrawejStronie"]
local TlumaczGlobalnyDymek = prywatna_tabela["TlumaczGlobalnyDymek"]

local ramka = CreateFrame("Frame")
ramka:RegisterEvent("ADDON_LOADED")
ramka:RegisterEvent("QUEST_DETAIL")
ramka:RegisterEvent("QUEST_PROGRESS")
ramka:RegisterEvent("QUEST_COMPLETE")
ramka:RegisterEvent("GOSSIP_SHOW")
ramka:RegisterEvent("QUEST_LOG_UPDATE")
ramka:RegisterEvent("CHAT_MSG_MONSTER_SAY")      -- Mówienie
ramka:RegisterEvent("CHAT_MSG_MONSTER_YELL")     -- Krzyczenie
ramka:RegisterEvent("CHAT_MSG_MONSTER_WHISPER")  -- Szeptanie
ramka:RegisterEvent("CHAT_MSG_MONSTER_EMOTE")    -- Emotki
ramka:RegisterEvent("ZONE_CHANGED")              -- zmiana krainy
ramka:RegisterEvent("ZONE_CHANGED_NEW_AREA")
ramka:RegisterEvent("ZONE_CHANGED_INDOORS")

-- === FUNKCJA MODYFIKUJĄCA UI ===
local function PodmienTekstOknienko()
-- 1. TYTUŁ
    local TytulDoTlumaczenia = nil

    if QuestMapFrame and QuestMapFrame:IsVisible() then
        local MisjaID = QuestMapFrame_GetDetailQuestID() 
        
        if MisjaID and MisjaID > 0 then
             TytulDoTlumaczenia = C_QuestLog.GetTitleForQuestID(MisjaID)
        end
    end

    if not TytulDoTlumaczenia then
        TytulDoTlumaczenia = GetTitleText()
    end

    if (not TytulDoTlumaczenia) and QuestInfoTitleHeader then
        TytulDoTlumaczenia = QuestInfoTitleHeader:GetText()
    end

    if TytulDoTlumaczenia then
        local TytulPL = PrzetlumaczTekst(TytulDoTlumaczenia)
        if TytulPL then
            DostosujKolorkiFont(QuestInfoTitleHeader, TytulPL, FontTytulu, 18, nil, nil, nil, true)
        end
    end

    -- obsluga dynamicznych podrzednych celi (te po wcisnieciu M)
    local i = 1
    local MisjaID = QuestMapFrame_GetDetailQuestID() 

    if not MisjaID or MisjaID == 0 then 
        MisjaID = GetQuestID() 
    end

    while true do
        local PodrzednyCel = _G["QuestInfoObjective" .. i]

        if not PodrzednyCel then break end
        if PodrzednyCel:IsVisible() and PodrzednyCel:GetText() then
            local TekstPelny = PodrzednyCel:GetText() -- np. "0/7 Quilboar slain"
            local Licznik, TrescWlasciwa = TekstPelny:match("^(%d+/%d+)%s+(.+)$")

            if not Licznik then
                Licznik = ""
                TrescWlasciwa = TekstPelny
            else
                Licznik = Licznik .. " "
            end

            local Tlumaczenie = PrzetlumaczTekst(TrescWlasciwa)
            if Tlumaczenie and Tlumaczenie ~= "" and Tlumaczenie ~= TrescWlasciwa then
                DostosujKolorkiFont(PodrzednyCel, Licznik .. Tlumaczenie, FontTresci, 14, 0, 0, 0, true)
            else
                if MisjaID and MisjaID > 0 then
                    ZbierajCelPodrzedny(MisjaID, TrescWlasciwa) -- ===== chwilowo niestety zapisuje tez polski tekst...... =====
                end
            end
        end
        i = i + 1
    end

    -- 2. CELE MISJI (Nagłówek)
    if QuestInfoObjectivesHeader then
        DostosujKolorkiFont(QuestInfoObjectivesHeader, "Cele misji", FontTytulu, 18, 0, 0, 0, true)
    end

    -- 3. NAGRODY (Nagłówek)
    if QuestInfoRewardsFrame then
        if QuestInfoRewardsFrame.Header then
            DostosujKolorkiFont(QuestInfoRewardsFrame.Header, "Nagrody", FontTytulu, 18, 0, 0, 0, true)
        end
        
        if QuestInfoRewardsFrame.ItemReceiveText then
            DostosujKolorkiFont(QuestInfoRewardsFrame.ItemReceiveText, "Otrzymasz:", FontTresci, 14, 0, 0, 0, true)
        end
    end

    -- 3.1 NAGRODY (jak nacisne M)
    if QuestMapFrame.QuestsFrame.DetailsFrame.RewardsFrameContainer.RewardsFrame then
        DostosujKolorkiFont(QuestMapFrame.QuestsFrame.DetailsFrame.RewardsFrameContainer.RewardsFrame.Label, "Nagrody", FontTytulu, 18, 0.85, 0.77, 0.60, true)
    end

    -- 3.2 NAGRODY - you will (also) receive: (jak nacisne M)
    if MapQuestInfoRewardsFrame and MapQuestInfoRewardsFrame.ItemReceiveText then
        local TekstEN = MapQuestInfoRewardsFrame.ItemReceiveText:GetText()
        local TekstPL = nil
    
        if TekstEN == "You will receive:" then
            TekstPL = "Otrzymasz:"
        elseif TekstEN == "You will also receive:" then
            TekstPL = "Otrzymasz również:"
        end
    
        if TekstPL then
            DostosujKolorkiFont(MapQuestInfoRewardsFrame.ItemReceiveText, TekstPL, FontTresci, 11, 0.85, 0.77, 0.60, true)
        end
    end

    -- 3.3 NAGRODY - item choose (jak nacisne M)
    if MapQuestInfoRewardsFrame and MapQuestInfoRewardsFrame.ItemChooseText then
        local TekstEN = MapQuestInfoRewardsFrame.ItemChooseText:GetText()
        local TekstPL = nil
    
        if TekstEN == "You will receive:" then
            TekstPL = "Otrzymasz przedmiot:"
        elseif TekstEN == "You will receive one of:" then
            TekstPL = "Otrzymasz jeden z poniższych:"
        end
    
        if TekstPL then
            DostosujKolorkiFont(MapQuestInfoRewardsFrame.ItemChooseText, TekstPL, FontTresci, 11, 0.85, 0.77, 0.60, true)
        end
    end

    -- 4. DOŚWIADCZENIE
    if QuestInfoXPFrame and QuestInfoXPFrame.ReceiveText then
        DostosujKolorkiFont(QuestInfoXPFrame.ReceiveText, "Doświadczenie:", FontTresci, 14, 0, 0, 0, true)
    end

    -- === TREŚCI WŁAŚCIWE ===
    local OpisOryginal = GetQuestText()

    if (not OpisOryginal or OpisOryginal == "") and QuestInfoDescriptionText then
        OpisOryginal = QuestInfoDescriptionText:GetText()
    end -- tlumaczy po nacisnieciu M

    if OpisOryginal then
        local OpisPL = PrzetlumaczTekst(OpisOryginal)
        if OpisPL and QuestInfoDescriptionText then 
            DostosujKolorkiFont(QuestInfoDescriptionText, OpisPL, FontTresci, 14, 0, 0, 0, true)
        end
    end -- tlumaczy po nacisnieciu M

    if QuestInfoDescriptionHeader then
        DostosujKolorkiFont(QuestInfoDescriptionHeader, "Opis", FontTytulu, 18, 0, 0, 0, true)
    end  -- tlumaczy po nacisnieciu M

    local CelOryginal = GetObjectiveText()

    if (not CelOryginal or CelOryginal == "") and QuestInfoObjectivesText:IsVisible() then
        CelOryginal = QuestInfoObjectivesText:GetText()
    end -- tlumaczy po nacisnieciu M

    local OpisMoba = QuestNPCModelText:GetText()
    if OpisMoba then
        ZbierajOpisMoba(OpisMoba)
        local OpisMobaPL = PrzetlumaczTekst(OpisMoba)
        if OpisMobaPL and OpisMobaPL ~= "" and OpisMobaPL ~= OpisMoba then
            QuestNPCModelText:SetText(OpisMobaPL)
        end
    end  -- to sie nie zmienia w trakcie gry, jest stale

    if CelOryginal and QuestInfoObjectivesText:IsVisible() then
        local CelPL = PrzetlumaczTekst(CelOryginal)
        if CelPL then
            DostosujKolorkiFont(QuestInfoObjectivesText, CelPL, FontTresci, 14, 0, 0, 0, true)
        end
    end

    local PostepOryginal = GetProgressText()
    if PostepOryginal and QuestProgressText and QuestProgressText:IsVisible() then
        local PostepPL = PrzetlumaczTekst(PostepOryginal)
        if PostepPL then
            DostosujKolorkiFont(QuestProgressText, PostepPL, FontTresci, 14, 0, 0, 0, true)
        end
    end

    if QuestProgressTitleText and QuestProgressTitleText:IsVisible() then
        local Tytul = GetTitleText()
        local TytulPL = PrzetlumaczTekst(Tytul)
        
        if TytulPL then
            DostosujKolorkiFont(QuestProgressTitleText, TytulPL, FontTytulu, 18, 0, 0, 0, true)
        end
    end

    local ZakonczenieOryginal = GetRewardText()
    if ZakonczenieOryginal and QuestInfoRewardText and QuestInfoRewardText:IsVisible() then
        local ZakonczeniePL = PrzetlumaczTekst(ZakonczenieOryginal)
        if ZakonczeniePL then
            DostosujKolorkiFont(QuestInfoRewardText, ZakonczeniePL, FontTresci, 14, 0, 0, 0, true)
        end
    end

    local GossipDialog = C_GossipInfo.GetText()
    if GossipDialog and GossipFrame and GossipFrame:IsVisible() then
        local GossipDialogPL = PrzetlumaczTekst(GossipDialog)    

        if GossipGreetingText and GossipGreetingText:IsShown() then
            DostosujKolorkiFont(GossipGreetingText, GossipDialogPL, FontTresci, 14, 0, 0, 0, true)
        end

        if GossipFrame.GreetingPanel and GossipFrame.GreetingPanel.ScrollBox then
            local RamkiWLiscie = GossipFrame.GreetingPanel.ScrollBox:GetFrames()

            for _, PojedynczaRamka in ipairs(RamkiWLiscie) do
                if PojedynczaRamka.GreetingText and PojedynczaRamka.GreetingText:GetText() then
                    local ObecnyTekst = PojedynczaRamka.GreetingText:GetText()
                    local TekstPL = PrzetlumaczTekst(ObecnyTekst)
                    
                    if TekstPL then
                        DostosujKolorkiFont(PojedynczaRamka.GreetingText, TekstPL, FontTresci, 14, 0, 0, 0, true)
                    end
                end

                if PojedynczaRamka.GetText and PojedynczaRamka:GetText() then
                    local TekstPrzycisku = PojedynczaRamka:GetText()
                    local TekstPL = PrzetlumaczTekst(TekstPrzycisku)

                    if TekstPL then
                        PojedynczaRamka:SetText(TekstPL)

                        if PojedynczaRamka.GetFontString then
                            local FontString = PojedynczaRamka:GetFontString()
                            if FontString then
                                FontString:SetFont(FontTresci, 14)
                                FontString:SetTextColor(0, 0, 0) -- te kody w grze np 'cFF0000FF' maja wyzszy priorytet
                            end
                        end
                    end
                end
            end
        end
    end
end

-- === FUNKCJA TŁUMACZĄCA TRACKER KAMPANII ===
local function TlumaczTrackerKampanii()
    local GlownaRamka = CampaignQuestObjectiveTracker.ContentsFrame
    if GlownaRamka then
        -- to tlumaczy tytuly zadan po prawej stronie
        local WszystkieDzieci = {GlownaRamka:GetChildren()}
        for _, PojedynczeDziecko in ipairs(WszystkieDzieci) do
            if PojedynczeDziecko["HeaderText"] and PojedynczeDziecko["HeaderText"]["GetText"] then
                local OryginalnyTekst = PojedynczeDziecko["HeaderText"]:GetText()
                if OryginalnyTekst then
                    local PrzetlumaczonyTekst = PrzetlumaczTekst(OryginalnyTekst)
                    if PrzetlumaczonyTekst and PrzetlumaczonyTekst ~= "" and PrzetlumaczonyTekst ~= OryginalnyTekst then
                        PojedynczeDziecko["HeaderText"]:SetText(PrzetlumaczonyTekst)
                    end
                end
            end
            -- to tlumaczy zadania (cele) po prawej stronie
            local MisjaID = PojedynczeDziecko["questID"] or PojedynczeDziecko.id
            local ElementyWSrodkuZadania = {PojedynczeDziecko:GetChildren()}
            for _, Element in ipairs(ElementyWSrodkuZadania) do
                if Element["Text"] and Element["Text"]["GetText"] then
                    local PelnyTekst = Element["Text"]:GetText()
                    
                    if PelnyTekst then
                        local Licznik, TrescWlasciwa = PelnyTekst:match("^(%d+/%d+)%s+(.+)$")
                        
                        if not Licznik then
                            Licznik = ""
                            TrescWlasciwa = PelnyTekst
                        else
                            Licznik = Licznik .. " "
                        end
                        
                        local PrzetlumaczonyTekstCelu = PrzetlumaczTekst(TrescWlasciwa)
                        
                        if PrzetlumaczonyTekstCelu and PrzetlumaczonyTekstCelu ~= "" and PrzetlumaczonyTekstCelu ~= TrescWlasciwa then
                            Element["Text"]:SetText(Licznik .. PrzetlumaczonyTekstCelu)
                        else
                            -- zapisz brakujacy cel do bazy (tylko angielski tekst)
                            if MisjaID and MisjaID > 0 and TrescWlasciwa then
                                ZbierajCelPodrzedny(MisjaID, TrescWlasciwa)
                            end
                        end
                    end
                end
            end
        end
    end
end

local TrwaTlumaczenieMinimapy = false

local function PodmienTekstLokacji(self, tekst)
    if self["TrwaTlumaczenie"] then return end
    if not tekst or tekst == "" then return end

    ZbierajNazwyKrain(tekst)
    local PrzetlumaczonyTekst = PrzetlumaczTekst(tekst)

    if PrzetlumaczonyTekst and PrzetlumaczonyTekst ~= "" and PrzetlumaczonyTekst ~= tekst then
        self["TrwaTlumaczenie"] = true
        
        C_Timer.After(0, function()
            self:SetText(PrzetlumaczonyTekst)
            self["TrwaTlumaczenie"] = false
        end)
    end
end

local function PrzetlumaczZawartoscQuestLogu()
    local GlownaRamka = QuestScrollFrame.Contents
    
    if not GlownaRamka then
        return
    end
    
    local WszystkieDzieci = {GlownaRamka:GetChildren()}
    
    for _, PojedynczeDziecko in ipairs(WszystkieDzieci) do
        if PojedynczeDziecko.Text and PojedynczeDziecko.Text.GetText then
            local OdczytanyTekst = PojedynczeDziecko.Text:GetText()
            
            if OdczytanyTekst then
                local Licznik, TrescWlasciwa = OdczytanyTekst:match("^(%d+/%d+)%s+(.+)$")
                
                if not Licznik then
                    local PrzetlumaczonyTekst = PrzetlumaczTekst(OdczytanyTekst)
                    if PrzetlumaczonyTekst and PrzetlumaczonyTekst ~= "" and PrzetlumaczonyTekst ~= OdczytanyTekst then
                        PojedynczeDziecko.Text:SetText(PrzetlumaczonyTekst)
                    else
                        ZbierajOpisMoba(OdczytanyTekst)
                    end
                else
                    local PrzetlumaczonyTekstCelu = PrzetlumaczTekst(TrescWlasciwa)
                    if PrzetlumaczonyTekstCelu and PrzetlumaczonyTekstCelu ~= "" and PrzetlumaczonyTekstCelu ~= TrescWlasciwa then
                        PojedynczeDziecko.Text:SetText(Licznik .. " " .. PrzetlumaczonyTekstCelu)
                    else
                        ZbierajOpisMoba(TrescWlasciwa)
                    end
                end
            end
        end
        
        local ElementyWSrodkuZadania = {PojedynczeDziecko:GetChildren()}
        
        for _, Element in ipairs(ElementyWSrodkuZadania) do
            if Element.Text and Element.Text.GetText then
                local PelnyTekst = Element.Text:GetText()
                
                if PelnyTekst then
                    local Licznik, TrescWlasciwa = PelnyTekst:match("^(%d+/%d+)%s+(.+)$")
                    
                    if not Licznik then
                        Licznik = ""
                        TrescWlasciwa = PelnyTekst
                    else
                        Licznik = Licznik .. " "
                    end
                    
                    local PrzetlumaczonyTekstCelu = PrzetlumaczTekst(TrescWlasciwa)
                    
                    if PrzetlumaczonyTekstCelu and PrzetlumaczonyTekstCelu ~= "" and PrzetlumaczonyTekstCelu ~= TrescWlasciwa then
                        Element.Text:SetText(Licznik .. PrzetlumaczonyTekstCelu)
                    else
                        ZbierajOpisMoba(TrescWlasciwa)
                    end
                end
            end
        end
    end
end

-- === OBSLUGA EVENTOW ===
local function GlownyHandler(self, event, ...)
    if event == "ADDON_LOADED" then
        local nazwaZaladowanegoAddonu = ...

        if nazwaZaladowanegoAddonu == nazwaAddonu then
            InicjujDB(self, event, ...)
            C_Timer.NewTicker(0.33, TlumaczDymki) -- 3 razy na sekunde sprawdza czy jest dymek na ekranie
            self:UnregisterEvent("ADDON_LOADED")

            -- filtr na czat: zanim wyswietlisz tekst, przepusc go przez funkcje filtrujaca; dzieki temu w czacie jest tekst PL
            ChatFrame_AddMessageEventFilter("CHAT_MSG_MONSTER_SAY", TlumaczDymkiCzat)
            ChatFrame_AddMessageEventFilter("CHAT_MSG_MONSTER_YELL", TlumaczDymkiCzat)
            ChatFrame_AddMessageEventFilter("CHAT_MSG_MONSTER_WHISPER", TlumaczDymkiCzat)
            ChatFrame_AddMessageEventFilter("CHAT_MSG_MONSTER_EMOTE", TlumaczDymkiCzat)  

            if ObjectiveTrackerFrame then
                ObjectiveTrackerFrame.headerText = "Wszystkie zadania"
                if ObjectiveTrackerFrame.Header and ObjectiveTrackerFrame.Header.Text then
                    ObjectiveTrackerFrame.Header.Text:SetText("Wszystkie zadania")
                end
            end

            if CampaignQuestObjectiveTracker then
                CampaignQuestObjectiveTracker.headerText = "Kampania"
                if CampaignQuestObjectiveTracker.Header and CampaignQuestObjectiveTracker.Header.Text then
                    CampaignQuestObjectiveTracker.Header.Text:SetText("Kampania")
                end
            end

            if QuestObjectiveTracker then
                QuestObjectiveTracker.headerText = "Misje"
                if QuestObjectiveTracker.Header and QuestObjectiveTracker.Header.Text then
                    QuestObjectiveTracker.Header.Text:SetText("Misje")
                end
            end
            PodmienTekstOknienko() -- po wejsciu do gry tlumaczy stale elementy, jak naglowki All Objectives/Campaign
        end

    elseif event == "QUEST_DETAIL" or event == "QUEST_PROGRESS" or event == "QUEST_COMPLETE" then
        ZbierajMisje(self, event, ...)

        if event == "QUEST_PROGRESS" then
            C_Timer.After(0, PodmienTekstOknienko)
        end

    elseif event == "GOSSIP_SHOW" then
        ZbierajGossipy(self, event, ...)
        C_Timer.After(0, PodmienTekstOknienko) -- 0 oznacza 'w nastepnej klatce'

    elseif event == "QUEST_LOG_UPDATE" then
        C_Timer.After(0, PodmienTekstOknienko) -- podmienia tytul w objectivach (po prawej stronie)

    elseif event == "CHAT_MSG_MONSTER_SAY"     or 
           event == "CHAT_MSG_MONSTER_YELL"    or 
           event == "CHAT_MSG_MONSTER_WHISPER" or 
           event == "CHAT_MSG_MONSTER_EMOTE"   then
           
        ZbierajDymki(self, event, ...)
           end
        end

hooksecurefunc("QuestInfo_Display", function(template, parentFrame, acceptButton, material, mapView)
    PodmienTekstOknienko()
end) -- podstawia dane do misji

hooksecurefunc("QuestMapFrame_ShowQuestDetails", function(MisjaID)
    PodmienTekstOknienko()
end) -- aby pokazaly sie dane dla misji po kliknieciu 'M'

if CampaignQuestObjectiveTracker then
    hooksecurefunc(CampaignQuestObjectiveTracker, "Update", function(...)
        C_Timer.After(0.1, function()
            TlumaczTrackerKampanii()
            TlumaczCelePoPrawejStronie()
        end)
    end)
end

local RamkaInicjalizacyjna = CreateFrame("Frame")
RamkaInicjalizacyjna:RegisterEvent("PLAYER_ENTERING_WORLD")
RamkaInicjalizacyjna:SetScript("OnEvent", function()
    C_Timer.After(2.5, function()
        if ObjectiveTrackerFrame and ObjectiveTrackerFrame.Update then
            ObjectiveTrackerFrame:Update()
        end
    end)
end)

if QuestMapFrame and QuestMapFrame.QuestsFrame and QuestMapFrame.QuestsFrame.CampaignOverview then
    QuestMapFrame.QuestsFrame.CampaignOverview:HookScript("OnShow", function()
        C_Timer.After(0.1, function()
            ZbierajOpisKampanii()
            TlumaczOpisKampanii()
        end)
    end)
end

if MinimapZoneText then
    hooksecurefunc(MinimapZoneText, "SetText", PodmienTekstLokacji)
end

if ZoneTextString then
    hooksecurefunc(ZoneTextString, "SetText", PodmienTekstLokacji)
end

if SubZoneTextString then
    hooksecurefunc(SubZoneTextString, "SetText", PodmienTekstLokacji)
end

if GameTooltip then
    GameTooltip:HookScript("OnShow", function(self)
       C_Timer.After(0.05, function()
          TlumaczGlobalnyDymek(self)
       end)
    end)
 end

if QuestScrollFrame then
    if QuestScrollFrame.Update then
        hooksecurefunc(QuestScrollFrame, "Update", function()
            C_Timer.After(0, PrzetlumaczZawartoscQuestLogu)
        end)
    elseif QuestScrollFrame.update then
        hooksecurefunc(QuestScrollFrame, "update", function()
            C_Timer.After(0, PrzetlumaczZawartoscQuestLogu)
        end)
    end
    
    QuestScrollFrame:HookScript("OnShow", function()
        C_Timer.After(0, PrzetlumaczZawartoscQuestLogu)
    end)
end

ramka:SetScript("OnEvent", GlownyHandler)