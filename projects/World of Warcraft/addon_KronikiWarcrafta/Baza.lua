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
    local FontTytulu = "Interface\\AddOns\\addon_KronikiWarcrafta\\Media\\MorpheusPL.ttf"     -- ========= TO PUSCIC NA ZMIENNA GLOBALNA =========
    local FontTresci = "Interface\\AddOns\\addon_KronikiWarcrafta\\Media\\FrizQuadrataPL.ttf" -- ========= TO PUSCIC NA ZMIENNA GLOBALNA =========

-- 1. TYTUŁ
    local TytulDoTlumaczenia = nil

    if QuestMapFrame and QuestMapFrame:IsVisible() then
        local MisjaID = QuestMapFrame_GetDetailQuestID() 
        
        if MisjaID and MisjaID > 0 then
             TytulDoTlumaczenia = C_QuestLog.GetTitleForQuestID(MisjaID)
        end
    end

    if (not TytulDoTlumaczenia) then
        TytulDoTlumaczenia = GetTitleText()
    end

    if (not TytulDoTlumaczenia) and QuestInfoTitleHeader then
        TytulDoTlumaczenia = QuestInfoTitleHeader:GetText()
    end

    if TytulDoTlumaczenia then
        local TytulPL = PrzetlumaczTekst(TytulDoTlumaczenia)
        if TytulPL then
            QuestInfoTitleHeader:SetText(TytulPL)
            QuestInfoTitleHeader:SetFont(FontTytulu, 18)
            QuestInfoTitleHeader:SetTextColor(0, 0, 0)
            QuestInfoTitleHeader:Show()
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
                PodrzednyCel:SetText(Licznik .. Tlumaczenie)
                PodrzednyCel:SetFont(FontTresci, 14)
                PodrzednyCel:SetTextColor(0, 0, 0)
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
        QuestInfoObjectivesHeader:SetText("Cele misji")
        QuestInfoObjectivesHeader:SetFont(FontTytulu, 18)
        QuestInfoObjectivesHeader:SetTextColor(0, 0, 0)
        QuestInfoObjectivesHeader:Show() -- WYMUSZENIE POKAZANIA
    end

    -- 3. NAGRODY (Nagłówek)
    if QuestInfoRewardsFrame then
        if QuestInfoRewardsFrame.Header then
            QuestInfoRewardsFrame.Header:SetText("Nagrody")
            QuestInfoRewardsFrame.Header:SetFont(FontTytulu, 18)
            QuestInfoRewardsFrame.Header:SetTextColor(0, 0, 0)
            QuestInfoRewardsFrame.Header:Show() -- WYMUSZENIE POKAZANIA
        end
        
        if QuestInfoRewardsFrame.ItemReceiveText then
            QuestInfoRewardsFrame.ItemReceiveText:SetText("Otrzymasz:")
            QuestInfoRewardsFrame.ItemReceiveText:SetFont(FontTresci, 14)
            QuestInfoRewardsFrame.ItemReceiveText:SetTextColor(0, 0, 0)
            QuestInfoRewardsFrame.ItemReceiveText:Show() -- WYMUSZENIE POKAZANIA
        end
    end

    -- 4. DOŚWIADCZENIE
    if QuestInfoXPFrame and QuestInfoXPFrame.ReceiveText then
        QuestInfoXPFrame.ReceiveText:SetText("Doświadczenie:")
        QuestInfoXPFrame.ReceiveText:SetFont(FontTresci, 14)
        QuestInfoXPFrame.ReceiveText:SetTextColor(0, 0, 0)
        QuestInfoXPFrame.ReceiveText:Show() -- WYMUSZENIE POKAZANIA
    end

    -- === TREŚCI WŁAŚCIWE ===
    local OpisOryginal = GetQuestText()

    if (not OpisOryginal or OpisOryginal == "") and QuestInfoDescriptionText then
        OpisOryginal = QuestInfoDescriptionText:GetText()
    end -- tlumaczy po nacisnieciu M

    if OpisOryginal then
        local OpisPL = PrzetlumaczTekst(OpisOryginal)
        if OpisPL and QuestInfoDescriptionText then 
            QuestInfoDescriptionText:SetText(OpisPL) 
            QuestInfoDescriptionText:SetFont(FontTresci, 14)
            QuestInfoDescriptionText:SetTextColor(0, 0, 0)
        end
    end -- tlumaczy po nacisnieciu M

    if QuestInfoDescriptionHeader then
        QuestInfoDescriptionHeader:SetText("Opis")
        QuestInfoDescriptionHeader:SetFont(FontTytulu, 18)
        QuestInfoDescriptionHeader:SetTextColor(0, 0, 0)
        QuestInfoDescriptionHeader:Show()
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
             QuestInfoObjectivesText:SetText(CelPL) 
             QuestInfoObjectivesText:SetFont(FontTresci, 14)
             QuestInfoObjectivesText:SetTextColor(0, 0, 0)
        end
    end

    local PostepOryginal = GetProgressText()
    if PostepOryginal and QuestProgressText and QuestProgressText:IsVisible() then
        local PostepPL = PrzetlumaczTekst(PostepOryginal)
        if PostepPL then
            QuestProgressText:SetText(PostepPL)
            QuestProgressText:SetFont(FontTresci, 14)
            QuestProgressText:SetTextColor(0, 0, 0)
        end
    end

    if QuestProgressTitleText and QuestProgressTitleText:IsVisible() then
        local Tytul = GetTitleText()
        local TytulPL = PrzetlumaczTekst(Tytul)
        
        if TytulPL then
            QuestProgressTitleText:SetText(TytulPL)
            QuestProgressTitleText:SetFont(FontTytulu, 18)
            QuestProgressTitleText:SetTextColor(0, 0, 0)
        end
    end

    local ZakonczenieOryginal = GetRewardText()
    if ZakonczenieOryginal and QuestInfoRewardText and QuestInfoRewardText:IsVisible() then
        local ZakonczeniePL = PrzetlumaczTekst(ZakonczenieOryginal)
        if ZakonczeniePL then
            QuestInfoRewardText:SetText(ZakonczeniePL)
            QuestInfoRewardText:SetFont(FontTresci, 14)
            QuestInfoRewardText:SetTextColor(0, 0, 0)
        end
    end

    local GossipDialog = C_GossipInfo.GetText()
    if GossipDialog and GossipFrame and GossipFrame:IsVisible() then
        local GossipDialogPL = PrzetlumaczTekst(GossipDialog)    

        if GossipGreetingText and GossipGreetingText:IsShown() then
            GossipGreetingText:SetText(GossipDialogPL)
            GossipGreetingText:SetFont(FontTresci, 14)
            GossipGreetingText:SetTextColor(0, 0, 0)
        end

        if GossipFrame.GreetingPanel and GossipFrame.GreetingPanel.ScrollBox then
            local RamkiWLiscie = GossipFrame.GreetingPanel.ScrollBox:GetFrames()

            for _, PojedynczaRamka in ipairs(RamkiWLiscie) do
                if PojedynczaRamka.GreetingText and PojedynczaRamka.GreetingText:GetText() then
                    local ObecnyTekst = PojedynczaRamka.GreetingText:GetText()
                    local TekstPL = PrzetlumaczTekst(ObecnyTekst)
                    
                    if TekstPL then
                        PojedynczaRamka.GreetingText:SetText(TekstPL)
                        PojedynczaRamka.GreetingText:SetFont(FontTresci, 14)
                        PojedynczaRamka.GreetingText:SetTextColor(0, 0, 0)
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
local function TlumaczTrackerKampanii(self)
    local GlownaRamka = self["ContentsFrame"]
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
            -- to tlumaczy zadania celow po prawej stronie
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
    hooksecurefunc(CampaignQuestObjectiveTracker, "Update", TlumaczTrackerKampanii)
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

ramka:SetScript("OnEvent", GlownyHandler)