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

-- Globalne Ramki UI
local QuestInfoTitleHeader = QuestInfoTitleHeader
local QuestInfoObjectivesHeader = QuestInfoObjectivesHeader
local QuestInfoObjectivesText = QuestInfoObjectivesText
local QuestInfoDescriptionText = QuestInfoDescriptionText
local QuestInfoRewardsFrame = QuestInfoRewardsFrame
local QuestInfoRewardText = QuestInfoRewardText
local QuestProgressText = QuestProgressText
local GetProgressTitleText = GetProgressTitleText
local QuestProgressTitleText = QuestProgressTitleText
local QuestInfoXPFrame = QuestInfoXPFrame
local ObjectiveTrackerFrame = ObjectiveTrackerFrame
local CampaignQuestObjectiveTracker = CampaignQuestObjectiveTracker
local C_GossipInfo = C_GossipInfo
local GossipGreetingText = GossipGreetingText
local GossipFrame = GossipFrame

-- Funkcje z innych plikow
local InicjujDB = prywatna_tabela["InicjujDB"]
local ZbierajMisje = prywatna_tabela["ZbierajMisje"]
local ZbierajGossipy = prywatna_tabela["ZbierajGossipy"]
local ZbierajDymki = prywatna_tabela["ZbierajDymki"]
local PrzetlumaczTekst = prywatna_tabela["PrzetlumaczTekst"]
local TlumaczDymki = prywatna_tabela["TlumaczDymki"]
local TlumaczDymkiCzat = prywatna_tabela["TlumaczDymkiCzat"]

local ramka = CreateFrame("Frame")
ramka:RegisterEvent("ADDON_LOADED")
ramka:RegisterEvent("QUEST_DETAIL")
ramka:RegisterEvent("QUEST_PROGRESS")
ramka:RegisterEvent("QUEST_COMPLETE")
ramka:RegisterEvent("GOSSIP_SHOW")
ramka:RegisterEvent("CHAT_MSG_MONSTER_SAY")      -- Mówienie
ramka:RegisterEvent("CHAT_MSG_MONSTER_YELL")     -- Krzyczenie
ramka:RegisterEvent("CHAT_MSG_MONSTER_WHISPER")  -- Szeptanie
ramka:RegisterEvent("CHAT_MSG_MONSTER_EMOTE")    -- Emotki

-- === FUNKCJA MODYFIKUJĄCA UI ===
local function PodmienTekstOknienko()
    local FontTytulu = "Interface\\AddOns\\addon_KronikiWarcrafta\\Media\\MorpheusPL.ttf"     -- ========= TO PUSCIC NA ZMIENNA GLOBALNA =========
    local FontTresci = "Interface\\AddOns\\addon_KronikiWarcrafta\\Media\\FrizQuadrataPL.ttf" -- ========= TO PUSCIC NA ZMIENNA GLOBALNA =========

-- 1. TYTUŁ
    local TytulDoTlumaczenia = nil

    if QuestMapFrame and QuestMapFrame:IsVisible() then
        local questID = QuestMapFrame_GetDetailQuestID() 
        
        if questID and questID > 0 then
             TytulDoTlumaczenia = C_QuestLog.GetTitleForQuestID(questID)
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

    if (not OpisOryginal or OpisOryginal == "") and QuestInfoDescriptionText:IsVisible() then
        OpisOryginal = QuestInfoDescriptionText:GetText()
    end -- tlumaczy po nacisnieciu M

    if OpisOryginal and QuestInfoDescriptionText:IsVisible() then
        local OpisPL = PrzetlumaczTekst(OpisOryginal)
        if OpisPL then 
            QuestInfoDescriptionText:SetText(OpisPL) 
            QuestInfoDescriptionText:SetFont(FontTresci, 14)
            QuestInfoDescriptionText:SetTextColor(0, 0, 0)
        end
    end

    local CelOryginal = GetObjectiveText()

    if (not CelOryginal or CelOryginal == "") and QuestInfoObjectivesText:IsVisible() then
        CelOryginal = QuestInfoObjectivesText:GetText()
    end -- tlumaczy po nacisnieciu M

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

hooksecurefunc("QuestMapFrame_ShowQuestDetails", function(questID)
    PodmienTekstOknienko()
end) -- aby pokazaly sie dane dla misji po kliknieciu 'M'

ramka:SetScript("OnEvent", GlownyHandler)