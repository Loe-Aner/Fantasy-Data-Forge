local nazwaAddonu, prywatna_tabela = ...

-- === 0. CACHE ===

-- Funkcje Podstawowe WoW API
local CreateFrame = CreateFrame
local hooksecurefunc = hooksecurefunc

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
local QuestInfoXPFrame = QuestInfoXPFrame

-- 5. Funkcje z innych plikow
local InicjujDB = prywatna_tabela["InicjujDB"]
local ZbierajMisje = prywatna_tabela["ZbierajMisje"]
local PrzetlumaczTekst = prywatna_tabela["PrzetlumaczTekst"]

local ramka = CreateFrame("Frame")
ramka:RegisterEvent("ADDON_LOADED")
ramka:RegisterEvent("QUEST_DETAIL")
ramka:RegisterEvent("QUEST_PROGRESS")
ramka:RegisterEvent("QUEST_COMPLETE")

-- === FUNKCJA MODYFIKUJĄCA UI ===
local function PodmienTekstOknienko()
    local FontTytulu = "Interface\\AddOns\\addon_KronikiWarcrafta\\Media\\MorpheusPL.ttf"
    local FontTresci = "Interface\\AddOns\\addon_KronikiWarcrafta\\Media\\FrizQuadrataPL.ttf"

    -- 1. TYTUŁ
    if QuestInfoTitleHeader then
        local TytulOryginal = GetTitleText()
        local TytulPL = PrzetlumaczTekst(TytulOryginal)
        
        if TytulPL and TytulPL ~= "" then
            QuestInfoTitleHeader:SetText(TytulPL)
            QuestInfoTitleHeader:SetFont(FontTytulu, 18)
            QuestInfoTitleHeader:SetTextColor(0, 0, 0)
            QuestInfoTitleHeader:Show() -- WYMUSZENIE POKAZANIA
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
    if OpisOryginal and QuestInfoDescriptionText:IsVisible() then
        local OpisPL = PrzetlumaczTekst(OpisOryginal)
        if OpisPL then 
            QuestInfoDescriptionText:SetText(OpisPL) 
            QuestInfoDescriptionText:SetFont(FontTresci, 14)
            QuestInfoDescriptionText:SetTextColor(0, 0, 0)
        end
        
        local CelOryginal = GetObjectiveText()
        local CelPL = PrzetlumaczTekst(CelOryginal)
        if CelPL and QuestInfoObjectivesText then
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

    local ZakonczenieOryginal = GetRewardText()
    if ZakonczenieOryginal and QuestInfoRewardText and QuestInfoRewardText:IsVisible() then
        local ZakonczeniePL = PrzetlumaczTekst(ZakonczenieOryginal)
        if ZakonczeniePL then
            QuestInfoRewardText:SetText(ZakonczeniePL)
            QuestInfoRewardText:SetFont(FontTresci, 14)
            QuestInfoRewardText:SetTextColor(0, 0, 0)
        end
    end
end

hooksecurefunc("QuestInfo_Display", function(template, parentFrame, acceptButton, material, mapView)
    PodmienTekstOknienko()
end)

-- === OBSLUGA EVENTOW (TYLKO DLA BAZY DANYCH I STARTU) ===
local function GlownyHandler(self, event, ...)
    if event == "ADDON_LOADED" then
        InicjujDB(self, event, ...)

    elseif event == "QUEST_DETAIL" or event == "QUEST_PROGRESS" or event == "QUEST_COMPLETE" then
        ZbierajMisje(self, event, ...)
    end
end

ramka:SetScript("OnEvent", GlownyHandler)