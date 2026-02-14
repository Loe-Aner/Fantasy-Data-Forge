local nazwaAddonu, prywatna_tabela = ...

local CreateFrame = CreateFrame
local GetTitleText = GetTitleText
local GetQuestText = GetQuestText
local GetObjectiveText = GetObjectiveText
local GetProgressText = GetProgressText
local GetRewardText = GetRewardText

local QuestInfoTitleHeader = QuestInfoTitleHeader
local QuestInfoObjectivesText = QuestInfoObjectivesText
local QuestInfoDescriptionText = QuestInfoDescriptionText
local QuestInfoRewardText = QuestInfoRewardText
local QuestProgressText = QuestProgressText

local InicjujDB = prywatna_tabela["InicjujDB"]
local ZbierajMisje = prywatna_tabela["ZbierajMisje"]
local PrzetlumaczTekst = prywatna_tabela["PrzetlumaczTekst"]

local ramka = CreateFrame("Frame")      -- nadsluchuje eventow, nie widac jej nigdzie
ramka:RegisterEvent("ADDON_LOADED")     -- konkretny event; przekazuje obiekt po lewej stronie jako 1szy argument funkcji
                                        -- dłużej: f["RegisterEvent"](f, "ADDON_LOADED")

ramka:RegisterEvent("QUEST_DETAIL")     -- ramki pod questy
ramka:RegisterEvent("QUEST_PROGRESS")
ramka:RegisterEvent("QUEST_COMPLETE")

local function PodmienTekstOknienko(event)
    -- === KONFIGURACJA CZCIONKI ===
    local FontTytulu = "Interface\\AddOns\\addon_KronikiWarcrafta\\Media\\MorpheusPL.ttf"
    local FontTresci = "Interface\\AddOns\\addon_KronikiWarcrafta\\Media\\FrizQuadrataStd.ttf"
    
    -- 1. Tytul
    if QuestInfoTitleHeader then
        local TytulOryginal = GetTitleText()
        local TytulPL = PrzetlumaczTekst(TytulOryginal)
        if TytulPL and TytulPL ~= "" then
            QuestInfoTitleHeader:SetText(TytulPL)
            QuestInfoTitleHeader:SetFont(FontTytulu, 18)
            QuestInfoTitleHeader:SetTextColor(0, 0, 0)
        end
    end

    -- 2. Quest Detail
    if event == "QUEST_DETAIL" then
        local CelOryginal = GetObjectiveText()
        local OpisOryginal = GetQuestText()

        local CelPL = PrzetlumaczTekst(CelOryginal)
        local OpisPL = PrzetlumaczTekst(OpisOryginal)

        if QuestInfoObjectivesText and CelPL and CelPL ~= "" then
            QuestInfoObjectivesText:SetText(CelPL)
            QuestInfoObjectivesText:SetFont(FontTresci, 14)
            QuestInfoObjectivesText:SetTextColor(0, 0, 0)
        end
        if QuestInfoDescriptionText and OpisPL and OpisPL ~= "" then
            QuestInfoDescriptionText:SetText(OpisPL)
            QuestInfoDescriptionText:SetFont(FontTresci, 14)
            QuestInfoDescriptionText:SetTextColor(0, 0, 0)
        end

    -- 3. Quest Progress
    elseif event == "QUEST_PROGRESS" then
        local PostepOryginal = GetProgressText()
        local PostepPL = PrzetlumaczTekst(PostepOryginal)

        if QuestProgressText and PostepPL and PostepPL ~= "" then
            QuestProgressText:SetText(PostepPL)
            QuestProgressText:SetFont(FontTresci, 14)
            QuestProgressText:SetTextColor(0, 0, 0)
        end

    -- 4. Quest Complete
    elseif event == "QUEST_COMPLETE" then
        local ZakonczenieOryginal = GetRewardText()
        local ZakonczeniePL = PrzetlumaczTekst(ZakonczenieOryginal)

        if QuestInfoRewardText and ZakonczeniePL and ZakonczeniePL ~= "" then
            QuestInfoRewardText:SetText(ZakonczeniePL)
            QuestInfoRewardText:SetFont(FontTresci, 14)
            QuestInfoRewardText:SetTextColor(0, 0, 0)
        end
    end
end

local function GlownyHandler(self, event, ...)
    if event == "ADDON_LOADED" then
        InicjujDB(self, event, ...)

    elseif event == "QUEST_DETAIL" or event == "QUEST_PROGRESS" or event == "QUEST_COMPLETE" then
        ZbierajMisje(self, event, ...)  -- zbiera dane do bazy (nieprzetlumaczone)
        PodmienTekstOknienko(event)     -- podmienia tekst na ekranie
    end
end

ramka:SetScript("OnEvent", GlownyHandler)