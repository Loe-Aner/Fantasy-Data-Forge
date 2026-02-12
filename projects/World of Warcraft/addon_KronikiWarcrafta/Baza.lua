local nazwaAddonu, prywatna_tabela = ...

local CreateFrame = CreateFrame

local ramka = CreateFrame("Frame")    -- nadsluchuje eventow, nie widac jej nigdzie
ramka:RegisterEvent("ADDON_LOADED")   -- konkretny event; przekazuje obiekt po lewej stronie jako 1szy argument funkcji
                                      -- dłużej: f["RegisterEvent"](f, "ADDON_LOADED")

ramka:RegisterEvent("QUEST_DETAIL")   -- ramki pod questy
ramka:RegisterEvent("QUEST_PROGRESS")
ramka:RegisterEvent("QUEST_COMPLETE")

local function GlownyHandler(self, event, ...) -- trzy kropki to odpowiednik *args w pythonie
    if event == "ADDON_LOADED" then
        prywatna_tabela.InicjujDB(self, event, ...) -- trzy kropki, by dostac nazwe mojego addonu w tym przypadku

    elseif event == "QUEST_DETAIL" or event == "QUEST_PROGRESS" or event == "QUEST_COMPLETE" then
        prywatna_tabela.ZbierajMisje(self, event, ...) -- trzy kropki 'dla porzadku'
    end
end

ramka:SetScript("OnEvent", GlownyHandler)