local nazwaAddonu, prywatna_tabela = ...

local ramka = CreateFrame("Frame")  -- nadsluchuje eventow, nie widac jej nigdzie
ramka:RegisterEvent("ADDON_LOADED") -- konkretny event; przekazuje obiekt po lewej stronie jako 1szy argument funkcji
                                    -- dłużej: f["RegisterEvent"](f, "ADDON_LOADED")

ramka:SetScript("OnEvent", prywatna_tabela["InicjujDB"])