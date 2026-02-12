local nazwaAddonu, prywatna_tabela = ...

local CreateFrame = CreateFrame

local AKTUALNA_WERSJA_DB = prywatna_tabela["AKTUALNA_WERSJA_DB"]

prywatna_tabela["InicjujDB"] = function(self, event, NazwaZaladowanegoAddonu)
    if NazwaZaladowanegoAddonu ~= nazwaAddonu then
        return -- dzieki temu ponizsze wykona sie tylko dla mojego addona
    end

    print("|cff00ff00Kroniki Warcrafta:|r Załadowano pomyślnie!")
    self:UnregisterEvent("ADDON_LOADED")
    
    -- dla nowych graczy
    if KronikiDB_Nieprzetlumaczone == nil then
        KronikiDB_Nieprzetlumaczone = {
            ["Wersja"] = AKTUALNA_WERSJA_DB,
            ["ListaMisji"] = {}
        }
        print("|cff00ff00Kroniki Warcrafta:|r Utworzono nową bazę danych na nieprzetłumaczone rekordy.")
        return
    end

    -- dla graczy, ktorzy aktualizuja addon
    if KronikiDB_Nieprzetlumaczone["Wersja"] == nil or KronikiDB_Nieprzetlumaczone["Wersja"] < AKTUALNA_WERSJA_DB then
        local stara_wersja = KronikiDB_Nieprzetlumaczone["Wersja"] or "brak"
        print("|cff00ff00Kroniki:|r Aktualizacja DB: " .. stara_wersja .. " -> " .. AKTUALNA_WERSJA_DB)
        
        KronikiDB_Nieprzetlumaczone["Wersja"] = AKTUALNA_WERSJA_DB
        print("|cff00ff00Kroniki Warcrafta:|r Aktualizacja zakończona.")
    end
end