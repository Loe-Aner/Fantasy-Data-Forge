local nazwaAddonu, prywatna_tabela = ...

local AKTUALNA_WERSJA_DB = prywatna_tabela["AKTUALNA_WERSJA_DB"]

prywatna_tabela["InicjujDB"] = function(self, event, NazwaZaladowanegoAddonu)
    if NazwaZaladowanegoAddonu ~= nazwaAddonu then
        return -- dzieki temu ponizsze wykona sie tylko dla mojego addona
    end

    print("|cff00ff00Kroniki Warcrafta:|r Załadowano pomyślnie!")

    -- dla nowych graczy
    if KronikiDB_Nieprzetlumaczone == nil then
        KronikiDB_Nieprzetlumaczone = {
            ["Wersja"] = AKTUALNA_WERSJA_DB,
            ["ListaMisji"] = {}
        }
        print("|cff00ff00Kroniki Warcrafta:|r Utworzono nową bazę danych.")
        self:UnregisterEvent("ADDON_LOADED")
        return
    end

    -- dla graczy, ktorzy aktualizuja addon
    if KronikiDB_Nieprzetlumaczone["Wersja"] == nil or KronikiDB_Nieprzetlumaczone["Wersja"] < AKTUALNA_WERSJA_DB then
        print("|cff00ff00Kroniki Warcrafta:|r Wykryto starą bazę (" .. (KronikiDB_Nieprzetlumaczone["Wersja"] or "brak") .. "). Aktualizacja do v" .. AKTUALNA_WERSJA_DB .. "...")
        
        KronikiDB_Nieprzetlumaczone["Wersja"] = AKTUALNA_WERSJA_DB
        print("|cff00ff00Kroniki Warcrafta:|r Aktualizacja zakończona.")
    end

    self:UnregisterEvent("ADDON_LOADED")
end