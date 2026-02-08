local nazwaAddonu, prywatna_tabela = ...

prywatna_tabela["ZbierajMisje"] = function (self, event)
    local MisjaID = GetQuestID()
    
    if not MisjaID or MisjaID == 0 then
        return
    end

    local BazaMisji = KronikiDB_Nieprzetlumaczone["ListaMisji"]
    local misja_w_bazie = BazaMisji[MisjaID]

    -- check czy istnieje misja
    if not misja_w_bazie then
        misja_w_bazie = {
            ["ID"] = MisjaID
        }
        BazaMisji[MisjaID] = misja_w_bazie
    end

    -- zapis
    misja_w_bazie["MISJA_TYTUL_EN"] = GetTitleText()

    if event == "QUEST_DETAIL" then
        misja_w_bazie["CEL"] = GetObjectiveText()
        misja_w_bazie["TREŚĆ"] = GetQuestText()
    
    elseif event == "QUEST_PROGRESS" then
        misja_w_bazie["POSTĘP"] = GetProgressText()

    elseif event == "QUEST_COMPLETE" then
        misja_w_bazie["ZAKOŃCZENIE"] = GetRewardText()
    end

    print("Zaktualizowano quest ID:", MisjaID)
end