local nazwaAddonu, prywatna_tabela = ...

-- funkcja zamieniajaca tekst na unikalny numer (jako string)
prywatna_tabela["GenerujHash"] = function(tekst)
    if not tekst or tekst == "" then
        return nil
    end

    local hash = 5381
    local dlugosc = #tekst

    for i = 1, dlugosc do
        local znak_kod = string.byte(tekst, i)            -- pobranie kodu ASCI znaku
        hash = bit.band(hash * 33 + znak_kod, 0xFFFFFFFF) 
        -- aby liczba nie urosla w nieskonczonosc / zawsze miesci sie w 32 bitach
    end

    return string.format("%x", hash)
end