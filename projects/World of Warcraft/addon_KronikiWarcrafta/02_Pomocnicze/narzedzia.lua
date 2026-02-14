local nazwaAddonu, prywatna_tabela = ...

prywatna_tabela["GenerujHash"] = function(tekst)
    if not tekst or tekst == "" then
        return nil
    end

    local hash1 = 5381
    local hash2 = 0
    local dlugosc = #tekst

    for i = 1, dlugosc do
        local znak_kod = string.byte(tekst, i)
        
        hash1 = bit.band(hash1 * 33 + znak_kod, 0xFFFFFFFF)
        hash2 = bit.band(hash2 * 65599 + znak_kod, 0xFFFFFFFF)
    end

    return string.format("%08x%08x", hash1, hash2)
end