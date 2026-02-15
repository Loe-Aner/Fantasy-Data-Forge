local nazwaAddonu, prywatna_tabela = ...

local ipairs = ipairs
local table = table

local NormalizujTekst = prywatna_tabela["NormalizujTekst"]  -- funkcja z zbieracz.lua
local PodzielTekst    = prywatna_tabela["PodzielTekst"]     -- funkcja z zbieracz.lua
local GenerujHash     = prywatna_tabela["GenerujHash"]      -- funkcja z narzedzia.lua

local KronikiDB_Przetlumaczone_0001 = KronikiDB_Przetlumaczone_0001

local function PrzetlumaczTekst(Tekst)
    if not Tekst or Tekst == "" then return "" end

    local TekstPodzielony = PodzielTekst(Tekst, "\r\n\r\n") -- jezeli nie ma separatora, przetlumaczy normalnie calosc (dobre dla gossipow i wyborow)
    local Akumulator = {}
    
    for _, OryginalnyTekst in ipairs(TekstPodzielony) do
        local TekstZnormalizowany = NormalizujTekst(OryginalnyTekst)
        local HashTekstu = GenerujHash(TekstZnormalizowany)

        local PrzetlumaczonyTekst = KronikiDB_Przetlumaczone_0001[HashTekstu]
      
        if HashTekstu and PrzetlumaczonyTekst then
            table.insert(Akumulator, PrzetlumaczonyTekst)   -- jezeli przetlumaczone, bierz ten tekst
        else
            table.insert(Akumulator, OryginalnyTekst)       -- a jezeli nie, to zwroc oryginal nieznormalizowany
        end
    end
    return table.concat(Akumulator, "\r\n\r\n")
end
prywatna_tabela["PrzetlumaczTekst"] = PrzetlumaczTekst