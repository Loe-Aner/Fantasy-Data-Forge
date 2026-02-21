local nazwaAddonu, prywatna_tabela = ...

local bit = bit
local string = string
local QuestMapFrame = QuestMapFrame

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

local function SprawdzTloMisji()
    local RamkaTla = QuestMapFrame.QuestsFrame.DetailsFrame.SealMaterialBG
    
    if RamkaTla then
       local KolorR, KolorG, KolorB = nil, nil, nil
       
       if RamkaTla.GetVertexColor then
          KolorR, KolorG, KolorB = RamkaTla:GetVertexColor()
       end
       
       return KolorR, KolorG, KolorB
    end
    return nil
 end

 prywatna_tabela["DostosujKolorkiFont"] = function(ElementUI, PrzetlumaczonyTekst, Font, RozmiarFontu, KolorR, KolorG, KolorB, WymusPokazanie)
    ElementUI:SetText(PrzetlumaczonyTekst)
    ElementUI:SetFont(Font, RozmiarFontu)

    if KolorR == nil then
        KolorR, KolorG, KolorB = SprawdzTloMisji()
    end

    if KolorR == 0 and KolorG == 0 and KolorB == 0 then
        KolorR, KolorG, KolorB = 0.85, 0.77, 0.60
    end

    if KolorR then
        ElementUI:SetTextColor(KolorR, KolorG, KolorB)
    end

    if WymusPokazanie then
        ElementUI:Show()
    end
end