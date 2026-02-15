local nazwaAddonu, prywatna_tabela = ...

local ipairs = ipairs
local table = table

local NormalizujTekst = prywatna_tabela["NormalizujTekst"]  -- funkcja z zbieracz.lua
local PodzielTekst    = prywatna_tabela["PodzielTekst"]     -- funkcja z zbieracz.lua
local GenerujHash     = prywatna_tabela["GenerujHash"]      -- funkcja z narzedzia.lua

local function PrzetlumaczTekst(Tekst)
   if not Tekst or Tekst == "" then return "" end

   local TekstPodzielony = PodzielTekst(Tekst, "\r\n\r\n") -- jezeli nie ma separatora, przetlumaczy normalnie calosc (dobre dla gossipow i wyborow)
   local Akumulator = {}
   
   local Baza = KronikiDB_Przetlumaczone_0001 or {}

   for _, OryginalnyTekst in ipairs(TekstPodzielony) do
      local TekstZnormalizowany = NormalizujTekst(OryginalnyTekst)
      local HashTekstu = GenerujHash(TekstZnormalizowany)

      local PrzetlumaczonyTekst = Baza[HashTekstu]
      
      if HashTekstu and PrzetlumaczonyTekst then
         table.insert(Akumulator, PrzetlumaczonyTekst)   -- jezeli przetlumaczone, bierz ten tekst
      else
         table.insert(Akumulator, OryginalnyTekst)       -- a jezeli nie, to zwroc oryginal nieznormalizowany
      end
   end
   return table.concat(Akumulator, "\r\n\r\n")
end

-- Funkcja pomocnicza szukająca tekstu w głąb (w dzieciach ramki)
local function SkanujRamke(Obiekt)
   if not Obiekt or not Obiekt.GetRegions then return end

   local Regiony = {Obiekt:GetRegions()}
   
   for _, Region in ipairs(Regiony) do
      if Region:GetObjectType() == "FontString" then
         local TekstOryginalny = Region:GetText()
         
         if TekstOryginalny and TekstOryginalny ~= Region["OstatniTekst"] then 
            local TekstPL = PrzetlumaczTekst(TekstOryginalny)
            
            if TekstPL then
               Region:SetText(TekstPL)
               Region:SetFont("Interface\\AddOns\\addon_KronikiWarcrafta\\Media\\FrizQuadrataPL.ttf", 14)
               
               Region["OstatniTekst"] = TekstPL -- zapamietywany jest przetlumaczony, zeby nie mielic tego samego    
            else
               Region["OstatniTekst"] = TekstOryginalny
            end
         end
      end
   end

   local Dzieci = {Obiekt:GetChildren()}
   for _, Dziecko in ipairs(Dzieci) do
      SkanujRamke(Dziecko) -- rekurencja leci dalej
   end
end

local function TlumaczDymki()
   local CzyInstancja, RodzajInstancji = IsInInstance()
   
   if CzyInstancja and (RodzajInstancji == "party" or RodzajInstancji == "raid" or RodzajInstancji == "pvp" or RodzajInstancji == "arena") then
      return
   end

   local WszystkieDymki = C_ChatBubbles.GetAllChatBubbles()

   for _, Dymek in ipairs(WszystkieDymki) do
      if not Dymek:IsForbidden() then
         SkanujRamke(Dymek)
      end
   end
end

-- przypisanie do tabeli addonu na koncu
prywatna_tabela["PrzetlumaczTekst"] = PrzetlumaczTekst
prywatna_tabela["SkanujRamke"] = SkanujRamke
prywatna_tabela["TlumaczDymki"] = TlumaczDymki