local nazwaAddonu, prywatna_tabela = ...

-- === 1. DANE GRACZA ===
local ImieGracza = UnitName("player")
-- UnitRace zwraca: nazwa lokalna, nazwa angielska. Biore te druga (angielska) dla bezpieczenstwa
local _, RasaGracza = UnitRace("player") 
local RasaGraczaMala = string.lower(RasaGracza or "")

-- UnitClass zwraca: nazwa lokalna, TAG (angielski). Biore TAG.
local _, KlasaGracza = UnitClass("player") 
-- Klasa (TAG) jest zawsze z duzych (np. WARRIOR), wiec robie formatowanie:
-- Zamieniam np. WARRIOR na Warrior (zeby pasowalo do tekstow w questach)
KlasaGracza = string.upper(string.sub(KlasaGracza, 1, 1)) .. string.lower(string.sub(KlasaGracza, 2))
local KlasaGraczaMala = string.lower(KlasaGracza)


-- === 2. FUNKCJA NORMALIZUJĄCA ===
local function NormalizujTekst(Tekst)
    if not Tekst or Tekst == "" then return "" end

    Tekst = string.match(Tekst, "^%s*(.-)%s*$") -- usuwam niepotrzebne spacje z surowego tekstu

    -- 1. Imie (np. Loe'Aner -> {imie})
    Tekst = string.gsub(Tekst, ImieGracza, "{imie}")

    -- 2. Rasa (np. Human -> {rasa})
    if RasaGracza then
        Tekst = string.gsub(Tekst, RasaGracza, "{rasa}")
        Tekst = string.gsub(Tekst, RasaGraczaMala, "{rasa}")
    end

    -- 3. Klasa (np. Warrior -> {klasa})
    if KlasaGracza then
        Tekst = string.gsub(Tekst, KlasaGracza, "{klasa}")
        Tekst = string.gsub(Tekst, KlasaGraczaMala, "{klasa}")
    end

    return Tekst
end

local function PodzielTekst(TekstOryginalny, sep)
   if sep == nil or TekstOryginalny == nil then
      return
   end
   
   local TekstPodzielony = {}
   local Wzorzec = "([^" .. sep .. "]+)"
   
   for str in string.gmatch(TekstOryginalny, Wzorzec) do
      table.insert(TekstPodzielony, str)
   end
   return TekstPodzielony
end

-- === 3. ZAPIS DO BAZY ===
local function ZapiszPojedynczyTekst(TypTekstu, TekstOryginalny, MisjaID)
   if not TekstOryginalny or TekstOryginalny == "" then 
      return 
   end

   local TekstPodzielony = PodzielTekst(TekstOryginalny, "\r\n\r\n")
   local BazaBrakujacych = KronikiDB_Nieprzetlumaczone["ListaMisji"]

   for _, PojedynczaLinia in ipairs(TekstPodzielony) do
      local TekstZnormalizowany = NormalizujTekst(PojedynczaLinia)
      local HashTekstu = prywatna_tabela.GenerujHash(TekstZnormalizowany)
      
      if HashTekstu then 
         if not BazaBrakujacych[HashTekstu] then
            BazaBrakujacych[HashTekstu] = {
               ["MISJA_ID"] = MisjaID,
               ["TYP"] = TypTekstu,
               ["TEKST_ENG"] = TekstZnormalizowany, 
               ["TEKST_RAW"] = PojedynczaLinia, 
               ["HASH"] = HashTekstu
            }
            print("|cff00ccff[Kroniki]|r Nowy znormalizowany wpis: " .. HashTekstu)
         end
      end
   end
end


-- === 4. GLOWNA FUNKCJA ZBIERACZA ===
prywatna_tabela["ZbierajMisje"] = function (self, event)
    local MisjaID = GetQuestID()

    local TytulMisji = GetTitleText()
    ZapiszPojedynczyTekst("TYTUŁ", TytulMisji, MisjaID)

    if event == "QUEST_DETAIL" then
        local TrescMisji = GetQuestText()
        ZapiszPojedynczyTekst("TREŚĆ", TrescMisji, MisjaID)

        local CelMisji = GetObjectiveText()
        ZapiszPojedynczyTekst("CEL", CelMisji, MisjaID)

    elseif event == "QUEST_PROGRESS" then
        local PostepMisji = GetProgressText()
        ZapiszPojedynczyTekst("POSTĘP", PostepMisji, MisjaID)

    elseif event == "QUEST_COMPLETE" then
        local ZakonczenieMisji = GetRewardText()
        ZapiszPojedynczyTekst("ZAKOŃCZENIE", ZakonczenieMisji, MisjaID)
    end
end