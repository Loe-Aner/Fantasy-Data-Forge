-- w lua 5.1 zmienne globalne są przechowywane w specjalnej tabeli '_G'
--[[ aby odczytac zmienna globalna, lua musi:
        a. pobrać nazwe,
        b. obliczyć hash,
        c. znalezc miejsce w tabeli,
        d. pobrac wartosc
--]]

-- ZAWSZE uzywac local, chyba ze celowo chce cos udostepnic np. jakiemus addonowi
local ss = "aaaa"
print(ss)

-- lua jest typowana dynamicznie
-- zmienna nie ma typu, wartosc ja ma
-- jezeli sprobuje pobrac tlumaczenie ktorego nie ma, dostane nil (brak). Nil usuwa tez zmienna z pamieci

-- do laczenia stringow uzywa sie ' .. '
-- kazde laczenie typ skrotem tworzy nowy string w pamieci, leczenie wielu stringow w petli jest zabojcze dla wydajnosci (pomyslec nad table.concat)


-- KOD DO POPRAWY:
--[[
PlayerName = "Arthas"
PlayerClass = "Death Knight"
Level = 80
--]]

local PlayerName = "Arthas"
local PlayerClass = "Death Knight"
local Level = 80
-- Chce wypisać: "Gracz Arthas to Death Knight (Poziom: 80)"
local Message = "Gracz " .. PlayerName .. " to " .. PlayerClass .. " (Poziom: " .. Level .. ")"
print(Message)

local currentGoldText = "150" -- To jest string!
local questReward = 50        -- To jest liczba
local sumowanie = tonumber(currentGoldText) + questReward

if sumowanie > 100 then
    print("Bogacz!")
else
    print("Biedak!")
end

-- jest zdanie: Arthas hits you for 50 damage
-- w kodzie wyglada tak: %s hits you for %d damage
-- podczas tlumaczenia szyk zdania moze sie zmienic
-- Otrzymano %d obrażeń od %s

-- gdybym stosowal konkatenacje, musialbym zmienic kod programu
local monsterName = "Hogger"
local damageTaken = 150
-- %d to dziura na liczbę (digit)
-- %s to dziura na tekst (string)
local template = "Otrzymano %d obrażeń od %s."
-- string.format wstawia w kolejnosci wystepowania argumentow
local ft = string.format(template, damageTaken, monsterName)
print(ft)

local imie = "Thrall"
local leczenie = 150

local en = string.format("Healed %s for %d HP.", imie, leczenie)
-- Wynik: Healed Thrall for 150 HP.

-- Wynik po pl: Uleczono 150 HP gracza Thrall
-- to zadziala ale tylko w WoWie, w czystym lua nie
local pl = string.format("Uleczono %2$d HP gracza %1$s.", imie, leczenie)
print(en, pl)

local translation = {
    ["Hello"] = "Witaj",
    ["Quest Log"] = "Dziennik zadań"
}
print(translation["Hello"])

-- tabela jako lista
local bosses = {"a", "b", "c"}
print(bosses[1]) -- 'a'

local AddonDB = {
    ["settings"] = {
        ["language"] = "plPL",
        ["fontSize"] = 14
    },
    ["zones"] = {
        "Elwynn Forest",
        "Durotar"
    }
}
print(AddonDB["settings"]["language"])

--[[
Ciekawostka: Języki takie jak C czy Python liczą "przesunięcie w pamięci" (offset). 0 oznacza "start tutaj". Lua została stworzona w Brazylii dla inżynierów z branży paliwowej (Petrobras), nie dla informatyków. A normalni ludzie liczą od 1, nie od 0. Dlatego Lua jest taka "ludzka".
]]

local Locales = {
    ["Attack"] = "Atakuj",
    ["Defense"] = "Obrona"
}
print(Locales["Attack"])
print(Locales["Defense"])

local ItemDB = {
    ["Przedmiot"] = {
        ["Thunderfury"] = {
            ["Typ"] = "Weapon",
            ["Statystyki"] = {
                ["MinDmg"] = 200,
                ["MaxDmg"] = 300
            }
        }
    }
}
local w = ItemDB["Przedmiot"]["Thunderfury"]["Statystyki"]["MaxDmg"]
print(string.format("Max damage is %d.", w))

local Config = {
    ["sound"] = true,
    ["music"] = false,
    ["debug_mode"] = 'sss'
}

Config["debug_mode"] = nil
print(Config["debug_mode"])

-- chyba stary sposob (deprecated)
table.foreach(Config, print)

--[[
Iterowanie po tabelach
a. ipairs = sluzy tylko do list, iteruje po kolei: 1, 2, 3...
            zatrzymuje sie, gdy trafi na nil - jak jest 1, 2, nil, 4, wyswietli tylko 1 i 2
b. pairs  = sluzy do slownikow. Iteruje po wszystkich kluczach, kolejnosc jest losowa.
--]]

local dict = { ["Hi"] = "Cześć", ["Bye"] = "Papa" }
for k, v in pairs(dict) do
    print(k, v)
end

local idict = {"a", "b", "c"}
for i, a in ipairs(idict) do
    print(i, a)
end


local function Tlumacz(tekst)
    local slownik = {["Shield"] = "Tarcza", ["Sword"] = "Miecz"}
    if slownik[tekst] ~= nil then
        return slownik[tekst]
    else
        return tekst
    end
end

print(Tlumacz("Sho"))

local party = {
    [1] = "Tank",
    [2] = nil,
    [3] = "Healer",
    [4] = "DPS"
}
for k, v in pairs(party) do
    print(v)
end

local slownik = {["Shield"] = "Tarcza", ["Sword"] = "Miecz"}
local sentence = {"I", "have", "a", "Sword", "and", "Shield"}
for _, slowo in pairs(sentence) do
    local tlum = slownik[slowo]
    if tlum then
        print(tlum)
    else
        print(slowo)
    end
end

local ActionBar = {"Fireball", "Blink", "Frostbolt", "Polymorph"}
local PolskiSlownik = {
    ["Fireball"] = "Kula Ognia",
    ["Frostbolt"] = "Pocisk Lodu"
}

for _, slowo in pairs(ActionBar) do
    local a = PolskiSlownik[slowo] or slowo
    print("Rzucam: ", a)
end


-- wzorce (uproszoczne regex)
--[[
Zamiast szukać konkretnego słowa "100", szukamy "jakiejkolwiek liczby". W Lua używamy do tego procenta % i litery:
  - %d (digit) – dowolna cyfra (0-9).
  - %a (letter) – dowolna litera (a-z, A-Z).
  - %s (space) – biały znak (spacja, tabulator).
  - %w (word) – litera lub cyfra (alphanumeric). W twoich notatkach przykład %w+ oznacza "jedno lub więcej słowo/liczbę".
  - . (kropka) – cokolwiek (każdy znak).

Wskazówka: Wielka litera oznacza zaprzeczenie. %D to wszystko, co NIE jest cyfrą.

Sama klasa %d znajdzie tylko jedną cyfrę (np. "1" z "100"). Żeby złapać całość, potrzebujemy "mnożników":
  - + (plus) – 1 lub więcej. Np. %d+ znajdzie "5", "50", "5000".
  - * (gwiazdka) – 0 lub więcej. Pasuje nawet, jak nic nie ma. W Twoich notatkach [%w_]* oznacza "ciąg znaków alfanumerycznych lub podkreślników, nawet jeśli jest pusty".
  - - (minus) – najmniej jak się da (tzw. leniwy). Bardzo ważne w WoW, żeby nie „zjeść” za dużo tekstu.
--]]

-- string.find(tekst, wzorzec) = mówi gdzie coś jest, zwraca indeks początkowy i końcowy
-- string.match(tekst, wzorzec) = zwraca to co znalazl

local hp = string.match("Health: 5000", "%d+")
print(hp) -- "5000"
-- string.gsub(tekst, wzorzec, zamiennik) = zamienia tekst
local nowy = string.gsub("Mam 5 złota i 2 miecze.", "%d+", "X")
print(nowy)