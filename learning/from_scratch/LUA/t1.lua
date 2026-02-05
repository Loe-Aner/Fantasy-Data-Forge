-- https://github.com/Ketho/vscode-wow-api/wiki

-- print("Hello World!")

-- funkcja rekurencyjna liczaca silnie

-- definicja funkcji: "Tworzę nową funkcję o nazwie fact, która przyjmuje jeden argument o nazwie n"
function fact(n)
    if n == 0 then
        return 1
    else
        return n * fact(n-1)
    end
end

print("Wpisz liczbe: ...")
a = io.read("*number*")
-- io.read zatrzymuje program i czeka az user cos wpisze 
print(fact(a))

-- w lua zmienne sa domyslnie globalne, preferowane jest uzywanie local aby poprawic czytelnosc i wydajnosc


a=1 ; b=2
-- srednik oddziela instrukcje
print(a)

-- lua jest case sensitive

-- komentarze blokowe
--[[
1234
ssssss
--]]

-- aby ponownie aktywowac kod, mozna dodac kolejny myslnik w pierwszym bloku
---[[
print(10)
--]]

-- lua jest jezykiem typowanym dynamicznie
-- jest 8 typow: nil, bool, number, string, userdata, function, thread, table

-- stringi sa niemutowalne
local a = "xxxfaf"
local b = string.gsub(a, "faf", "xxx")
print(b)

-- lua ma rowniez garbage collector; zarzadza automatycznie pamiecia

-- podczas operacji arytmetycznych, lua probuje przekonwertowac sama string na int
print("10" + 22)

-- operator ' .. ' to znak konkatenacji
local pt = 10 .. 20
print(pt)
print(type(pt)) -- string

-- to wyzej, stosowac w ostatecznosci, bo moze wprowadzac chaos do kodu i troche nieintuicyjne jest

-- lepiej konwertowac string funkcja tonumber

-- ciag na liczbe
line = io.read() -- wczytaj cos
n = tonumber(line) -- sprobuj przekonwertowac
if n == nil then
    error(line .. " is not a valid number")
else
    print(n*2)
end

-- liczba na ciag
print(tostring(10) == "10") -- zwroci true

-- dlugosc ciagu znaku moge uzyskac stosujac prefiks #
local a = "safsafsa"
print(#a) --> zwroci 8

--[[ tablica = struktura, ktora moze byc indeksowana nie tylko liczbami, ale takze ciagami znakow lub czymkolwiek innym oprocz nil.
Mozna dynamicznie dodawac do tablicy elementy, nie maja okreslonej wielkosci.
Jest to glowna struktura danych w pythonie. Reprezentuje wszystko: zbiory, rekordy, itp.
--]]

a = {} -- tworzy tablice i zachowuje jej referencje w 'a'
a["x"] = 10 -- nowy wpis, klucz=x, a wartosc=10
a[20] = "git" -- nowy wpis, klucz=20 a wartosc="git"
print(a["x"])
print(a[20])
print(a) --> zwraca table: .....
-- gdy w programie nie zostanie zadna referencja do tablicy, pamiec zostanie automatycznie zwolniona (poprzez usuniecie obiektu)

a = {}
-- tworzenie 1000 wpisów
for i=1, 1000 do a[i] = i*2 end
print(a[9])

a = {}
for i=2, 100, 2 do
    a[i] = i
    print(a[i])
end

-- funkcje sa wartosciami pierwszej kategorii, co oznacza, ze mozna je przechowywac w zmiennych, przekazywac jako argumenty do innych funkcji

-- negacja rownosci: ~=
a = {}; b = {}
a["x"] = 1; a["y"] = 0
b["x"] = 1; b["y"] = 0
print(c == a)

-- lua porownuje ciagi znakow w porzadku alfabetycznym

-- w przypadku operatorow and, or i not wyglada to tak:
print(4 and 5) --> 5
print(4 and 5 and 10) --> 10
print(1 or 4) --> 1
-- w takim przypadku zwraca jedna z wartosci, ktora podam
-- tylko false i nil sa traktowane jako fałsz
-- wszystko inne, w tym liczba 0, jest prawdą

-- Zasady:
-- AND: jezeli pierwsza wartosc jest falszem, zwroc ja i zakoncz. W innym przypadku zwroc wartosc drugą.
--  OR: jezeli pierwsza wartosc jest prawda, zwroc ja i zakoncz. W innym przypadku zwroc wartosc druga.
-- oceniaja drugi operand tylko wtedy, gdy jest to konieczne

x = x or v
if not x then x = v end -- rownowazne powyzszemu

-- konstruktory to wyrazenia, ktore tworza i inicjalizuja tablice
days = {"Sunday", "Monday", "Tuesday", "Wednesday",
        "Thursday", "Friday", "Saturday"}

print(days[0]) -- zwraca nil, indeksowanie jest od jedynki
print(days[1]) -- zwraca sunday

a={x=10, y=20}
-- to samo co
a={}; a["x"] = 10; a["y"] = 20

x = {a=1, b=2, -- te dwa obiekciki nie sa indeksowane, bo sa przypisane do nazwy
    {x=0, c="ss"}, -- ta tablice i ta na dole juz maja indeksy odpowiednio 1 i 2, bo nie maja nazwy
    {d="sss", s=100}
}
print(x[1]["c"]) -- zwraca 'ss'
-- w lua konstruktor tabeli obsluguje dwa oddzielne mechanizmy:
-- a. czesc slownikowa (hash map): wszystko, co ma przypisany klucz
-- b. czesc tablicowa: wszystko, co nie ma klucza

i = 1
x = 20
while i <= x do
    local x = i*2
    print(x)
    i = i + 1
end

