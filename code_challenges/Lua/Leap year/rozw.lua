-- funkcje sa first-classes values, tzn. traktowane sa tak jak string czy liczby i mozna je przypisać do zmiennej

local leap_year = function(number) -- to funkcja anonimowa, wszystko do end to cialo tej funkcji
local rok_p = (number % 4 == 0 and number % 100 ~= 0) or number % 400 == 0
return rok_p
end
print(leap_year(2020))