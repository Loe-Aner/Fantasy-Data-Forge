local kolory_map = {
    ["black"] = 0,
    ["brown"] = 1,
    ["red"] = 2,
    ["orange"] = 3,
    ["yellow"] = 4,
    ["green"] = 5,
    ["blue"] = 6,
    ["violet"] = 7,
    ["grey"] = 8,
    ["white"] = 9
}

local function Zadanie(a)
    local pierwszy = kolory_map[a[1]]
    local drugi = kolory_map[a[2]]
    local polaczone = pierwszy .. drugi
    print(tonumber(polaczone))
end

Zadanie({"green", "black", "white"})