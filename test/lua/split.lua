local cjson = require 'cjson'

function split(s,p)
    local t = {}
    local b = string.gsub(s,'||','| |')
    string.gsub(b,'[^' ..p.. ']+',function(w) 
            table.insert(t,w) 
        end)
    return t
end

local a = "a|b|c||d| |"
print(cjson.encode(split(a,'|')))
