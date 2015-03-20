function geo2string(geo)
    local user_coor = {}
    for i,v in pairs(geo.user_coordinate) do
        table.insert(user_coor,v.standard or ' ')
        table.insert(user_coor,v.latitude or ' ')
        table.insert(user_coor,v.longitude or ' ')
       -- user_coor = user_coor ..'_'.. table.concat({v.standard or ' ',v.latitude or ' ',v.longitude or ' '},'__')
    end
    
    local location = geo.user_location or {}
    local location_str = table.concat({location.province or ' ',location.city or ' ',location.district or ' ',location.street or ' '},'__')
    return location_str .. '_' .. table.concat(user_coor,'__')
end

local user_geo_info = {['user_coordinate'] = {{ standard = 1,latitude = 111, longitude = 222}}, ['user_location'] = { ['province'] = 'aaa',['city'] = 'bbb',['district'] = 'ccc',['street'] = 'ddd'}}

print(geo2string(user_geo_info))
