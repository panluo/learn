local redis = require 'redis'
local logger = require 'shared.log'
require "shared.print_table"

local conn = redis.connect('127.0.0.1',6379)
local res = conn:subscribe("news.*")
 
local t = os.time()
while true do 
	if os.time()-t >100 then
		break
	end
	logger:info("time" ..tostring( os.time))
	logger:debug("%s",ser(res))
end

