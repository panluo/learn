local redis = require 'redis'
local logger = require 'shared.log'
require 'shared.print_table'

function sub()
	local client = redis.connect('127.0.0.1',6379)
	local channels = {'control_channel','delete','create','update'}

	for msg,abort in client:pubsub({subscribe = channels}) do 
		if msg.kind == 'subscribe' then
			logger:info("Subscribed to channel " .. msg.channel)
		elseif msg.kind == 'message' then
			if msg.channel == 'control_channel' then
				if msg.payload == 'quit_loop' then
					logger:info("Aborting pubsub loop ...")
					abort()
				else
					logger:info("Received an unrecognized command:" .. msg.payload)
				end
			elseif msg.channel == 'delete' then
				--delete(msg.payload)
				logger:info("delete campaign info")
			elseif msg.channel == 'create' then
				--create(msg.payload)
			elseif msg.channel == 'update' then
				--update(msg.payload)
				logger:info("Received \n" .. msg.payload)
			else 
				logger:info("Received \n" .. msg.channel .. "\n" .. msg.payload)
			end
		end
	end
end
--function sub(client)
--	local client = redis.connect('127.0.0.1',6379)
--	local result = client:subscribe("create")
--local res = client:publish("new.art","hello")
	--client:command("publish","news.art","hello1")

--	return ser(result)
--end
--sub()
