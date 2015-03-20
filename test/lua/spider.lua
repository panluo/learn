local http = require "socket.http"
local url = require "socket.url"
--local host = "http://bbs.1lou.com"
local host = "38.103.161.147"
require "queue"
--init
http.TIMEOUT = 10
local parse_url = url.parse(host)[authority]
local jpg = {}
--html
local L = List.new()
--torrent
local T = List.new()
local hash = {}
List.pushleft(T, host)
hash[host] = host

--down
while true do
	local h
	if T.first > T.last then
		if L.first > L.last then
			break
		else
			h = List.popleft(L)
		end
	else
		h = List.popleft(T)
	end

	hash[h] = h
	local res, state, header = http.request(h)
	print("get: " .. h)
	print("res" .. res)
	if res then
		--string.gsub(res, "<script>.-</script>", "")
		--string.gsub(res, "<style>.-</style>", "")
		local content_disposition = header["content-disposition"]
		if content_disposition then
			--download torrent
			--print(content_disposition)
			local filename = string.match(content_disposition, "filename=\"(.-)\"")
			print("download " .. filename)
			if string.find(filename, "%.torrent") then
				if res ~= nil and not hash[filename] then
					hash[filename] = filename
					io.open("torrent/" .. filename, "wb"):write(res)
				else
					print(">>failed to get " .. v)
				end	
			end
		else
			--check & reg jpg and html
			string.gsub(res, "src=\"(.-)\"", 
			function(s)
				if string.find(s, "%.jpg$") then
					if string.find(s, "http") then
						--full url
						if url.parse(s)[authority] == parse_url then
							if not hash[s] then
								jpg[s] = s
							end
						end
					else
						--relative url
						local p = string.match(h, "(.+)/")
						p = p .. s
						if not hash[p] then
							jpg[p] = p
						end
					end
				end
			end)
			--TODO rewritethis function to reduce code length
			string.gsub(res, "href=\"(.-)\"", 
			function(s)
				if string.find(s, "%.html?$") then 
					--high posibility to be a normal html
					if string.find(s, "typeid") == nil then
						if string.find(s, "forum%-index") or 
							string.find(s, "thread%-index") or 
							string.find(s, "index%-index") then
							if string.find(s, "http") then
								if url.parse(s)[authority] == parse_url and not hash[s]then
									List.pushright(L, s)
								end
							else
								local p = string.match(h, "(.+)/")
								p = p .. s 
								if not hash[p] then
									List.pushright(L, p)
								end
							end
						elseif string.find(s, "attach%-dialog") then
							--high posibility to link to a torrent
							if string.find(s, "http") then
								if url.parse(s)[authority] == parse_url and not hash[s] then
									List.pushleft(L, s)
								end
							else
								local p = string.match(h, "(.+)/")
								p = p .. s 
								if not hash[p] then
									List.pushleft(L, p)
								end
							end
						elseif string.find(s, "attach%-download") then
							--high posibility to have a torrent
							if string.find(s, "http") then
								if url.parse(s)[authority] == parse_url and not hash[s]then
									List.pushleft(T, s)
								end
							else
								local p = string.match(h, "(.+)/")
								p = p .. s 
								if not hash[p] then
									List.pushleft(T, p)
								end
							end
						end
					end
				end
			end)

			--download jpg
			for k, v in pairs(jpg or {}) do
				--string.gsub(v, ".*/(.-)")
				local filename = string.reverse(string.match(string.reverse(v), "(.-)/"))
				if not hash[v] and not hash[filename] then
					--local data, s, h = http.request(v)
					jpg[v] = nil
					hash[v] = v
					hash[filename] = filename
					--if data ~= nil and s == 200 then
					--	print("download " .. filename)
					--	io.open("image/" .. filename, "wb"):write(data)
					--else
					--	print(">>failed to get " .. v)
					--end
					io.open("image/image.txt", "wb"):write(v .. " " .. filename)
				end
			end
		end
	end
	if L.first > L.last then
		break
	end
	print("T.first: " .. T.first .. "  T.last: " .. T.last)
	print("L.first: " .. L.first .. "  L.last: " .. L.last)
end
