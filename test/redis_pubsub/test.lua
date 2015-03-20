local redis = require 'redis'
require 'redis_server'
local client = redis.connect('127.0.0.1',6379)
sub(client)

