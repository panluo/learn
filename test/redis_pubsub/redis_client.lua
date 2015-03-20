local redis = require 'redis'
local conn = redis.connect('127.0.0.1',6379)
conn:publish("notic","nihao")

