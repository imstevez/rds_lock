pub const W_LOCK: &str = r#"if redis.call("EXISTS", KEYS[1]) > 0  then
    return 0
end
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
return 1"#;

pub const R_LOCK: &str = r#"local key_type = redis.call("TYPE", KEYS[1]).ok
if key_type ~= "none" and key_type ~= "zset" then
    return 0
end
local ct = redis.call("TIME")
local score = tonumber(ct[1]) * 1000 + math.floor(tonumber(ct[2]) / 1000) + tonumber(ARGV[2])
local rt = redis.call("ZADD", KEYS[1], "NX", score, ARGV[1])
if rt == 0 then
    return 0
end
local longest = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
redis.call('PEXPIREAT', KEYS[1], tonumber(longest[2]))
return 1"#;

pub const W_EXTEND: &str = r#"local key_type = redis.call("TYPE", KEYS[1]).ok
if key_type ~= "string" then
    return 0
end
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call("PEXPIRE", KEYS[1], ARGV[2])"#;

pub const R_EXTEND: &str = r#"local key_type = redis.call("TYPE", KEYS[1]).ok
if key_type ~= "zset" then
    return 0
end
if redis.call("ZSCORE", KEYS[1], ARGV[1]) == false then
    return 0
end
local ct = redis.call("TIME")
local score = tonumber(ct[1]) * 1000 + math.floor(tonumber(ct[2]) / 1000) + tonumber(ARGV[2])
redis.call("ZADD", KEYS[1], score, ARGV[1])
local longest = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
redis.call('PEXPIREAT', KEYS[1], tonumber(longest[2]))
return 1"#;

pub const W_UNLOCK: &str = r#"local key_type = redis.call("TYPE", KEYS[1]).ok
if key_type ~= "string" then
    return 0
end
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call("DEL", KEYS[1])"#;

pub const R_UNLOCK: &str = r#"local key_type = redis.call("TYPE", KEYS[1]).ok
if key_type ~= "zset" then
    return 0
end
local rt = redis.call("ZREM", KEYS[1], ARGV[1])
if rt == 0 then
    return 0
end
local longest = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if #longest > 0 then
    redis.call('PEXPIREAT', KEYS[1], tonumber(longest[2]))
end
return 1"#;
