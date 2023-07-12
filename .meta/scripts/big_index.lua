#!lua name=metadatum
local function check_keys(keys, num_key)
    local error = nil
    local nkeys = table.getn(keys)
    if nkeys == 0 then
      error = 'Hash key name not provided'
    elseif nkeys > num_key then
      error = string.format('Only %d key name is allowed', num_key)
    end
  
    if error ~= nil then
      redis.log(redis.LOG_WARNING, error);
      return redis.error_reply(error)
    end
    return nil
end

-- Each term is added to the big index as a separate entry ('big_idx:' prefix).
-- Each term has a corresponding set of entity references ('bi_ref:' prefix).
-- keys:
--   1) key[1] - reference key (normalized SHA1 id, prefix with SHA1 hash, but without ':')
--   2) key[2] - number of chars from SHA1 hash (only) to be used as a bicket id
-- args: list of terms to be added to the index. For performance reasons, 
--       it is better to remove duplicates before calling this script
local function big_index_update(keys, args)
    local error = check_keys(keys, 2)
    if error ~= nil then
      return error
    end
  
    for i= 1,#args do
        -- string.gsub(str, "%s+") will remove all spaces from the string
        -- local term = string.lower(string.gsub(args[i], "%s+"))
        local term = args[i]
        local id = redis.sha1hex(term)
        -- add entity reference to the term related entity references set 
        local bi_id = 'big_idx' .. ':' .. id
        local ref_id = 'bi_ref' .. ':' .. id       
        redis.call('SADD', ref_id, keys[1])
        -- big index hash
        local bi = {}
        table.insert(bi, "__id")
        table.insert(bi, id)
        table.insert(bi, 'name')
        table.insert(bi, term)
        table.insert(bi, 'TF')
        table.insert(bi, redis.call('SCARD', ref_id))
        table.insert(bi, 'bucket')
        table.insert(bi, string.sub(id, 1, keys[2]))

        -- bi['__id'] = id
        -- bi['name'] = term
        -- bi['TF'] = redis.call('SCARD', ref_id)
        -- bi['bucket'] = string.sub(id, 1, keys[2])

        redis.call('HSET', bi_id, unpack(bi))
    end
    -- update HLL for entity reference hll prefix and last 40 chars of the entity id (it is SHA1 hash only)
    local _hll_id = '_hll:' .. string.sub(keys[1], -40)
    redis.call('PFADD', _hll_id, unpack(args))
    -- return HLL cardinality
    return redis.call('PFCOUNT', _hll_id)
end

redis.register_function('big_index_update', big_index_update)