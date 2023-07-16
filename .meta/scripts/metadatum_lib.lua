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

local function areHllEqual(hll1, hll2)
  local hll1_size = redis.call('PFCOUNT', hll1)
  local hll2_size = redis.call('PFCOUNT', hll2)
  if hll1_size ~= hll2_size then
    return false
  end

  local hll1_union = redis.call('PFMERGE', 'hll1_union', hll1, hll2)
  local hll1_union_size = redis.call('PFCOUNT', hll1_union)
  if hll1_union_size ~= hll1_size then
    return false
  end

  return true
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

        redis.call('HSET', bi_id, unpack(bi))
    end
    -- update HLL for entity reference hll prefix and last 40 chars of the entity id (it is SHA1 hash only)
    local _hll_id = '_hll:' .. string.sub(keys[1], -40)
    redis.call('PFADD', _hll_id, unpack(args))
    -- return HLL cardinality
    return redis.call('PFCOUNT', _hll_id)
end

-- commit all changes to the processed (completed) records from 
-- the transaction index
-- keys:
--   1) key[1] - reference to sha1_id from full commit instance id
--   3) key[2] - timestamp from commit instance
-- args: list of doc id from the transaction index
local function commit(keys, args)
    local error = check_keys(keys, 2)
    if error ~= nil then
      return error
    end

    local original = 'original'
    local updated = 'updated'
    local deleted = 'deleted'
    
    local commit_id = keys[1]
    local timestamp = keys[3]

    for i= 1, #args do
        local item_id = redis.call('HGET', args[i], 'item_id')
        local item_prefix = redis.call('HGET', args[i], 'item_prefix')
        -- The processing item is still with underscored prefix
        local _item_key = '_' .. item_prefix .. ':' .. item_id
        local item_key = item_prefix .. ':' .. item_id
        
        local _hll_id = '_hll:' .. item_id
        local hll_id = 'hll:' .. item_id

        if redis.call('PFCOUNT', _hll_id) == 0 then
          -- if new item is empty 
          -- Make no action just remove temporary keys from Redis
          redis.call('DEL', _item_key)
          redis.call('DEL', _hll_id)
        
        elseif redis.call('PFCOUNT', hll_id) == 0 then
          --  commit status ('original', 'updated', 'deleted')
          redis.call('HSET', _item_key, 'commit_id', commit_id)
          redis.call('HSET', _item_key, 'commit_status', original)
          -- We have a new item and HLL. Rename them to the final names
          redis.call('RENAME', _hll_id, hll_id)
          redis.call('RENAME', _item_key, item_key)

        elseif areHllEqual(hll_id, _hll_id) then
          -- if new and old items are equal 
          -- Make no action just remove temporary keys from Redis
          -- !!! This condition may be contested !!!
          redis.call('DEL', _item_key)
          redis.call('DEL', _hll_id)
        else
          -- We have an updated item and HLL. Rename them to the final names
          -- create references to the HLL and item in the commit_tail index.
          -- tail_id based on item_id and commit_id. Each of them may be not unique
          -- in commit_tail index, but the combination of both should be unique
          local tail_id = redis.sha1hex(item_id .. commit_id)
          local tail_key = 'commit_tail:' .. tail_id

          local t_commit_id = redis.call('HGET', item_key, 'commit_id')
          local t_commit_status = redis.call('HGET', item_key, 'commit_status')

          local tail = {}
          table.insert(tail, '__id')
          table.insert(tail, tail_id)
          table.insert(tail, 'item_id')
          table.insert(tail, item_id)
          table.insert(tail, 'commit_id')
          table.insert(tail, t_commit_id)
          table.insert(tail, 'timestamp')
          table.insert(tail, timestamp)
          table.insert(tail, 'commit_status')
          table.insert(tail, t_commit_status)

          redis.call('HSET', tail_key, unpack(tail))
          -- Move current item and HLL to the commit_tail index by renaming to final names
          redis.call('RENAME', hll_id, '_hll:' .. tail_id)
          redis.call('RENAME', item_id, item_prefix .. ':' .. tail_id)
          
          --  commit status ('original', 'updated', 'deleted')
          redis.call('HSET', _item_key, 'commit_id', commit_id)
          -- Update temporary item with the new commit status
          redis.call('HSET', _item_key, 'commit_status', updated)
          -- Make them current by renaming them to the final names
          redis.call('RENAME', _hll_id, hll_id)
          redis.call('RENAME', _item_key, item_key)
        end

        -- Clean transaction index
        redis.call('DEL', args[i])
    end

end

redis.register_function('big_index_update', big_index_update)
redis.register_function('commit', commit)