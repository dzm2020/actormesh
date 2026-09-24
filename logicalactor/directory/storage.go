package directory

import (
	"context"
	"fmt"
	"github.com/dzm2020/actormesh/logicalactor"
	"github.com/dzm2020/actormesh/pkg/serialize/jsoncodec"
	"time"

	"github.com/redis/go-redis/v9"
)

var getOwnerScript = redis.NewScript(`
local current_value = redis.call('GET', KEYS[1]) or ''
local epoch =  redis.call('GET', KEYS[2]) or '0'
return {epoch,current_value}
`)

var acquireOwnerScript = redis.NewScript(`
local candidate = cjson.decode(ARGV[1])
local current = redis.call('GET', KEYS[1])
local epoch =  redis.call('GET', KEYS[2]) or '0'
if current then
    local owner = cjson.decode(current)
    if owner.node_id and owner.instance_id then
        if owner.node_id ~= candidate.node_id then
            return {0,epoch, current}
        end
    end
end
epoch = redis.call('INCR', KEYS[2])
local ttl = tonumber(ARGV[2]) or 0
if ttl > 0 then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ttl)
else
    redis.call('SET', KEYS[1], ARGV[1])
end
return {1,epoch, ARGV[1]}
`)

var renewOwnerScript = redis.NewScript(`
local current_value = redis.call('GET', KEYS[1])
if not current_value then return 0 end
local current = cjson.decode(current_value)
local expected = cjson.decode(ARGV[1])
if current.node_id ~= expected.node_id or current.instance_id ~= expected.instance_id then
    return 0
end
local ttl = tonumber(ARGV[2]) or 0
if ttl <= 0 then return 1 end
return redis.call('PEXPIRE', KEYS[1], ttl)
`)

var deleteOwnerScript = redis.NewScript(`
local current_value = redis.call('GET', KEYS[1])
local epoch = redis.call('GET', KEYS[2]) or '0'
if not current_value then return {0,epoch} end

local current = cjson.decode(current_value)

local expected = cjson.decode(ARGV[1])

	if current.node_id == expected.node_id
		and current.instance_id == expected.instance_id then
		redis.call('DEL', KEYS[1])
		epoch = redis.call('INCR', KEYS[2])
		return {1,epoch}
end
return {0,epoch}
`)

func acquireOwnerFromRedis(client redis.UniversalClient, prefix string, actorId logicalactor.ActorID, candidate logicalactor.NodeInfo, leaseTTL time.Duration) (*Owner, bool, error) {
	candidateJSON, err := jsoncodec.Marshal(candidate)
	if err != nil {
		return nil, false, fmt.Errorf("encode candidate %s: %w", actorId.String(), err)
	}
	keys := []string{
		genRedisKey(prefix, redisOwnerKey, actorId.String()),
		genRedisKey(prefix, redisEpochKey, actorId.String()),
	}
	result, err := acquireOwnerScript.Run(context.Background(), client, keys, candidateJSON, leaseTTL.Milliseconds()).Slice()
	if err != nil {
		return nil, false, err
	}
	if len(result) != 3 {
		return nil, false, fmt.Errorf("acquire owner returned %d values", len(result))
	}
	acquire, err := redisResultBool(result[0])
	if err != nil {
		return nil, false, err
	}
	epoch, err := redisResultUint64(result[1])
	if err != nil {
		return nil, false, err
	}
	ownerStr, ok := result[2].(string)
	if !ok || ownerStr == "" {
		return nil, false, fmt.Errorf("acquire owner returned invalid owner %T", result[2])
	}
	var node logicalactor.NodeInfo
	if err = jsoncodec.Unmarshal([]byte(ownerStr), &node); err != nil {
		return nil, false, err
	}
	if err = node.Validate(); err != nil {
		return nil, false, fmt.Errorf("validate owner %s: %w", actorId.String(), err)
	}
	owner := &Owner{Node: node, Epoch: epoch}
	return owner, acquire, nil
}

func renewOwnerFromRedis(client redis.UniversalClient, prefix string, actorId logicalactor.ActorID, expected logicalactor.NodeInfo, leaseTTL time.Duration) (bool, error) {
	expectedJSON, err := jsoncodec.Marshal(expected)
	if err != nil {
		return false, fmt.Errorf("encode expected owner %s: %w", actorId.String(), err)
	}
	keys := []string{genRedisKey(prefix, redisOwnerKey, actorId.String())}
	result, err := renewOwnerScript.Run(context.Background(), client, keys, expectedJSON, leaseTTL.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func getOwnerFromRedis(client redis.UniversalClient, prefix string, actorId logicalactor.ActorID) (*Owner, bool, error) {
	keys := []string{
		genRedisKey(prefix, redisOwnerKey, actorId.String()),
		genRedisKey(prefix, redisEpochKey, actorId.String()),
	}
	result, err := getOwnerScript.Run(context.Background(), client, keys).Slice()
	if err != nil {
		return nil, false, err
	}
	if len(result) != 2 {
		return nil, false, fmt.Errorf("get owner returned %d values", len(result))
	}
	ownerValue, ok := result[1].(string)
	if !ok || ownerValue == "" {
		return nil, false, nil
	}
	epoch, err := redisResultUint64(result[0])
	if err != nil {
		return nil, false, err
	}
	var node logicalactor.NodeInfo
	if err = jsoncodec.Unmarshal([]byte(ownerValue), &node); err != nil {
		return nil, false, fmt.Errorf("decode owner %s: %w", actorId.String(), err)
	}
	if err = node.Validate(); err != nil {
		return nil, false, fmt.Errorf("validate owner %s: %w", actorId.String(), err)
	}
	return &Owner{Node: node, Epoch: epoch}, true, nil
}

func deleteOwnerFromRedis(client redis.UniversalClient, prefix string, actorId logicalactor.ActorID, expected logicalactor.NodeInfo) (bool, uint64, error) {
	expectedJSON, err := jsoncodec.Marshal(expected)
	if err != nil {
		return false, 0, fmt.Errorf("encode expected owner %s: %w", actorId.String(), err)
	}
	keys := []string{
		genRedisKey(prefix, redisOwnerKey, actorId.String()),
		genRedisKey(prefix, redisEpochKey, actorId.String()),
	}
	result, err := deleteOwnerScript.Run(context.Background(), client, keys, expectedJSON).Slice()
	if err != nil {
		return false, 0, err
	}
	if len(result) != 2 {
		return false, 0, fmt.Errorf("delete owner returned %d values", len(result))
	}
	deleted, err := redisResultBool(result[0])
	if err != nil {
		return false, 0, err
	}
	epoch, err := redisResultUint64(result[1])
	if err != nil {
		return false, 0, err
	}
	return deleted, epoch, nil
}

func redisResultBool(value interface{}) (bool, error) {
	n, err := redisResultUint64(value)
	return n == 1, err
}

func redisResultUint64(value interface{}) (uint64, error) {
	switch v := value.(type) {
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("negative redis integer %d", v)
		}
		return uint64(v), nil
	case uint64:
		return v, nil
	case string:
		var n uint64
		if _, err := fmt.Sscan(v, &n); err != nil {
			return 0, fmt.Errorf("invalid redis integer %q: %w", v, err)
		}
		return n, nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("invalid redis result type %T", value)
	}
}
