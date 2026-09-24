package directory

import (
	"fmt"
	"game-server/framework/logicalactor"
)

type RedisOwnerDirectoryOptions struct {
	KeyPrefix string
}

type ownerEvent struct {
	ActorId logicalactor.ActorID `json:"actor_id"`
	Epoch   uint64               `json:"epoch"`
}

type Owner struct {
	Node  logicalactor.NodeInfo `json:"node"`
	Epoch uint64                `json:"epoch"`
}

var (
	redisOwnerKey = "%s"
	redisEpochKey = "%s.epoch"
)

func genRedisKey(prefix string, key string, args ...interface{}) string {
	return "{directory}." + prefix + fmt.Sprintf(key, args...)
}
