package snowflake

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const totalBitLength = 63

// Config controls one SnowFlake instance. The bit lengths must add up to 63.
type Config struct {
	TimestampBits int
	MachineIDBits int
	SequenceBits  int
	MachineID     int64
	StartTime     time.Time
}

// DefaultConfig returns the default SnowFlake layout.
func DefaultConfig() Config {
	return Config{
		TimestampBits: 41,
		MachineIDBits: 12,
		SequenceBits:  10,
		MachineID:     0,
		StartTime:     time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

// SnowFlake generates and decodes IDs using an instance-local configuration.
// A SnowFlake is safe for concurrent use.
type SnowFlake struct {
	timestampBits int
	machineIDBits int
	sequenceBits  int

	maxTimestamp int64
	maxMachineID int64
	maxSequence  int64

	shiftMachineID int
	shiftTimestamp int

	machineID  int64
	offsetTime int64 // Unix milliseconds.

	mu       sync.Mutex
	lastTime int64
	lastSeq  int64
}

// New creates an independently configured SnowFlake instance.
func New(config Config) (*SnowFlake, error) {
	if config.TimestampBits <= 0 || config.MachineIDBits <= 0 || config.SequenceBits <= 0 {
		return nil, errors.New("snowflake bit lengths must be positive")
	}
	if config.TimestampBits+config.MachineIDBits+config.SequenceBits != totalBitLength {
		return nil, fmt.Errorf("snowflake bit lengths must add up to %d", totalBitLength)
	}
	if config.StartTime.IsZero() {
		return nil, errors.New("snowflake start time cannot be zero")
	}
	if config.StartTime.After(time.Now()) {
		return nil, errors.New("snowflake start time cannot be in the future")
	}
	if config.TimestampBits >= 63 || config.MachineIDBits >= 63 || config.SequenceBits >= 63 {
		return nil, errors.New("snowflake bit length is too large")
	}

	s := &SnowFlake{
		timestampBits:  config.TimestampBits,
		machineIDBits:  config.MachineIDBits,
		sequenceBits:   config.SequenceBits,
		maxTimestamp:   (int64(1) << config.TimestampBits) - 1,
		maxMachineID:   (int64(1) << config.MachineIDBits) - 1,
		maxSequence:    (int64(1) << config.SequenceBits) - 1,
		shiftMachineID: config.SequenceBits,
		shiftTimestamp: config.SequenceBits + config.MachineIDBits,
		machineID:      config.MachineID,
		offsetTime:     config.StartTime.UnixMilli(),
	}
	if config.MachineID < 0 || config.MachineID > s.maxMachineID {
		return nil, fmt.Errorf("snowflake machine ID %d is outside [0,%d]", config.MachineID, s.maxMachineID)
	}
	if time.Since(config.StartTime).Milliseconds() > s.maxTimestamp {
		return nil, errors.New("snowflake start time is outside timestamp range")
	}
	return s, nil
}

// NewDefault creates an instance using DefaultConfig.
func NewDefault() *SnowFlake {
	s, err := New(DefaultConfig())
	if err != nil {
		panic(err)
	}
	return s
}

func (s *SnowFlake) waitForMillis(last int64) int64 {
	now := time.Now().UnixMilli()
	for now <= last {
		time.Sleep(time.Microsecond)
		now = time.Now().UnixMilli()
	}
	return now
}

// GenId generates one ID.
func (s *SnowFlake) GenId() int64 {
	if s == nil {
		panic("snowflake is nil")
	}
	for {
		s.mu.Lock()
		now := time.Now().UnixMilli()
		if now < s.lastTime {
			now = s.lastTime
		}

		seq := int64(0)
		if now == s.lastTime {
			if s.lastSeq >= s.maxSequence {
				last := s.lastTime
				s.mu.Unlock()
				now = s.waitForMillis(last)
				continue
			}
			seq = s.lastSeq + 1
		}

		s.lastTime = now
		s.lastSeq = seq
		id := (now-s.offsetTime)<<s.shiftTimestamp | s.machineID<<s.shiftMachineID | seq
		s.mu.Unlock()
		return id
	}
}

// ParseIdToTimestamp returns the creation timestamp in Unix milliseconds.
func (s *SnowFlake) ParseIdToTimestamp(id int64) int64 {
	return (id >> s.shiftTimestamp) + s.offsetTime
}

// ParseIdToMachineId returns the machine ID encoded in id.
func (s *SnowFlake) ParseIdToMachineId(id int64) int32 {
	return int32((id >> s.sequenceBits) & s.maxMachineID)
}

// ParseIdToSequence returns the sequence number encoded in id.
func (s *SnowFlake) ParseIdToSequence(id int64) int32 {
	return int32(id & s.maxSequence)
}
