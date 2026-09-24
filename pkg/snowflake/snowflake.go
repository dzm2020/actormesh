package snowflake

import (
	"fmt"
	"sync/atomic"
	"time"
)

// 默认雪花算法常量
const (
	timestampLength = 41
	sequenceLength  = 10
	machineIDLength = 63 - timestampLength - sequenceLength
	maxSequence     = (1 << sequenceLength) - 1
	maxMachineID    = (1 << machineIDLength) - 1
	maxTimestamp    = (1 << timestampLength) - 1
	shiftMachineID  = sequenceLength
	shiftTimestamp  = sequenceLength + machineIDLength
)

// SnowFlake 雪花算法-数据结构
type SnowFlake struct {
	timestampBitLen int // 时间偏移位
	machineIDBitLen int // 机器ID偏移位
	sequenceBitLen  int // 自增序号偏移位

	maxSequence  int64 // 最大序号
	maxMachineID int64 // 最大机器ID
	maxTimestamp int64 // 最大时间戳

	shiftMachineID int // 机器码左移位数
	shiftTimestamp int // 时间戳左移位数

	machineID  int64     // 机器ID
	startTime  time.Time // 开始偏移时间
	offsetTime int64     // 起始偏移量

	lastTime int64 // 最后更新时间
	lastSeq  int64 // 最后自增的ID
	lastID   int64 // 最后生成的ID
}

// *SnowFlake
var globalSnowFlake *SnowFlake

func init() {
	globalSnowFlake = &SnowFlake{
		timestampBitLen: timestampLength,
		sequenceBitLen:  sequenceLength,
		machineIDBitLen: machineIDLength,
		maxSequence:     maxSequence,
		maxMachineID:    maxMachineID,
		maxTimestamp:    maxTimestamp,
		shiftMachineID:  shiftMachineID,
		shiftTimestamp:  shiftTimestamp,
		machineID:       0,
		startTime:       time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	globalSnowFlake.offsetTime = globalSnowFlake.startTime.UnixNano()
}

func waitForMillis(last int64) int64 {
	now := time.Now().UnixNano() / 1e6
	for now == last {
		now = time.Now().UnixNano() / 1e6
	}
	return now
}

func (s *SnowFlake) Show() {
	fmt.Println("===snowflake===")
	fmt.Println("maxYears",
		(1<<s.timestampBitLen-1)/(1000*60*60*24*365),
		"maxMachineID", 1<<s.machineIDBitLen-1,
		"maxSequence", 1<<s.sequenceBitLen-1)
}

// 自定义雪花算法偏移时间
func (s *SnowFlake) setStartTime(t time.Time) {
	// 开始偏移时间不能为0
	if t.IsZero() {
		panic("the start time cannot be a zero value")
	}
	// 开始偏移时间不能大于当前时间（毫秒时间戳）
	if t.After(time.Now()) {
		panic("the time cannot be greater than the current millisecond")
	}
	// 雪花算法，时间的周期决定了算法最大能支撑的时间，这里判断偏移时间和当前时间的最大差值不能超过设定的范围
	df := (time.Now().UnixNano() - t.UnixNano()) / 1e6
	if df > s.maxTimestamp {
		panic("the offset time cannot exceed the lifetime of the snowflake algorithm")
	}
	s.startTime = t
	s.offsetTime = s.startTime.UnixNano()
}

// 自定义机器码ID
func (s *SnowFlake) setMachineID(id int64) {
	// 设置的机器ID不能超过最大值，不能小于0
	if id > s.maxMachineID || id < 0 {
		panic(fmt.Sprintf("the machine id %d cannot be > %d or <= 0", id, s.maxMachineID))
	}
	s.machineID = id
}

// 减去偏移量后的时间
func (s *SnowFlake) getCurrentTimestamp() int64 {
	// 纳秒转毫秒
	return (time.Now().UnixNano() - s.offsetTime) >> 20 & s.maxTimestamp
}

// 另一种算法思路
// 以最后生成的ID进行原子操作，达到最大序号时，通过sleep等待
func (s *SnowFlake) fastId() int64 {
	for {
		localLastId := atomic.LoadInt64(&s.lastID)
		seq := localLastId & s.maxSequence
		lastTime := localLastId >> s.shiftTimestamp
		now := s.getCurrentTimestamp()
		if now > lastTime {
			seq = 0
		} else if seq >= s.maxSequence {
			time.Sleep(time.Duration(0xFFFFF - (time.Now().UnixNano() & 0xFFFFF)))
			continue
		} else {
			seq++
		}
		newID := now<<s.shiftTimestamp + s.machineID<<s.shiftMachineID + seq
		// 利用CAS原子操作，更新最后生成的ID
		if atomic.CompareAndSwapInt64(&s.lastID, localLastId, newID) {
			return newID
		}
		time.Sleep(time.Duration(20))
	}
}

// 当前采用生成的方法
func (s *SnowFlake) generateId() int64 {
	// 当前毫秒时间戳
	now := time.Now().UTC().UnixNano() / 1e6
	var last, localSeq, seq int64
	for {
		// 最后生成的时间
		last = atomic.LoadInt64(&s.lastTime)
		// 最后生成的序号
		localSeq = atomic.LoadInt64(&s.lastSeq)
		if last > now {
			//  发生时间回退 重置时间
			if last-now > 1000 {
				atomic.StoreInt64(&s.lastTime, now)
			} else {
				// 自旋等待下一毫秒
				now = waitForMillis(now)
			}
			continue
		}

		if last == now {
			// 自增序号
			seq = s.maxSequence & (localSeq + 1)
			// 达到最大
			if seq == 0 {
				// 自旋等待下一毫秒
				now = waitForMillis(now)
				continue
			}
		}

		// 通过CAS交换更新时间和序号
		if atomic.CompareAndSwapInt64(&s.lastTime, last, now) && atomic.CompareAndSwapInt64(&s.lastSeq, localSeq, seq) {
			// 成功返回生成的ID
			return (now-s.offsetTime/1e6)<<s.shiftTimestamp + s.machineID<<s.shiftMachineID + seq
		}
	}
}

// SetSnowFlakeConfig 自定义雪花算法的常量
// 根据各自使用环境和场景的不同，提供自定义常量功能
// 通过性能测试分析，在密集生成ID时，自增序号分配越大，效率越高
// 对于一般应用，单机每秒理论生成1024000个ID，在实际过程中，已经足够使用
func SetSnowFlakeConfig(tLength, mLength, sLength int, machineID int64, t time.Time) {
	if tLength+mLength+sLength != (1<<6)-1 {
		panic("error summation of all parts of snowflake algorithm")
	}
	// 偏移位
	globalSnowFlake.timestampBitLen = tLength
	globalSnowFlake.machineIDBitLen = mLength
	globalSnowFlake.sequenceBitLen = sLength
	// 最大值
	globalSnowFlake.maxTimestamp = (1 << tLength) - 1
	globalSnowFlake.maxMachineID = (1 << mLength) - 1
	globalSnowFlake.maxSequence = (1 << sLength) - 1
	// 位移
	globalSnowFlake.shiftMachineID = sLength
	globalSnowFlake.shiftTimestamp = sLength + mLength
	// 偏移时间和机器ID
	globalSnowFlake.setStartTime(t)
	globalSnowFlake.setMachineID(machineID)

	globalSnowFlake.offsetTime = globalSnowFlake.startTime.UnixNano()
	globalSnowFlake.Show()
}

// GenId 对外提供生成ID的接口
func GenId() int64 {
	return globalSnowFlake.generateId()
}

// ParseIdToTimestamp 解析ID中的时间戳(得到生成时的时间戳）毫秒
func ParseIdToTimestamp(id int64) int64 {
	return id>>globalSnowFlake.shiftTimestamp + (globalSnowFlake.offsetTime / 1e6)
}

// ParseIdToMachineId 解析ID中的机器码
func ParseIdToMachineId(id int64) int32 {
	return int32((id >> globalSnowFlake.sequenceBitLen) & globalSnowFlake.maxMachineID)
}

// ParseIdToSequence 解析ID中的序列号
func ParseIdToSequence(id int64) int32 {
	return int32(id & globalSnowFlake.maxSequence)
}
