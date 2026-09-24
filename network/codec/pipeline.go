package codec

import "math/bits"

const (
	FlagCompressed uint8 = 1 << iota
	FlagEncrypted
)

type Transform interface {
	Flag() uint8
	Encode(data []byte) ([]byte, bool, error)
	Decode(data []byte) ([]byte, error)
}

type Pipeline struct {
	transforms []Transform
	knownFlags uint8
}

// New
//
//	@Description: 创建编解码流水线
//	@param transforms 转换器列表
//	@return *Pipeline 编解码流水线
//	@return error 创建错误
func New(transforms ...Transform) (*Pipeline, error) {
	pipeline := &Pipeline{transforms: append([]Transform(nil), transforms...)}
	for _, transform := range pipeline.transforms {
		if transform == nil || bits.OnesCount8(transform.Flag()) != 1 {
			return nil, ErrInvalidFlag
		}
		if pipeline.knownFlags&transform.Flag() != 0 {
			return nil, ErrDuplicateFlag
		}
		pipeline.knownFlags |= transform.Flag()
	}
	return pipeline, nil
}

// Encode
//
//	@Description: 按顺序执行编码转换并汇总标记位
//	@receiver p 编解码流水线
//	@param data 原始数据
//	@return []byte 编码后的数据
//	@return uint8 已应用的标记位
//	@return error 编码错误
func (p *Pipeline) Encode(data []byte) ([]byte, uint8, error) {
	encoded := data
	var flags uint8
	for _, transform := range p.transforms {
		var (
			applied bool
			err     error
		)
		encoded, applied, err = transform.Encode(encoded)
		if err != nil {
			return nil, 0, err
		}
		if applied {
			flags |= transform.Flag()
		}
	}
	return encoded, flags, nil
}

// Decode
//
//	@Description: 按逆序执行解码转换
//	@receiver p 编解码流水线
//	@param data 编码后的数据
//	@param flags 已应用的标记位
//	@return []byte 解码后的数据
//	@return error 解码错误
func (p *Pipeline) Decode(data []byte, flags uint8) ([]byte, error) {
	if flags & ^p.knownFlags != 0 {
		return nil, ErrUnknownFlags
	}
	decoded := data
	for i := len(p.transforms) - 1; i >= 0; i-- {
		transform := p.transforms[i]
		if flags&transform.Flag() == 0 {
			continue
		}
		var err error
		decoded, err = transform.Decode(decoded)
		if err != nil {
			return nil, err
		}
	}
	return decoded, nil
}
