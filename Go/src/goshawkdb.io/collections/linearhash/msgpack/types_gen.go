package msgpack

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Bucket) DecodeMsg(dc *msgp.Reader) (err error) {
	var zbai uint32
	zbai, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(zbai) {
		(*z) = (*z)[:zbai]
	} else {
		(*z) = make(Bucket, zbai)
	}
	for zbzg := range *z {
		(*z)[zbzg], err = dc.ReadBytes((*z)[zbzg])
		if err != nil {
			return
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Bucket) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		return
	}
	for zcmr := range z {
		err = en.WriteBytes(z[zcmr])
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z Bucket) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for zcmr := range z {
		o = msgp.AppendBytes(o, z[zcmr])
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Bucket) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zwht uint32
	zwht, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if cap((*z)) >= int(zwht) {
		(*z) = (*z)[:zwht]
	} else {
		(*z) = make(Bucket, zwht)
	}
	for zajw := range *z {
		(*z)[zajw], bts, err = msgp.ReadBytesBytes(bts, (*z)[zajw])
		if err != nil {
			return
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z Bucket) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zhct := range z {
		s += msgp.BytesPrefixSize + len(z[zhct])
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *RootRaw) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zcua uint32
	zcua, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zcua > 0 {
		zcua--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Size":
			err = z.Size.DecodeMsg(dc)
			if err != nil {
				return
			}
		case "BucketCount":
			err = z.BucketCount.DecodeMsg(dc)
			if err != nil {
				return
			}
		case "SplitIndex":
			err = z.SplitIndex.DecodeMsg(dc)
			if err != nil {
				return
			}
		case "MaskHigh":
			err = z.MaskHigh.DecodeMsg(dc)
			if err != nil {
				return
			}
		case "MaskLow":
			err = z.MaskLow.DecodeMsg(dc)
			if err != nil {
				return
			}
		case "HashKey":
			z.HashKey, err = dc.ReadBytes(z.HashKey)
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *RootRaw) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 6
	// write "Size"
	err = en.Append(0x86, 0xa4, 0x53, 0x69, 0x7a, 0x65)
	if err != nil {
		return err
	}
	err = z.Size.EncodeMsg(en)
	if err != nil {
		return
	}
	// write "BucketCount"
	err = en.Append(0xab, 0x42, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x43, 0x6f, 0x75, 0x6e, 0x74)
	if err != nil {
		return err
	}
	err = z.BucketCount.EncodeMsg(en)
	if err != nil {
		return
	}
	// write "SplitIndex"
	err = en.Append(0xaa, 0x53, 0x70, 0x6c, 0x69, 0x74, 0x49, 0x6e, 0x64, 0x65, 0x78)
	if err != nil {
		return err
	}
	err = z.SplitIndex.EncodeMsg(en)
	if err != nil {
		return
	}
	// write "MaskHigh"
	err = en.Append(0xa8, 0x4d, 0x61, 0x73, 0x6b, 0x48, 0x69, 0x67, 0x68)
	if err != nil {
		return err
	}
	err = z.MaskHigh.EncodeMsg(en)
	if err != nil {
		return
	}
	// write "MaskLow"
	err = en.Append(0xa7, 0x4d, 0x61, 0x73, 0x6b, 0x4c, 0x6f, 0x77)
	if err != nil {
		return err
	}
	err = z.MaskLow.EncodeMsg(en)
	if err != nil {
		return
	}
	// write "HashKey"
	err = en.Append(0xa7, 0x48, 0x61, 0x73, 0x68, 0x4b, 0x65, 0x79)
	if err != nil {
		return err
	}
	err = en.WriteBytes(z.HashKey)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *RootRaw) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 6
	// string "Size"
	o = append(o, 0x86, 0xa4, 0x53, 0x69, 0x7a, 0x65)
	o, err = z.Size.MarshalMsg(o)
	if err != nil {
		return
	}
	// string "BucketCount"
	o = append(o, 0xab, 0x42, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x43, 0x6f, 0x75, 0x6e, 0x74)
	o, err = z.BucketCount.MarshalMsg(o)
	if err != nil {
		return
	}
	// string "SplitIndex"
	o = append(o, 0xaa, 0x53, 0x70, 0x6c, 0x69, 0x74, 0x49, 0x6e, 0x64, 0x65, 0x78)
	o, err = z.SplitIndex.MarshalMsg(o)
	if err != nil {
		return
	}
	// string "MaskHigh"
	o = append(o, 0xa8, 0x4d, 0x61, 0x73, 0x6b, 0x48, 0x69, 0x67, 0x68)
	o, err = z.MaskHigh.MarshalMsg(o)
	if err != nil {
		return
	}
	// string "MaskLow"
	o = append(o, 0xa7, 0x4d, 0x61, 0x73, 0x6b, 0x4c, 0x6f, 0x77)
	o, err = z.MaskLow.MarshalMsg(o)
	if err != nil {
		return
	}
	// string "HashKey"
	o = append(o, 0xa7, 0x48, 0x61, 0x73, 0x68, 0x4b, 0x65, 0x79)
	o = msgp.AppendBytes(o, z.HashKey)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *RootRaw) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zxhx uint32
	zxhx, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zxhx > 0 {
		zxhx--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Size":
			bts, err = z.Size.UnmarshalMsg(bts)
			if err != nil {
				return
			}
		case "BucketCount":
			bts, err = z.BucketCount.UnmarshalMsg(bts)
			if err != nil {
				return
			}
		case "SplitIndex":
			bts, err = z.SplitIndex.UnmarshalMsg(bts)
			if err != nil {
				return
			}
		case "MaskHigh":
			bts, err = z.MaskHigh.UnmarshalMsg(bts)
			if err != nil {
				return
			}
		case "MaskLow":
			bts, err = z.MaskLow.UnmarshalMsg(bts)
			if err != nil {
				return
			}
		case "HashKey":
			z.HashKey, bts, err = msgp.ReadBytesBytes(bts, z.HashKey)
			if err != nil {
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *RootRaw) Msgsize() (s int) {
	s = 1 + 5 + z.Size.Msgsize() + 12 + z.BucketCount.Msgsize() + 11 + z.SplitIndex.Msgsize() + 9 + z.MaskHigh.Msgsize() + 8 + z.MaskLow.Msgsize() + 8 + msgp.BytesPrefixSize + len(z.HashKey)
	return
}
