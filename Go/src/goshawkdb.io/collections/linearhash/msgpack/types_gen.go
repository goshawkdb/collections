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
func (z *Root) DecodeMsg(dc *msgp.Reader) (err error) {
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
			z.Size, err = dc.ReadInt64()
			if err != nil {
				return
			}
		case "BucketCount":
			z.BucketCount, err = dc.ReadInt64()
			if err != nil {
				return
			}
		case "SplitIndex":
			z.SplitIndex, err = dc.ReadUint64()
			if err != nil {
				return
			}
		case "MaskHigh":
			z.MaskHigh, err = dc.ReadUint64()
			if err != nil {
				return
			}
		case "MaskLow":
			z.MaskLow, err = dc.ReadUint64()
			if err != nil {
				return
			}
		case "Hashkey":
			z.Hashkey, err = dc.ReadBytes(z.Hashkey)
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
func (z *Root) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 6
	// write "Size"
	err = en.Append(0x86, 0xa4, 0x53, 0x69, 0x7a, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteInt64(z.Size)
	if err != nil {
		return
	}
	// write "BucketCount"
	err = en.Append(0xab, 0x42, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x43, 0x6f, 0x75, 0x6e, 0x74)
	if err != nil {
		return err
	}
	err = en.WriteInt64(z.BucketCount)
	if err != nil {
		return
	}
	// write "SplitIndex"
	err = en.Append(0xaa, 0x53, 0x70, 0x6c, 0x69, 0x74, 0x49, 0x6e, 0x64, 0x65, 0x78)
	if err != nil {
		return err
	}
	err = en.WriteUint64(z.SplitIndex)
	if err != nil {
		return
	}
	// write "MaskHigh"
	err = en.Append(0xa8, 0x4d, 0x61, 0x73, 0x6b, 0x48, 0x69, 0x67, 0x68)
	if err != nil {
		return err
	}
	err = en.WriteUint64(z.MaskHigh)
	if err != nil {
		return
	}
	// write "MaskLow"
	err = en.Append(0xa7, 0x4d, 0x61, 0x73, 0x6b, 0x4c, 0x6f, 0x77)
	if err != nil {
		return err
	}
	err = en.WriteUint64(z.MaskLow)
	if err != nil {
		return
	}
	// write "Hashkey"
	err = en.Append(0xa7, 0x48, 0x61, 0x73, 0x68, 0x6b, 0x65, 0x79)
	if err != nil {
		return err
	}
	err = en.WriteBytes(z.Hashkey)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Root) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 6
	// string "Size"
	o = append(o, 0x86, 0xa4, 0x53, 0x69, 0x7a, 0x65)
	o = msgp.AppendInt64(o, z.Size)
	// string "BucketCount"
	o = append(o, 0xab, 0x42, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x43, 0x6f, 0x75, 0x6e, 0x74)
	o = msgp.AppendInt64(o, z.BucketCount)
	// string "SplitIndex"
	o = append(o, 0xaa, 0x53, 0x70, 0x6c, 0x69, 0x74, 0x49, 0x6e, 0x64, 0x65, 0x78)
	o = msgp.AppendUint64(o, z.SplitIndex)
	// string "MaskHigh"
	o = append(o, 0xa8, 0x4d, 0x61, 0x73, 0x6b, 0x48, 0x69, 0x67, 0x68)
	o = msgp.AppendUint64(o, z.MaskHigh)
	// string "MaskLow"
	o = append(o, 0xa7, 0x4d, 0x61, 0x73, 0x6b, 0x4c, 0x6f, 0x77)
	o = msgp.AppendUint64(o, z.MaskLow)
	// string "Hashkey"
	o = append(o, 0xa7, 0x48, 0x61, 0x73, 0x68, 0x6b, 0x65, 0x79)
	o = msgp.AppendBytes(o, z.Hashkey)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Root) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
			z.Size, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				return
			}
		case "BucketCount":
			z.BucketCount, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				return
			}
		case "SplitIndex":
			z.SplitIndex, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				return
			}
		case "MaskHigh":
			z.MaskHigh, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				return
			}
		case "MaskLow":
			z.MaskLow, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				return
			}
		case "Hashkey":
			z.Hashkey, bts, err = msgp.ReadBytesBytes(bts, z.Hashkey)
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
func (z *Root) Msgsize() (s int) {
	s = 1 + 5 + msgp.Int64Size + 12 + msgp.Int64Size + 11 + msgp.Uint64Size + 9 + msgp.Uint64Size + 8 + msgp.Uint64Size + 8 + msgp.BytesPrefixSize + len(z.Hashkey)
	return
}
