// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: auth.proto

/*
	Package authpb is a generated protocol buffer package.

	It is generated from these files:
		auth.proto

	It has these top-level messages:
		UserAddOptions
		User
		Permission
		Role
*/
package authpb

import (
	"fmt"

	proto "github.com/golang/protobuf/proto"

	math "math"

	_ "github.com/gogo/protobuf/gogoproto"

	io "io"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Permission_Type int32

const (
	READ      Permission_Type = 0
	WRITE     Permission_Type = 1
	READWRITE Permission_Type = 2
)

var Permission_Type_name = map[int32]string{
	0: "READ",
	1: "WRITE",
	2: "READWRITE",
}
var Permission_Type_value = map[string]int32{
	"READ":      0,
	"WRITE":     1,
	"READWRITE": 2,
}

func (x Permission_Type) String() string {
	return proto.EnumName(Permission_Type_name, int32(x))
}
func (Permission_Type) EnumDescriptor() ([]byte, []int) { return fileDescriptorAuth, []int{2, 0} }

type UserAddOptions struct {
	// 指定添加的用户是否为无密码模式
	NoPassword bool `protobuf:"varint,1,opt,name=no_password,json=noPassword,proto3" json:"no_password,omitempty"`
}

func (m *UserAddOptions) Reset()                    { *m = UserAddOptions{} }
func (m *UserAddOptions) String() string            { return proto.CompactTextString(m) }
func (*UserAddOptions) ProtoMessage()               {}
func (*UserAddOptions) Descriptor() ([]byte, []int) { return fileDescriptorAuth, []int{0} }

// User is a single entry in the bucket authUsers
type User struct {
	// 用户名
	Name     []byte          `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// 经过一系列加密算法加密后的 用户密码
	Password []byte          `protobuf:"bytes,2,opt,name=password,proto3" json:"password,omitempty"`
	// 当前用户具有的角色（即权限）
	Roles    []string        `protobuf:"bytes,3,rep,name=roles" json:"roles,omitempty"`
	Options  *UserAddOptions `protobuf:"bytes,4,opt,name=options" json:"options,omitempty"`
}

func (m *User) Reset()                    { *m = User{} }
func (m *User) String() string            { return proto.CompactTextString(m) }
func (*User) ProtoMessage()               {}
func (*User) Descriptor() ([]byte, []int) { return fileDescriptorAuth, []int{1} }

// Permission is a single entity
type Permission struct {
	PermType Permission_Type `protobuf:"varint,1,opt,name=permType,proto3,enum=authpb.Permission_Type" json:"permType,omitempty"`
	Key      []byte          `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	RangeEnd []byte          `protobuf:"bytes,3,opt,name=range_end,json=rangeEnd,proto3" json:"range_end,omitempty"`
}

func (m *Permission) Reset()                    { *m = Permission{} }
func (m *Permission) String() string            { return proto.CompactTextString(m) }
func (*Permission) ProtoMessage()               {}
func (*Permission) Descriptor() ([]byte, []int) { return fileDescriptorAuth, []int{2} }

// Role is a single entry in the bucket authRoles
type Role struct {
	Name          []byte        `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	KeyPermission []*Permission `protobuf:"bytes,2,rep,name=keyPermission" json:"keyPermission,omitempty"`
}

func (m *Role) Reset()                    { *m = Role{} }
func (m *Role) String() string            { return proto.CompactTextString(m) }
func (*Role) ProtoMessage()               {}
func (*Role) Descriptor() ([]byte, []int) { return fileDescriptorAuth, []int{3} }

func init() {
	proto.RegisterType((*UserAddOptions)(nil), "authpb.UserAddOptions")
	proto.RegisterType((*User)(nil), "authpb.User")
	proto.RegisterType((*Permission)(nil), "authpb.Permission")
	proto.RegisterType((*Role)(nil), "authpb.Role")
	proto.RegisterEnum("authpb.Permission_Type", Permission_Type_name, Permission_Type_value)
}
func (m *UserAddOptions) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *UserAddOptions) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.NoPassword {
		dAtA[i] = 0x8
		i++
		if m.NoPassword {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i++
	}
	return i, nil
}

func (m *User) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *User) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Name) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintAuth(dAtA, i, uint64(len(m.Name)))
		i += copy(dAtA[i:], m.Name)
	}
	if len(m.Password) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintAuth(dAtA, i, uint64(len(m.Password)))
		i += copy(dAtA[i:], m.Password)
	}
	if len(m.Roles) > 0 {
		for _, s := range m.Roles {
			dAtA[i] = 0x1a
			i++
			l = len(s)
			for l >= 1<<7 {
				dAtA[i] = uint8(uint64(l)&0x7f | 0x80)
				l >>= 7
				i++
			}
			dAtA[i] = uint8(l)
			i++
			i += copy(dAtA[i:], s)
		}
	}
	if m.Options != nil {
		dAtA[i] = 0x22
		i++
		i = encodeVarintAuth(dAtA, i, uint64(m.Options.Size()))
		n1, err := m.Options.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n1
	}
	return i, nil
}

func (m *Permission) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Permission) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.PermType != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintAuth(dAtA, i, uint64(m.PermType))
	}
	if len(m.Key) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintAuth(dAtA, i, uint64(len(m.Key)))
		i += copy(dAtA[i:], m.Key)
	}
	if len(m.RangeEnd) > 0 {
		dAtA[i] = 0x1a
		i++
		i = encodeVarintAuth(dAtA, i, uint64(len(m.RangeEnd)))
		i += copy(dAtA[i:], m.RangeEnd)
	}
	return i, nil
}

func (m *Role) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Role) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Name) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintAuth(dAtA, i, uint64(len(m.Name)))
		i += copy(dAtA[i:], m.Name)
	}
	if len(m.KeyPermission) > 0 {
		for _, msg := range m.KeyPermission {
			dAtA[i] = 0x12
			i++
			i = encodeVarintAuth(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	return i, nil
}

func encodeVarintAuth(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *UserAddOptions) Size() (n int) {
	var l int
	_ = l
	if m.NoPassword {
		n += 2
	}
	return n
}

func (m *User) Size() (n int) {
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovAuth(uint64(l))
	}
	l = len(m.Password)
	if l > 0 {
		n += 1 + l + sovAuth(uint64(l))
	}
	if len(m.Roles) > 0 {
		for _, s := range m.Roles {
			l = len(s)
			n += 1 + l + sovAuth(uint64(l))
		}
	}
	if m.Options != nil {
		l = m.Options.Size()
		n += 1 + l + sovAuth(uint64(l))
	}
	return n
}

func (m *Permission) Size() (n int) {
	var l int
	_ = l
	if m.PermType != 0 {
		n += 1 + sovAuth(uint64(m.PermType))
	}
	l = len(m.Key)
	if l > 0 {
		n += 1 + l + sovAuth(uint64(l))
	}
	l = len(m.RangeEnd)
	if l > 0 {
		n += 1 + l + sovAuth(uint64(l))
	}
	return n
}

func (m *Role) Size() (n int) {
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovAuth(uint64(l))
	}
	if len(m.KeyPermission) > 0 {
		for _, e := range m.KeyPermission {
			l = e.Size()
			n += 1 + l + sovAuth(uint64(l))
		}
	}
	return n
}

func sovAuth(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozAuth(x uint64) (n int) {
	return sovAuth(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *UserAddOptions) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowAuth
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: UserAddOptions: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: UserAddOptions: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field NoPassword", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAuth
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.NoPassword = bool(v != 0)
		default:
			iNdEx = preIndex
			skippy, err := skipAuth(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthAuth
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *User) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowAuth
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: User: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: User: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAuth
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthAuth
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = append(m.Name[:0], dAtA[iNdEx:postIndex]...)
			if m.Name == nil {
				m.Name = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Password", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAuth
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthAuth
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Password = append(m.Password[:0], dAtA[iNdEx:postIndex]...)
			if m.Password == nil {
				m.Password = []byte{}
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Roles", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAuth
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthAuth
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Roles = append(m.Roles, string(dAtA[iNdEx:postIndex]))
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Options", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAuth
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthAuth
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Options == nil {
				m.Options = &UserAddOptions{}
			}
			if err := m.Options.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipAuth(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthAuth
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Permission) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowAuth
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Permission: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Permission: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field PermType", wireType)
			}
			m.PermType = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAuth
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.PermType |= (Permission_Type(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Key", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAuth
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthAuth
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Key = append(m.Key[:0], dAtA[iNdEx:postIndex]...)
			if m.Key == nil {
				m.Key = []byte{}
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RangeEnd", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAuth
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthAuth
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.RangeEnd = append(m.RangeEnd[:0], dAtA[iNdEx:postIndex]...)
			if m.RangeEnd == nil {
				m.RangeEnd = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipAuth(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthAuth
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Role) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowAuth
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Role: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Role: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAuth
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthAuth
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = append(m.Name[:0], dAtA[iNdEx:postIndex]...)
			if m.Name == nil {
				m.Name = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field KeyPermission", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAuth
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthAuth
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.KeyPermission = append(m.KeyPermission, &Permission{})
			if err := m.KeyPermission[len(m.KeyPermission)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipAuth(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthAuth
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipAuth(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowAuth
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowAuth
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowAuth
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthAuth
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowAuth
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipAuth(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthAuth = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowAuth   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("auth.proto", fileDescriptorAuth) }

var fileDescriptorAuth = []byte{
	// 338 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x91, 0xcf, 0x4e, 0xea, 0x40,
	0x14, 0xc6, 0x3b, 0xb4, 0x70, 0xdb, 0xc3, 0x85, 0x90, 0x13, 0x72, 0x6f, 0x83, 0x49, 0x6d, 0xba,
	0x6a, 0x5c, 0x54, 0x85, 0x8d, 0x5b, 0x8c, 0x2c, 0x5c, 0x49, 0x26, 0x18, 0x97, 0xa4, 0xa4, 0x13,
	0x24, 0xc0, 0x4c, 0x33, 0x83, 0x31, 0x6c, 0x7c, 0x0e, 0x17, 0x3e, 0x10, 0x4b, 0x1e, 0x41, 0xf0,
	0x45, 0x4c, 0x67, 0xf8, 0x13, 0xa2, 0xbb, 0xef, 0x7c, 0xe7, 0xfb, 0x66, 0x7e, 0x99, 0x01, 0x48,
	0x5f, 0x16, 0xcf, 0x49, 0x2e, 0xc5, 0x42, 0x60, 0xa5, 0xd0, 0xf9, 0xa8, 0xd5, 0x1c, 0x8b, 0xb1,
	0xd0, 0xd6, 0x65, 0xa1, 0xcc, 0x36, 0xba, 0x86, 0xfa, 0xa3, 0x62, 0xb2, 0x9b, 0x65, 0x0f, 0xf9,
	0x62, 0x22, 0xb8, 0xc2, 0x73, 0xa8, 0x72, 0x31, 0xcc, 0x53, 0xa5, 0x5e, 0x85, 0xcc, 0x7c, 0x12,
	0x92, 0xd8, 0xa5, 0xc0, 0x45, 0x7f, 0xe7, 0x44, 0x6f, 0xe0, 0x14, 0x15, 0x44, 0x70, 0x78, 0x3a,
	0x67, 0x3a, 0xf1, 0x97, 0x6a, 0x8d, 0x2d, 0x70, 0x0f, 0xcd, 0x92, 0xf6, 0x0f, 0x33, 0x36, 0xa1,
	0x2c, 0xc5, 0x8c, 0x29, 0xdf, 0x0e, 0xed, 0xd8, 0xa3, 0x66, 0xc0, 0x2b, 0xf8, 0x23, 0xcc, 0xcd,
	0xbe, 0x13, 0x92, 0xb8, 0xda, 0xfe, 0x97, 0x18, 0xe0, 0xe4, 0x94, 0x8b, 0xee, 0x63, 0xd1, 0x07,
	0x01, 0xe8, 0x33, 0x39, 0x9f, 0x28, 0x35, 0x11, 0x1c, 0x3b, 0xe0, 0xe6, 0x4c, 0xce, 0x07, 0xcb,
	0xdc, 0xa0, 0xd4, 0xdb, 0xff, 0xf7, 0x27, 0x1c, 0x53, 0x49, 0xb1, 0xa6, 0x87, 0x20, 0x36, 0xc0,
	0x9e, 0xb2, 0xe5, 0x0e, 0xb1, 0x90, 0x78, 0x06, 0x9e, 0x4c, 0xf9, 0x98, 0x0d, 0x19, 0xcf, 0x7c,
	0xdb, 0xa0, 0x6b, 0xa3, 0xc7, 0xb3, 0xe8, 0x02, 0x1c, 0x5d, 0x73, 0xc1, 0xa1, 0xbd, 0xee, 0x5d,
	0xc3, 0x42, 0x0f, 0xca, 0x4f, 0xf4, 0x7e, 0xd0, 0x6b, 0x10, 0xac, 0x81, 0x57, 0x98, 0x66, 0x2c,
	0x45, 0x03, 0x70, 0xa8, 0x98, 0xb1, 0x5f, 0x9f, 0xe7, 0x06, 0x6a, 0x53, 0xb6, 0x3c, 0x62, 0xf9,
	0xa5, 0xd0, 0x8e, 0xab, 0x6d, 0xfc, 0x09, 0x4c, 0x4f, 0x83, 0xb7, 0xfe, 0x6a, 0x13, 0x58, 0xeb,
	0x4d, 0x60, 0xad, 0xb6, 0x01, 0x59, 0x6f, 0x03, 0xf2, 0xb9, 0x0d, 0xc8, 0xfb, 0x57, 0x60, 0x8d,
	0x2a, 0xfa, 0x23, 0x3b, 0xdf, 0x01, 0x00, 0x00, 0xff, 0xff, 0x61, 0x66, 0xc6, 0x9d, 0xf4, 0x01,
	0x00, 0x00,
}
