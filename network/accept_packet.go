package network

import "encoding/binary"

// type AcceptPacket Packet
type AcceptPacket struct {
	packet     Packet
	sessionCtx SessionContext
	buffer     []byte
}

func (pck *AcceptPacket) Bytes() []byte {
	output := pck.packet.Bytes()
	// output := make([]byte, pck.dataOffset)
	// binary.BigEndian.PutUint16(output[0:], pck.packet.length)
	// output[4] = uint8(pck.packet.packetType)
	// output[5] = pck.packet.flag
	binary.BigEndian.PutUint16(output[8:], pck.sessionCtx.Version)
	binary.BigEndian.PutUint16(output[10:], pck.sessionCtx.Options)
	binary.BigEndian.PutUint16(output[12:], pck.sessionCtx.SessionDataUnit)
	binary.BigEndian.PutUint16(output[14:], pck.sessionCtx.TransportDataUnit)
	binary.BigEndian.PutUint16(output[16:], pck.sessionCtx.Histone)
	binary.BigEndian.PutUint16(output[18:], uint16(len(pck.buffer)))
	binary.BigEndian.PutUint16(output[20:], pck.packet.dataOffset)
	output[22] = pck.sessionCtx.ACFL0
	output[23] = pck.sessionCtx.ACFL1
	output = append(output, pck.buffer...)
	return output
}

func (pck *AcceptPacket) getPacketType() PacketType {
	return pck.packet.packetType
}

//func NewAcceptPacket(sessionCtx SessionContext, acceptData []byte) *AcceptPacket {
//	sessionCtx.Histone = 1
//	sessionCtx.ACFL0 = 4
//	sessionCtx.ACFL1 = 4
//	pck := AcceptPacket{
//		sessionCtx: sessionCtx,
//		dataOffset: 32,
//		length:        0,
//		packetType:       2,
//		flag:       0,
//		NSPFSID:    0,
//		buffer:     acceptData,
//		SID:        nil,
//	}
//	if len(acceptData) > 230 {
//		pck.length = uint16(len(acceptData)) + pck.dataOffset
//	}
//	return &pck
//}
func newAcceptPacketFromData(packetData []byte) *AcceptPacket {
	if len(packetData) < 32 {
		return nil
	}
	reconAddStart := binary.BigEndian.Uint16(packetData[28:])
	reconAddLen := binary.BigEndian.Uint16(packetData[30:])
	reconAdd := ""
	if reconAddStart != 0 && reconAddLen != 0 && uint16(len(packetData)) > (reconAddStart+reconAddLen) {
		reconAdd = string(packetData[reconAddStart:(reconAddStart + reconAddLen)])
	}
	pck := AcceptPacket{
		packet: Packet{
			dataOffset: binary.BigEndian.Uint16(packetData[20:]),
			length:     binary.BigEndian.Uint16(packetData),
			packetType: PacketType(packetData[4]),
			flag:       packetData[5],
		},
		sessionCtx: SessionContext{
			connOption:          ConnectionOption{},
			SID:                 nil,
			Version:             binary.BigEndian.Uint16(packetData[8:]),
			LoVersion:           0,
			Options:             0,
			NegotiatedOptions:   binary.BigEndian.Uint16(packetData[10:]),
			OurOne:              0,
			Histone:             binary.BigEndian.Uint16(packetData[16:]),
			ReconAddr:           reconAdd,
			ACFL0:               packetData[22],
			ACFL1:               packetData[23],
			SessionDataUnit:     binary.BigEndian.Uint16(packetData[12:]),
			TransportDataUnit:   binary.BigEndian.Uint16(packetData[14:]),
			UsingAsyncReceivers: false,
			IsNTConnected:       false,
			OnBreakReset:        false,
			GotReset:            false,
		},
		buffer: packetData[32:],
	}
	//if pck.length != uint16(len(packetData)) {
	//	return nil
	//}
	//if pck.packetType != ACCEPT {
	//	return nil
	//}
	if pck.packet.dataOffset != 32 {
		return nil
	}
	if binary.BigEndian.Uint16(packetData[18:]) != uint16(len(pck.buffer)) {
		return nil
	}
	return &pck
}

//func (pck *AcceptPacket) SessionCTX() SessionContext {
//	return pck.sessionCtx
//}
