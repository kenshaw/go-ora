package ora

import (
	"errors"

	"github.com/sijms/go-ora/network"
)

type Lob struct {
	sourceLocator []byte
	destLocator   []byte
	scn           []byte
	sourceOffset  int
	destOffset    int
	charsetID     int
	size          int64
	data          []byte
}

func (lob *Lob) getSize(sess *network.Session) (size int64, err error) {
	err = lob.write(sess, 1)
	if err != nil {
		return
	}
	err = lob.read(sess)
	if err != nil {
		return
	}
	size = lob.size
	return
}

func (lob *Lob) getData(sess *network.Session) (data []byte, err error) {
	lob.sourceOffset = 1
	err = lob.write(sess, 2)
	if err != nil {
		return
	}
	err = lob.read(sess)
	if err != nil {
		return
	}
	data = lob.data
	return
}

func (lob *Lob) write(sess *network.Session, operationID int) error {
	sess.ResetBuffer()
	sess.PutBytes(3, 0x60, 0)
	if len(lob.sourceLocator) == 0 {
		sess.PutBytes(0)
	} else {
		sess.PutBytes(1)
	}
	sess.PutUint64(uint64(len(lob.sourceLocator)), 4, true, true)
	if len(lob.destLocator) == 0 {
		sess.PutBytes(0)
	} else {
		sess.PutBytes(1)
	}
	sess.PutUint64(uint64(len(lob.destLocator)), 4, true, true)
	// put offsets
	if sess.TTCVersion < 3 {
		sess.PutUint64(uint64(lob.sourceOffset), 4, true, true)
		sess.PutUint64(uint64(lob.destOffset), 4, true, true)
	} else {
		sess.PutBytes(0, 0)
	}
	if lob.charsetID != 0 {
		sess.PutBytes(1)
	} else {
		sess.PutBytes(0)
	}
	if sess.TTCVersion < 3 {
		sess.PutBytes(1)
	} else {
		sess.PutBytes(0)
	}
	// if bNullO2U (false) {
	// session.PutBytes(1)
	//} else {
	sess.PutBytes(0)
	sess.PutInt64(int64(operationID), 4, true, true)
	if len(lob.scn) == 0 {
		sess.PutBytes(0)
	} else {
		sess.PutBytes(1)
	}
	sess.PutUint64(uint64(len(lob.scn)), 4, true, true)
	if sess.TTCVersion >= 3 {
		sess.PutUint64(uint64(lob.sourceOffset), 8, true, true)
		sess.PutInt64(int64(lob.destOffset), 8, true, true)
		// sendAmount
		sess.PutBytes(1)
	}
	if sess.TTCVersion >= 4 {
		sess.PutBytes(0, 0, 0, 0, 0, 0)
	}
	if len(lob.sourceLocator) > 0 {
		sess.PutBytes(lob.sourceLocator...)
	}
	if len(lob.destLocator) > 0 {
		sess.PutBytes(lob.destLocator...)
	}
	if lob.charsetID != 0 {
		sess.PutUint64(uint64(lob.charsetID), 2, true, true)
	}
	if sess.TTCVersion < 3 {
		sess.PutUint64(uint64(lob.size), 4, true, true)
	}
	for x := 0; x < len(lob.scn); x++ {
		sess.PutUint64(uint64(lob.scn[x]), 4, true, true)
	}
	if sess.TTCVersion >= 3 {
		sess.PutUint64(uint64(lob.size), 8, true, true)
	}
	return sess.Write()
}

func (lob *Lob) read(sess *network.Session) error {
loop:
	for {
		msg, err := sess.GetByte()
		if err != nil {
			return err
		}
		switch msg {
		case 4:
			sess.Summary, err = network.NewSummary(sess)
			if err != nil {
				return err
			}
			if sess.HasError() {
				if sess.Summary.RetCode == 1403 {
					sess.Summary = nil
				} else {
					return errors.New(sess.GetError())
				}
			}
			break loop
		case 8:
			// read rpa message
			if len(lob.sourceLocator) != 0 {
				_, err = sess.GetBytes(len(lob.sourceLocator))
				if err != nil {
					return err
				}
			}
			if len(lob.destLocator) != 0 {
				_, err = sess.GetBytes(len(lob.destLocator))
				if err != nil {
					return err
				}
			}
			if lob.charsetID != 0 {
				lob.charsetID, err = sess.GetInt(2, true, true)
				if err != nil {
					return err
				}
			}
			// get datasize
			if sess.TTCVersion < 3 {
				lob.size, err = sess.GetInt64(4, true, true)
				if err != nil {
					return err
				}
			} else {
				lob.size, err = sess.GetInt64(8, true, true)
				if err != nil {
					return err
				}
			}
		case 9:
			if sess.HasEOSCapability {
				sess.Summary.EndOfCallStatus, err = sess.GetInt(4, true, true)
				if err != nil {
					return err
				}
			}
			break loop
		case 14:
			// get the data
			err = lob.readData(sess)
			if err != nil {
				return err
			}
		default:
			return errors.New("TTC error")
		}
	}
	return nil
}

func (lob *Lob) readData(sess *network.Session) error {
	num1 := 0 // data readed in the call of this function
	var chunkSize byte = 0
	var err error
	// num3 := offset // the data readed from the start of read operation
	num4 := 0
	for num4 != 4 {
		switch num4 {
		case 0:
			chunkSize, err = sess.GetByte()
			if err != nil {
				return err
			}
			if chunkSize == 0xFE {
				num4 = 2
			} else {
				num4 = 1
			}
		case 1:
			chunk, err := sess.GetBytes(int(chunkSize))
			if err != nil {
				return err
			}
			lob.data = append(lob.data, chunk...)
			num1 += int(chunkSize)
			num4 = 4
		case 2:
			chunkSize, err = sess.GetByte()
			if err != nil {
				return err
			}
			if chunkSize <= 0 {
				num4 = 4
			} else {
				num4 = 3
			}
		case 3:
			chunk, err := sess.GetBytes(int(chunkSize))
			if err != nil {
				return err
			}
			lob.data = append(lob.data, chunk...)
			num1 += int(chunkSize)
			// num3 += chunkSize
			num4 = 2
		}
	}
	return nil
}
