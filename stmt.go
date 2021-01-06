package ora

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/sijms/go-ora/conv"
	"github.com/sijms/go-ora/network"
	"github.com/sijms/go-ora/trace"
	// charmap "golang.org/x/text/encoding/charmap"
)

type StmtType int

const (
	StmtSELECT StmtType = 1
	StmtDML    StmtType = 2
	StmtPLQSL  StmtType = 3
	StmtOTHERS StmtType = 4
)

type Stmt struct {
	conn               *Conn
	text               string
	reExec             bool
	reSendParDef       bool
	typ                StmtType
	arrayBindingCount  int
	disableCompression bool
	hasLONG            bool
	hasBLOB            bool
	columns            []Param
	params             []Param
	hasReturnClause    bool
	parse              bool // means parse the command in the server this occur if the stmt is not cached
	execute            bool
	define             bool
	exeOption          int
	cursorID           int
	noOfRowsToFetch    int
	noOfDefCols        int
	al8i4              []byte
	arrayBindCount     int
	queryID            uint64
	scnFromExe         []int
	hasMoreRows        bool
}

type QueryResult struct {
	lastInsertedID int64
	rowsAffected   int64
}

func (rs *QueryResult) LastInsertId() (int64, error) {
	return rs.lastInsertedID, nil
}

func (rs *QueryResult) RowsAffected() (int64, error) {
	return rs.rowsAffected, nil
}

func NewStmt(text string, conn *Conn) *Stmt {
	stmt := &Stmt{
		conn:               conn,
		text:               text,
		reExec:             false,
		reSendParDef:       false,
		hasLONG:            false,
		hasBLOB:            false,
		disableCompression: true,
		arrayBindCount:     1,
		parse:              true,
		execute:            true,
		define:             false,
		al8i4:              make([]byte, 13),
		scnFromExe:         make([]int, 2),
	}
	// get stmt type
	uCmdText := strings.TrimSpace(strings.ToUpper(text))
	if strings.HasPrefix(uCmdText, "SELECT") || strings.HasPrefix(uCmdText, "WITH") {
		stmt.typ = StmtSELECT
	} else if strings.HasPrefix(uCmdText, "UPDATE") ||
		strings.HasPrefix(uCmdText, "INSERT") ||
		strings.HasPrefix(uCmdText, "DELETE") {
		stmt.typ = StmtDML
	} else if strings.HasPrefix(uCmdText, "DECLARE") || strings.HasPrefix(uCmdText, "BEGIN") {
		stmt.typ = StmtPLQSL
	} else {
		stmt.typ = StmtOTHERS
	}
	// returning cluase
	var err error
	stmt.hasReturnClause, err = regexp.MatchString(`\bRETURNING\b`, uCmdText)
	if err != nil {
		stmt.hasReturnClause = false
	}
	//ret.al8i4[0] = 1
	//switch ret.stmtType {
	//case DML:
	//	fallthrough
	//case PLSQL:
	//	if ret.arrayBindCount <= 1 {
	//		ret.al8i4[1] = 1
	//	} else {
	//		ret.al8i4[1] = uint8(ret.arrayBindCount)
	//	}
	//case OTHERS:
	//	ret.al8i4[1] = 1
	//default:
	//	ret.al8i4[1] = 0
	//}
	//if ret.stmtType == SELECT {
	//	ret.al8i4[7] = 1
	//} else {
	//	ret.al8i4[7] = 0
	//}
	return stmt
}

func (stmt *Stmt) write(sess *network.Session) error {
	if !stmt.parse && stmt.typ == StmtDML && !stmt.reSendParDef {
		exeOf := 0
		execFlag := 0
		count := stmt.arrayBindCount
		if stmt.typ == StmtSELECT {
			sess.PutBytes(3, 0x4E, 0)
			count = stmt.noOfRowsToFetch
			exeOf = 0x20
			if stmt.hasReturnClause || stmt.typ == StmtPLQSL || stmt.disableCompression {
				exeOf |= 0x40000
			}
		} else {
			sess.PutBytes(3, 4, 0)
		}
		if stmt.conn.autoCommit {
			execFlag = 1
		}
		sess.PutUint64(uint64(stmt.cursorID), 2, true, true)
		sess.PutUint64(uint64(count), 2, true, true)
		sess.PutUint64(uint64(exeOf), 2, true, true)
		sess.PutUint64(uint64(execFlag), 2, true, true)
	} else {
		exeOp := stmt.getExeOption()
		sess.PutBytes(3, 0x5E, 0)
		sess.PutUint64(uint64(exeOp), 4, true, true)
		sess.PutUint64(uint64(stmt.cursorID), 2, true, true)
		if stmt.cursorID == 0 {
			sess.PutBytes(1)
		} else {
			sess.PutBytes(0)
		}
		if !stmt.parse {
			sess.PutBytes(0, 1)
		} else {
			sess.PutUint64(uint64(len(stmt.conn.strConv.Encode(stmt.text))), 4, true, true)
			sess.PutBytes(1)
		}
		sess.PutUint64(13, 2, true, true)
		sess.PutBytes(0, 0)
		if exeOp&0x40 == 0 && exeOp&0x20 != 0 && exeOp&0x1 != 0 && stmt.typ == StmtSELECT {
			sess.PutBytes(0)
			sess.PutUint64(uint64(stmt.noOfRowsToFetch), 4, true, true)
		} else {
			sess.PutUint64(0, 4, true, true)
			sess.PutUint64(0, 4, true, true)
		}
		// add fetch size = max(int32)
		sess.PutUint64(0x7FFFFFFF, 4, true, true)
		if len(stmt.params) > 0 {
			sess.PutBytes(1)
			sess.PutUint64(uint64(len(stmt.params)), 2, true, true)
		} else {
			sess.PutBytes(0, 0)
		}
		sess.PutBytes(0, 0, 0, 0, 0)
		if stmt.define {
			sess.PutBytes(1)
			sess.PutUint64(uint64(len(stmt.columns)), 2, true, true)
		} else {
			sess.PutBytes(0, 0)
		}
		if sess.TTCVersion >= 4 {
			sess.PutBytes(0, 0, 1)
		}
		if sess.TTCVersion >= 5 {
			sess.PutBytes(0, 0, 0, 0, 0)
		}
		if stmt.parse {
			sess.PutBytes(stmt.conn.strConv.Encode(stmt.text)...)
		}
		if stmt.define {
			sess.PutBytes(0)
			for x := 0; x < len(stmt.columns); x++ {
				stmt.columns[x].Flag = 3
				stmt.columns[x].CharsetForm = 1
				// stmt.columns[x].MaxLen = 0x7fffffff
				err := stmt.columns[x].write(sess)
				if err != nil {
					return err
				}
				sess.PutBytes(0)
			}
		} else {
			if exeOp&1 <= 0 {
				stmt.al8i4[0] = 0
			} else {
				stmt.al8i4[0] = 1
			}
			switch stmt.typ {
			case StmtDML:
				fallthrough
			case StmtPLQSL:
				if stmt.arrayBindCount <= 1 {
					stmt.al8i4[1] = 1
				} else {
					stmt.al8i4[1] = uint8(stmt.arrayBindCount)
				}
			case StmtOTHERS:
				stmt.al8i4[1] = 1
			default:
				stmt.al8i4[1] = 0
			}
			if len(stmt.scnFromExe) == 2 {
				stmt.al8i4[5] = uint8(stmt.scnFromExe[0])
				stmt.al8i4[6] = uint8(stmt.scnFromExe[1])
			} else {
				stmt.al8i4[5] = 0
				stmt.al8i4[6] = 0
			}
			if stmt.typ == StmtSELECT {
				stmt.al8i4[7] = 1
			} else {
				stmt.al8i4[7] = 0
			}
			for x := 0; x < len(stmt.al8i4); x++ {
				sess.PutUint64(uint64(stmt.al8i4[x]), 2, true, true)
			}
		}
		for _, par := range stmt.params {
			_ = par.write(sess)
		}
		// stmt.reExec = true
		stmt.parse = false
		stmt.reSendParDef = false
		stmt.define = false
		// stmt.define = false
	}
	//if !stmt.reExec {
	//exeOp := stmt.getExeOption()
	//session.PutBytes(3, 0x5E, 0)
	//session.PutUint64(exeOp, 4, true, true)
	//session.PutUint64(stmt.cursorID, 2, true, true)
	//if stmt.cursorID == 0 {
	//	session.PutBytes(1)
	//	//session.PutUint64(1, 1, false, false)
	//} else {
	//	session.PutBytes(0)
	//	//session.PutUint64(0, 1, false, false)
	//}
	//session.PutUint64(len(stmt.text), 4, true, true)
	//session.PutBytes(1)
	//session.PutUint64(1, 1, false, false)
	//session.PutUint64(13, 2, true, true)
	//session.PutBytes(0, 0)
	//if exeOp&0x40 == 0 && exeOp&0x20 != 0 && exeOp&0x1 != 0 && stmt.stmtType == SELECT {
	//	session.PutBytes(0)
	//	//session.PutUint64(0, 1, false, false)
	//	session.PutUint64(stmt.noOfRowsToFetch, 4, true, true)
	//} else {
	//	session.PutUint64(0, 4, true, true)
	//	session.PutUint64(0, 4, true, true)
	//}
	// longFetchSize == 0 marshal 1 else marshal longFetchSize
	//session.PutUint64(1, 4, true, true)
	//if len(stmt.Pars) > 0 {
	//	session.PutBytes(1)
	//	//session.PutUint64(1, 1, false, false)
	//	session.PutUint64(len(stmt.Pars), 2, true, true)
	//} else {
	//	session.PutBytes(0, 0)
	//	//session.PutUint64(0, 1, false, false)
	//	//session.PutUint64(0, 1, false, false)
	//}
	//session.PutBytes(0, 0, 0, 0, 0)
	//if stmt.define {
	//	session.PutBytes(1)
	//	//session.PutUint64(1, 1, false, false)
	//	session.PutUint64(stmt.noOfDefCols, 2, true, true)
	//} else {
	//	session.PutBytes(0, 0)
	//	//session.PutUint64(0, 1, false, false)
	//	//session.PutUint64(0, 1, false, false)
	//}
	//if session.TTCVersion >= 4 {
	//	session.PutBytes(0, 0, 1)
	//	//session.PutUint64(0, 1, false, false) // dbChangeRegisterationId
	//	//session.PutUint64(0, 1, false, false)
	//	//session.PutUint64(1, 1, false, false)
	//}
	//if session.TTCVersion >= 5 {
	//	session.PutBytes(0, 0, 0, 0, 0)
	//	//session.PutUint64(0, 1, false, false)
	//	//session.PutUint64(0, 1, false, false)
	//	//session.PutUint64(0, 1, false, false)
	//	//session.PutUint64(0, 1, false, false)
	//	//session.PutUint64(0, 1, false, false)
	//}
	//session.PutBytes([]byte(stmt.text)...)
	//for x := 0; x < len(stmt.al8i4); x++ {
	//	session.PutUint64(stmt.al8i4[x], 2, true, true)
	//}
	//for _, par := range stmt.Pars {
	//	_ = par.write(session)
	//}
	//stmt.reExec = true
	//stmt.parse = false
	//} else {
	//
	//}
	if len(stmt.params) > 0 {
		for x := 0; x < stmt.arrayBindCount; x++ {
			sess.PutBytes(7)
			for _, par := range stmt.params {
				if par.DataType != TypeRAW {
					sess.PutClr(par.Value)
				}
			}
			for _, par := range stmt.params {
				if par.DataType == TypeRAW {
					sess.PutClr(par.Value)
				}
			}
		}
		//session.PutUint64(7, 1, false, false)
		//for _, par := range stmt.Pars {
		//	session.PutClr(par.Value)
		//}
	}
	return sess.Write()
}

func (stmt *Stmt) getExeOption() int {
	op := 0
	if stmt.typ == StmtPLQSL || stmt.hasReturnClause {
		op |= 0x40000
	}
	if stmt.arrayBindCount > 1 {
		op |= 0x80000
	}
	if stmt.conn.autoCommit && stmt.typ == StmtDML {
		op |= 0x100
	}
	if stmt.parse {
		op |= 1
	}
	if stmt.execute {
		op |= 0x20
	}
	if !stmt.parse && !stmt.execute {
		op |= 0x40
	}
	if len(stmt.params) > 0 {
		op |= 0x8
		if stmt.typ == StmtPLQSL || stmt.hasReturnClause {
			op |= 0x400
		}
	}
	if stmt.typ != StmtPLQSL && !stmt.hasReturnClause {
		op |= 0x8000
	}
	if stmt.define {
		op |= 0x10
	}
	return op
	/* HasReturnClause
	if  stmt.PLSQL or cmdText == "" return false
	Regex.IsMatch(cmdText, "\\bRETURNING\\b"
	*/
}

func (stmt *Stmt) fetch(res *Result) error {
	stmt.conn.sess.ResetBuffer()
	stmt.conn.sess.PutBytes(3, 5, 0)
	stmt.conn.sess.PutInt64(int64(stmt.cursorID), 2, true, true)
	stmt.conn.sess.PutInt64(int64(stmt.noOfRowsToFetch), 2, true, true)
	err := stmt.conn.sess.Write()
	if err != nil {
		return err
	}
	return stmt.read(res)
}

func (stmt *Stmt) read(res *Result) error {
	loop := true
	after7 := false
	containOutputPars := false
	res.parent = stmt
	sess := stmt.conn.sess
	for loop {
		msg, err := sess.GetByte()
		if err != nil {
			return err
		}
		switch msg {
		case 4:
			stmt.conn.sess.Summary, err = network.NewSummary(sess)
			if err != nil {
				return err
			}
			stmt.conn.connOption.Tracer.Printf("Summary: RetCode:%d, Error Message:%q", stmt.conn.sess.Summary.RetCode, string(stmt.conn.sess.Summary.ErrorMessage))
			stmt.cursorID = stmt.conn.sess.Summary.CursorID
			stmt.disableCompression = stmt.conn.sess.Summary.Flags&0x20 != 0
			if stmt.conn.sess.HasError() {
				if stmt.conn.sess.Summary.RetCode == 1403 {
					stmt.hasMoreRows = false
					stmt.conn.sess.Summary = nil
				} else {
					return errors.New(stmt.conn.sess.GetError())
				}
			}
			loop = false
		case 6:
			//_, err = session.GetByte()
			err = res.read(sess)
			if err != nil {
				return err
			}
			if !after7 {
				if stmt.typ == StmtSELECT {
				}
			}
		case 7:
			after7 = true
			if stmt.hasReturnClause {
				//if (bHasReturningParams && bindAccessors != null)
				//{
				//	int paramLen = bindAccessors.Length;
				//	this.m_marshallingEngine.m_oraBufRdr.m_bHavingParameterData = true;
				//	for (int index1 = 0; index1 < paramLen; ++index1)
				//	{
				//		if (bindAccessors[index1] != null)
				//		{
				//			int num = (int) this.m_marshallingEngine.UnmarshalUB4(false);
				//			if (num > 1)
				//				bMoreThanOneRowAffectedByDmlWithRetClause = true;
				//			if (num == 0)
				//			{
				//				bindAccessors[index1].AddNullForData();
				//			}
				//			else
				//			{
				//				for (int index2 = 0; index2 < num; ++index2)
				//				{
				//					bindAccessors[index1].m_bReceivedOutValueFromServer = true;
				//					bindAccessors[index1].UnmarshalOneRow();
				//				}
				//			}
				//		}
				//	}
				//	this.m_marshallingEngine.m_oraBufRdr.m_currentOB = (OraBuf) null;
				//	this.m_marshallingEngine.m_oraBufRdr.m_bHavingParameterData = false;
				//	++noOfRowsFetched;
				//	continue;
				//}
			} else {
				if containOutputPars {
					for x := 0; x < len(stmt.columns); x++ {
						if stmt.params[x].Type != BindInput {
							stmt.params[x].Value, err = sess.GetClr()
						} else {
							_, err = sess.GetClr()
						}
						if err != nil {
							return err
						}
						_, err = sess.GetInt(2, true, true)
					}
				} else {
					// see if it is re-execute
					// fmt.Println(res.Cols)
					if len(res.Cols) == 0 && len(stmt.columns) > 0 {
						res.Cols = make([]Param, len(stmt.columns))
						copy(res.Cols, stmt.columns)
					}
					for x := 0; x < len(res.Cols); x++ {
						if res.Cols[x].GetDataFromServer {
							temp, err := sess.GetClr()
							// fmt.Println("buffer: ", temp)
							if err != nil {
								return err
							}
							if temp == nil {
								res.currentRow[x] = nil
								if res.Cols[x].DataType == TypeLONG || res.Cols[x].DataType == TypeLongRaw {
									_, err = sess.GetBytes(2)
									if err != nil {
										return err
									}
									_, err = sess.GetInt(4, true, true)
									if err != nil {
										return err
									}
								}
							} else {
								//switch (this.m_definedColumnType)
								//{
								//case OraType.ORA_TIMESTAMP_DTY:
								//case OraType.ORA_TIMESTAMP:
								//case OraType.ORA_TIMESTAMP_LTZ_DTY:
								//case OraType.ORA_TIMESTAMP_LTZ:
								//	this.m_marshallingEngine.UnmarshalCLR_ColData(11);
								//	break;
								//case OraType.ORA_TIMESTAMP_TZ_DTY:
								//case OraType.ORA_TIMESTAMP_TZ:
								//	this.m_marshallingEngine.UnmarshalCLR_ColData(13);
								//	break;
								//case OraType.ORA_INTERVAL_YM_DTY:
								//case OraType.ORA_INTERVAL_DS_DTY:
								//case OraType.ORA_INTERVAL_YM:
								//case OraType.ORA_INTERVAL_DS:
								//case OraType.ORA_IBFLOAT:
								//case OraType.ORA_IBDOUBLE:
								//case OraType.ORA_RAW:
								//case OraType.ORA_CHAR:
								//case OraType.ORA_CHARN:
								//case OraType.ORA_VARCHAR:
								//	this.m_marshallingEngine.UnmarshalCLR_ColData(this.m_colMetaData.m_maxLength);
								//	break;
								//case OraType.ORA_RESULTSET:
								//	throw new InvalidOperationException();
								//case OraType.ORA_NUMBER:
								//case OraType.ORA_FLOAT:
								//case OraType.ORA_VARNUM:
								//	this.m_marshallingEngine.UnmarshalCLR_ColData(21);
								//	break;
								//case OraType.ORA_DATE:
								//	this.m_marshallingEngine.UnmarshalCLR_ColData(7);
								//	break;
								//default:
								//	throw new Exception("UnmarshalColumnData: Unimplemented type");
								//}
								//fmt.Println("type: ", res.Cols[x].DataType)
								switch res.Cols[x].DataType {
								case TypeNCHAR, TypeCHAR, TypeLONG:
									if stmt.conn.strConv.LangID != res.Cols[x].CharsetID {
										tempCharset := stmt.conn.strConv.LangID
										stmt.conn.strConv.LangID = res.Cols[x].CharsetID
										res.currentRow[x] = stmt.conn.strConv.Decode(temp)
										stmt.conn.strConv.LangID = tempCharset
									} else {
										res.currentRow[x] = stmt.conn.strConv.Decode(temp)
									}
								case TypeNUMBER:
									res.currentRow[x] = conv.DecodeNumber(temp)
									// if res.Cols[x].Scale == 0 {
									// 	res.currentRow[x] = int64(conv.DecodeInt(temp))
									// } else {
									// 	res.currentRow[x] = conv.DecodeDouble(temp)
									// 	//base := math.Pow10(int(res.Cols[x].Scale))
									// 	//if res.Cols[x].Scale < 0x80 {
									// 	//	res.currentRow[x] = math.Round(conv.DecodeDouble(temp)*base) / base
									// 	//} else {
									// 	//	res.currentRow[x] = conv.DecodeDouble(temp)
									// 	//}
									// }
								case TypeTimestamp:
									fallthrough
								case TypeTimestampDTY:
									fallthrough
								case TypeTimestampLTZ:
									fallthrough
								case TypeTimestampLTZ_DTY:
									fallthrough
								case TypeTimestampTZ:
									fallthrough
								case TypeTimestampTZ_DTY:
									fallthrough
								case TypeDATE:
									dateVal, err := conv.DecodeDate(temp)
									if err != nil {
										return err
									}
									res.currentRow[x] = dateVal
								//case :
								//	data, err := session.GetClr()
								//	if err != nil {
								//		return err
								//	}
								//	lob := &Lob{
								//		sourceLocator: data,
								//	}
								//	session.SaveState()
								//	dataSize, err := lob.getSize(session)
								//	if err != nil {
								//		return err
								//	}
								//	lobData, err := lob.getData(session)
								//	if err != nil {
								//		return err
								//	}
								//	if dataSize != int64(len(lobData)) {
								//		return errors.New("error reading lob data")
								//	}
								//	session.LoadState()
								//
								case TypeOCIBlobLocator, TypeOCIClobLocator:
									data, err := sess.GetClr()
									if err != nil {
										return err
									}
									lob := &Lob{
										sourceLocator: data,
									}
									sess.SaveState()
									dataSize, err := lob.getSize(sess)
									if err != nil {
										return err
									}
									lobData, err := lob.getData(sess)
									if err != nil {
										return err
									}
									if dataSize != int64(len(lobData)) {
										return errors.New("error reading lob data")
									}
									sess.LoadState()
									if res.Cols[x].DataType == TypeOCIBlobLocator {
										res.currentRow[x] = lobData
									} else {
										if stmt.conn.strConv.LangID != res.Cols[x].CharsetID {
											tempCharset := stmt.conn.strConv.LangID
											stmt.conn.strConv.LangID = res.Cols[x].CharsetID
											res.currentRow[x] = stmt.conn.strConv.Decode(lobData)
											stmt.conn.strConv.LangID = tempCharset
										} else {
											res.currentRow[x] = stmt.conn.strConv.Decode(lobData)
										}
									}
								default:
									res.currentRow[x] = temp
								}
								if res.Cols[x].DataType == TypeLONG || res.Cols[x].DataType == TypeLongRaw {
									_, err = sess.GetInt(4, true, true)
									if err != nil {
										return err
									}
									_, err = sess.GetInt(4, true, true)
									if err != nil {
										return err
									}
								}
							}
						}
					}
					newRow := make(Row, res.ColumnCount)
					copy(newRow, res.currentRow)
					res.Rows = append(res.Rows, newRow)
				}
			}
		case 8:
			size, err := sess.GetInt(2, true, true)
			if err != nil {
				return err
			}
			for x := 0; x < 2; x++ {
				stmt.scnFromExe[x], err = sess.GetInt(4, true, true)
				if err != nil {
					return err
				}
			}
			for x := 2; x < size; x++ {
				_, err = sess.GetInt(4, true, true)
				if err != nil {
					return err
				}
			}
			_, err = sess.GetInt(2, true, true)
			// fmt.Println(num)
			// if (num > 0)
			//	this.m_marshallingEngine.UnmarshalNBytes_ScanOnly(num);
			// get session timezone
			size, err = sess.GetInt(2, true, true)
			for x := 0; x < size; x++ {
				_, val, num, err := sess.GetKeyVal()
				if err != nil {
					return err
				}
				// fmt.Println(key, val, num)
				if num == 163 {
					sess.TimeZone = val
					// fmt.Println("session time zone", session.TimeZone)
				}
			}
			if sess.TTCVersion >= 4 {
				// get queryID
				size, err = sess.GetInt(4, true, true)
				if err != nil {
					return err
				}
				if size > 0 {
					bty, err := sess.GetBytes(size)
					if err != nil {
						return err
					}
					if len(bty) >= 8 {
						stmt.queryID = binary.LittleEndian.Uint64(bty[size-8:])
						fmt.Println("query ID: ", stmt.queryID)
					}
				}
			}
		case 11:
			err = res.read(sess)
			if err != nil {
				return err
			}
			// res.BindDirections = make([]byte, res.ColumnCount)
			for x := 0; x < res.ColumnCount; x++ {
				direction, err := sess.GetByte()
				switch direction {
				case 32:
					stmt.params[x].Type = BindInput
				case 16:
					stmt.params[x].Type = BindOutput
					containOutputPars = true
				case 48:
					stmt.params[x].Type = BindInputOutput
					containOutputPars = true
				}
				if err != nil {
					return err
				}
			}
		case 16:
			size, err := sess.GetByte()
			if err != nil {
				return err
			}
			_, err = sess.GetBytes(int(size))
			if err != nil {
				return err
			}
			res.MaxRowSize, err = sess.GetInt(4, true, true)
			if err != nil {
				return err
			}
			res.ColumnCount, err = sess.GetInt(4, true, true)
			if err != nil {
				return err
			}
			if res.ColumnCount > 0 {
				_, err = sess.GetByte() // session.GetInt(1, false, false)
			}
			res.Cols = make([]Param, res.ColumnCount)
			for x := 0; x < res.ColumnCount; x++ {
				err = res.Cols[x].read(sess)
				if err != nil {
					return err
				}
				if res.Cols[x].DataType == TypeLONG || res.Cols[x].DataType == TypeLongRaw {
					stmt.hasLONG = true
				}
				if res.Cols[x].DataType == TypeOCIClobLocator || res.Cols[x].DataType == TypeOCIBlobLocator {
					stmt.hasBLOB = true
				}
			}
			stmt.columns = make([]Param, res.ColumnCount)
			copy(stmt.columns, res.Cols)
			_, err = sess.GetDlc()
			if sess.TTCVersion >= 3 {
				_, err = sess.GetInt(4, true, true)
				_, err = sess.GetInt(4, true, true)
			}
			if sess.TTCVersion >= 4 {
				_, err = sess.GetInt(4, true, true)
				_, err = sess.GetInt(4, true, true)
			}
			if sess.TTCVersion >= 5 {
				_, err = sess.GetDlc()
			}
		case 21:
			_, err := sess.GetInt(2, true, true) // noOfColumnSent
			if err != nil {
				return err
			}
			bitVectorLen := res.ColumnCount / 8
			if res.ColumnCount%8 > 0 {
				bitVectorLen++
			}
			bitVector := make([]byte, bitVectorLen)
			for x := 0; x < bitVectorLen; x++ {
				bitVector[x], err = sess.GetByte()
				if err != nil {
					return err
				}
			}
			res.setBitVector(bitVector)
		default:
			loop = false
		}
	}
	if stmt.conn.connOption.Tracer.IsOn() {
		res.Trace(stmt.conn.connOption.Tracer)
	}
	return nil
}

func (stmt *Stmt) Close() error {
	session := stmt.conn.sess
	session.ResetBuffer()
	session.PutBytes(17, 105, 0, 1, 1, 1)
	session.PutInt64(int64(stmt.cursorID), 4, true, true)
	return (&simpleObject{
		session:     session,
		operationID: 0x93,
		data:        nil,
		err:         nil,
	}).write().read()
}

func (stmt *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	stmt.conn.connOption.Tracer.Printf("Exec:\n%s", stmt.text)
	for x := 0; x < len(args); x++ {
		par := *stmt.NewParam("", args[x], 0, BindInput)
		if x < len(stmt.params) {
			if par.MaxLen > stmt.params[x].MaxLen {
				stmt.reSendParDef = true
			}
			stmt.params[x] = par
		} else {
			stmt.params = append(stmt.params, par)
		}
		stmt.conn.connOption.Tracer.Printf("    %d:\n%v", x, args[x])
	}
	sess := stmt.conn.sess
	//if len(args) > 0 {
	//	stmt.Pars = nil
	//}
	//for x := 0; x < len(args); x++ {
	//	stmt.AddParam("", args[x], 0, Input)
	//}
	sess.ResetBuffer()
	err := stmt.write(sess)
	if err != nil {
		return nil, err
	}
	res := new(Result)
	err = stmt.read(res)
	if err != nil {
		return nil, err
	}
	result := new(QueryResult)
	if sess.Summary != nil {
		result.rowsAffected = int64(sess.Summary.CurRowNumber)
	}
	return result, nil
}

func (stmt *Stmt) NewParam(name string, val driver.Value, size int, direction BindType) *Param {
	param := &Param{
		Name:        name,
		Type:        direction,
		Flag:        3,
		CharsetID:   871,
		CharsetForm: 1,
	}
	if val == nil {
		param.DataType = TypeNCHAR
		param.Value = nil
		param.ContFlag = 16
		param.MaxCharLen = 0
		param.MaxLen = 1
		param.CharsetForm = 1
	} else {
		switch val := val.(type) {
		case int64:
			param.Value = conv.EncodeInt64(val)
			param.DataType = TypeNUMBER
		case int32:
			param.Value = conv.EncodeInt(int(val))
			param.DataType = TypeNUMBER
		case int16:
			param.Value = conv.EncodeInt(int(val))
			param.DataType = TypeNUMBER
		case int8:
			param.Value = conv.EncodeInt(int(val))
			param.DataType = TypeNUMBER
		case int:
			param.Value = conv.EncodeInt(val)
			param.DataType = TypeNUMBER
		case float32:
			param.Value, _ = conv.EncodeDouble(float64(val))
			param.DataType = TypeNUMBER
		case float64:
			param.Value, _ = conv.EncodeDouble(val)
			param.DataType = TypeNUMBER
		case time.Time:
			param.Value = conv.EncodeDate(val)
			param.DataType = TypeDATE
			param.ContFlag = 0
			param.MaxLen = 11
			param.MaxCharLen = 11
		case string:
			param.DataType = TypeNCHAR
			param.ContFlag = 16
			param.MaxCharLen = len(val)
			param.CharsetForm = 1
			if string(val) == "" && direction == BindInput {
				param.Value = nil
				param.MaxLen = 1
			} else {
				param.Value = stmt.conn.strConv.Encode(val)
				if size > len(val) {
					param.MaxCharLen = size
				}
				param.MaxLen = param.MaxCharLen * conv.MaxBytePerChar(stmt.conn.strConv.LangID)
			}
		case []byte:
			param.Value = val
			param.DataType = TypeRAW
			param.MaxLen = len(val)
			param.ContFlag = 0
			param.MaxCharLen = 0
			param.CharsetForm = 0
		}
		if param.DataType == TypeNUMBER {
			param.ContFlag = 0
			param.MaxCharLen = 0
			param.MaxLen = 22
			param.CharsetForm = 0
		}
		if direction == BindOutput {
			param.Value = nil
		}
	}
	return param
}

func (stmt *Stmt) AddParam(name string, val driver.Value, size int, direction BindType) {
	stmt.params = append(stmt.params, *stmt.NewParam(name, val, size, direction))
}

//func (stmt *Stmt) AddParam(name string, val driver.Value, size int, direction ParameterDirection) {
//	param := ParameterInfo{
//		Name:        name,
//		Direction:   direction,
//		Flag:        3,
//		CharsetID:   871,
//		CharsetForm: 1,
//	}
//	//if param.Direction == Output {
//	//	if _, ok := val.(string); ok {
//	//		param.MaxCharLen = size
//	//		param.MaxLen = size * conv.MaxBytePerChar(stmt.connection.strConv.LangID)
//	//	}
//	//	stmt.Pars = append(stmt.Pars, param)
//	//	return
//	//}
//	if val == nil {
//		param.DataType = NCHAR
//		param.Value = nil
//		param.ContFlag = 16
//		param.MaxCharLen = 0
//		param.MaxLen = 1
//		param.CharsetForm = 1
//	} else {
//		switch val := val.(type) {
//		case int64:
//			param.Value = conv.EncodeInt64(val)
//			param.DataType = NUMBER
//		case int32:
//			param.Value = conv.EncodeInt(int(val))
//			param.DataType = NUMBER
//		case int16:
//			param.Value = conv.EncodeInt(int(val))
//			param.DataType = NUMBER
//		case int8:
//			param.Value = conv.EncodeInt(int(val))
//			param.DataType = NUMBER
//		case int:
//			param.Value = conv.EncodeInt(val)
//			param.DataType = NUMBER
//		case float32:
//			param.Value, _ = conv.EncodeDouble(float64(val))
//			param.DataType = NUMBER
//		case float64:
//			param.Value, _ = conv.EncodeDouble(val)
//			param.DataType = NUMBER
//		case time.Time:
//			param.Value = conv.EncodeDate(val)
//			param.DataType = DATE
//			param.ContFlag = 0
//			param.MaxLen = 11
//			param.MaxCharLen = 11
//		case string:
//			param.Value = stmt.connection.strConv.Encode(val)
//			param.DataType = NCHAR
//			param.ContFlag = 16
//			param.MaxCharLen = len(val)
//			if size > len(val) {
//				param.MaxCharLen = size
//			}
//			param.MaxLen = param.MaxCharLen * conv.MaxBytePerChar(stmt.connection.strConv.LangID)
//			param.CharsetForm = 1
//		}
//		if param.DataType == NUMBER {
//			param.ContFlag = 0
//			param.MaxCharLen = 22
//			param.MaxLen = 22
//			param.CharsetForm = 1
//		}
//		if direction == Output {
//			param.Value = nil
//		}
//	}
//	stmt.Pars = append(stmt.Pars, param)
//}
//func (stmt *Stmt) reExec() (driver.Rows, error) {
//
//}
func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	stmt.conn.connOption.Tracer.Printf("Query:\n%s", stmt.text)
	stmt.noOfRowsToFetch = 25
	stmt.hasMoreRows = true
	for x := 0; x < len(args); x++ {
		par := *stmt.NewParam("", args[x], 0, BindInput)
		if x < len(stmt.params) {
			if par.MaxLen > stmt.params[x].MaxLen {
				stmt.reSendParDef = true
			}
			stmt.params[x] = par
		} else {
			stmt.params = append(stmt.params, par)
		}
	}
	//stmt.Pars = nil
	//for x := 0; x < len(args); x++ {
	//	stmt.AddParam()
	//}
	stmt.conn.sess.ResetBuffer()
	// if re-execute
	err := stmt.write(stmt.conn.sess)
	if err != nil {
		return nil, err
	}
	//err = stmt.connection.session.Write()
	//if err != nil {
	//	return nil, err
	//}
	res := new(Result)
	err = stmt.read(res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (stmt *Stmt) NumInput() int {
	return -1
}

/*
parse = true
execute = true
fetch = true if hasReturn or PLSQL
define = false
*/

// Compile time Sentinels for implemented Interfaces.
var (
	_ = driver.Rows((*Result)(nil))
	_ = driver.RowsColumnTypeDatabaseTypeName((*Result)(nil))
	_ = driver.RowsColumnTypeLength((*Result)(nil))
	_ = driver.RowsColumnTypeNullable((*Result)(nil))
)

// var _ = driver.RowsColumnTypePrecisionScale((*Result)(nil))
// var _ = driver.RowsColumnTypeScanType((*Result)(nil))
// var _ = driver.RowsNextResultSet((*Result)(nil))
type Row []driver.Value

type Result struct {
	ColumnCount     int
	RowCount        int
	UACBufferLength int
	MaxRowSize      int
	Cols            []Param
	Rows            []Row
	currentRow      Row
	index           int
	parent          *Stmt
}

func (res *Result) read(session *network.Session) error {
	_, err := session.GetByte()
	if err != nil {
		return err
	}
	columnCount, err := session.GetInt(2, true, true)
	if err != nil {
		return err
	}
	num, err := session.GetInt(4, true, true)
	if err != nil {
		return err
	}
	columnCount += num * 0x100
	if columnCount > res.ColumnCount {
		res.ColumnCount = columnCount
	}
	if len(res.currentRow) != res.ColumnCount {
		res.currentRow = make(Row, res.ColumnCount)
	}
	res.RowCount, err = session.GetInt(4, true, true)
	if err != nil {
		return err
	}
	res.UACBufferLength, err = session.GetInt(2, true, true)
	if err != nil {
		return err
	}
	bitVector, err := session.GetDlc()
	if err != nil {
		return err
	}
	res.setBitVector(bitVector)
	_, err = session.GetDlc()
	return nil
}

func (res *Result) setBitVector(bitVector []byte) {
	index := res.ColumnCount / 8
	if res.ColumnCount%8 > 0 {
		index++
	}
	if len(bitVector) > 0 {
		for x := 0; x < len(bitVector); x++ {
			for i := 0; i < 8; i++ {
				if (x*8)+i < res.ColumnCount {
					res.Cols[(x*8)+i].GetDataFromServer = bitVector[x]&(1<<i) > 0
				}
			}
		}
	} else {
		for x := 0; x < len(res.Cols); x++ {
			res.Cols[x].GetDataFromServer = true
		}
	}
}

func (res *Result) Close() error {
	return nil
}

func (res *Result) Next(dest []driver.Value) error {
	//fmt.Println("has more row: ", res.parent.hasMoreRows)
	//fmt.Println("row length: ", len(res.Rows))
	//fmt.Println("cursor id: ", res.parent.cursorID)
	//if res.parent.hasMoreRows && res.index == len(res.Rows) && len(res.Rows) < res.parent.noOfRowsToFetch {
	//	fmt.Println("inside first fetch")
	//	oldFetchCount := res.parent.noOfRowsToFetch;
	//	res.parent.noOfRowsToFetch = oldFetchCount - len(res.Rows)
	//	err := res.parent.fetch(res)
	//	if err != nil {
	//		return err
	//	}
	//	res.parent.noOfRowsToFetch = oldFetchCount
	//	fmt.Println("row count after first fetch: ", len(res.Rows))
	//}
	if res.parent.hasMoreRows && res.index != 0 && res.index%res.parent.noOfRowsToFetch == 0 {
		res.Rows = make([]Row, 0, res.parent.noOfRowsToFetch)
		err := res.parent.fetch(res)
		if err != nil {
			return err
		}
	}
	if res.parent.hasMoreRows && (res.parent.hasBLOB || res.parent.hasLONG) && res.index == 0 {
		if err := res.parent.fetch(res); err != nil {
			return err
		}
	}
	if res.index%res.parent.noOfRowsToFetch < len(res.Rows) {
		for x := 0; x < len(res.Rows[res.index%res.parent.noOfRowsToFetch]); x++ {
			dest[x] = res.Rows[res.index%res.parent.noOfRowsToFetch][x]
		}
		res.index++
		return nil
	}
	return io.EOF
}

func (res *Result) Columns() []string {
	if len(res.Cols) == 0 {
		return nil
	}
	ret := make([]string, len(res.Cols))
	for x := 0; x < len(res.Cols); x++ {
		ret[x] = res.Cols[x].Name
	}
	return ret
}

func (res Result) Trace(t trace.Tracer) {
	for r, row := range res.Rows {
		if r > 25 {
			break
		}
		t.Printf("Row %d", r)
		for c, col := range res.Cols {
			t.Printf("  %-20s: %v", col.Name, row[c])
		}
	}
}

func (res Result) ColumnTypeDatabaseTypeName(index int) string {
	return res.Cols[index].DataType.String()
}

func (res Result) ColumnTypeLength(index int) (length int64, ok bool) {
	switch res.Cols[index].DataType {
	case TypeNCHAR, TypeCHAR:
		return int64(res.Cols[index].MaxCharLen), true
	case TypeNUMBER:
		return int64(res.Cols[index].Precision), true
	}
	return int64(0), false
}

func (res Result) ColumnTypeNullable(index int) (nullable, ok bool) {
	return res.Cols[index].AllowNull, true
}

type Transaction struct {
	conn *Conn
}

func (tx *Transaction) Commit() error {
	tx.conn.autoCommit = true
	tx.conn.sess.ResetBuffer()
	return (&simpleObject{session: tx.conn.sess, operationID: 0xE}).write().read()
}

func (tx *Transaction) Rollback() error {
	tx.conn.autoCommit = true
	tx.conn.sess.ResetBuffer()
	return (&simpleObject{session: tx.conn.sess, operationID: 0xF}).write().read()
}

type simpleObject struct {
	session     *network.Session
	operationID uint8
	data        []byte
	err         error
}

func (obj *simpleObject) write() *simpleObject {
	// obj.session.ResetBuffer()
	obj.session.PutBytes(3, obj.operationID, 0)
	if obj.data != nil {
		obj.session.PutBytes(obj.data...)
	}
	obj.err = obj.session.Write()
	return obj
}

func (obj *simpleObject) read() error {
	if obj.err != nil {
		return obj.err
	}
	loop := true
	for loop {
		msg, err := obj.session.GetByte()
		if err != nil {
			return err
		}
		switch msg {
		case 4:
			obj.session.Summary, err = network.NewSummary(obj.session)
			if err != nil {
				return err
			}
			loop = false
		case 9:
			if obj.session.HasEOSCapability {
				if obj.session.Summary == nil {
					obj.session.Summary = new(network.SummaryObject)
				}
				obj.session.Summary.EndOfCallStatus, err = obj.session.GetInt(4, true, true)
				if err != nil {
					return err
				}
			}
			if obj.session.HasFSAPCapability {
				if obj.session.Summary == nil {
					obj.session.Summary = new(network.SummaryObject)
				}
				obj.session.Summary.EndToEndECIDSequence, err = obj.session.GetInt(2, true, true)
				if err != nil {
					return err
				}
			}
			loop = false
		default:
			return fmt.Errorf("message code error: received code %d and expected code is 4, 9", msg)
		}
	}
	if obj.session.HasError() {
		return errors.New(obj.session.GetError())
	}
	return nil
}

// BindType
type BindType int

const (
	BindInput       BindType = 1
	BindOutput      BindType = 2
	BindInputOutput BindType = 3
	BindReturn      BindType = 9
)

//internal enum BindDirection
//{
//Output = 16,
//Input = 32,
//InputOutput = 48,
//}

type ParamType int

const (
	ParamNumber ParamType = 1
	ParamString ParamType = 2
)

type Param struct {
	Name                 string
	Type                 BindType
	IsNull               bool
	AllowNull            bool
	ColumnAlias          string
	DataType             Type
	IsXmlType            bool
	Flag                 uint8
	Precision            uint8
	Scale                uint8
	MaxLen               int
	MaxCharLen           int
	MaxNoOfArrayElements int
	ContFlag             int
	ToID                 []byte
	Version              int
	CharsetID            int
	CharsetForm          int
	Value                []byte
	GetDataFromServer    bool
}

func (p *Param) read(sess *network.Session) error {
	p.GetDataFromServer = true
	dataType, err := sess.GetByte()
	if err != nil {
		return err
	}
	p.DataType = Type(dataType)
	p.Flag, err = sess.GetByte()
	if err != nil {
		return err
	}
	p.Precision, err = sess.GetByte()
	// precision, err := session.GetInt(1, false, false)
	// var scale int
	switch p.DataType {
	case TypeNUMBER:
		fallthrough
	case TypeTimestampDTY:
		fallthrough
	case TypeTimestampTZ_DTY:
		fallthrough
	case TypeIntervalDS_DTY:
		fallthrough
	case TypeTimestamp:
		fallthrough
	case TypeTimestampTZ:
		fallthrough
	case TypeIntervalDS:
		fallthrough
	case TypeTimestampLTZ_DTY:
		fallthrough
	case TypeTimestampLTZ:
		if scale, err := sess.GetInt(2, true, true); err != nil {
			return err
		} else {
			if scale == -127 {
				p.Precision = uint8(math.Ceil(float64(p.Precision) * 0.30103))
				p.Scale = 0xff
			} else {
				p.Scale = uint8(scale)
			}
		}
	default:
		p.Scale, err = sess.GetByte()
		// scale, err = session.GetInt(1, false, false)
	}
	//if par.Scale == uint8(-127) {
	//
	//}
	if p.DataType == TypeNUMBER && p.Precision == 0 && (p.Scale == 0 || p.Scale == 0xFF) {
		p.Precision = 38
		p.Scale = 0xff
	}
	// par.Scale = uint16(scale)
	// par.Precision = uint16(precision)
	p.MaxLen, err = sess.GetInt(4, true, true)
	if err != nil {
		return err
	}
	switch p.DataType {
	case TypeROWID:
		p.MaxLen = 128
	case TypeDATE:
		p.MaxLen = 7
	case TypeIBFloat:
		p.MaxLen = 4
	case TypeIBDouble:
		p.MaxLen = 8
	case TypeTimestampTZ_DTY:
		p.MaxLen = 13
	case TypeIntervalYM_DTY:
		fallthrough
	case TypeIntervalDS_DTY:
		fallthrough
	case TypeIntervalYM:
		fallthrough
	case TypeIntervalDS:
		p.MaxLen = 11
	}
	p.MaxNoOfArrayElements, err = sess.GetInt(4, true, true)
	if err != nil {
		return err
	}
	p.ContFlag, err = sess.GetInt(4, true, true)
	if err != nil {
		return err
	}
	p.ToID, err = sess.GetDlc()
	p.Version, err = sess.GetInt(2, true, true)
	if err != nil {
		return err
	}
	p.CharsetID, err = sess.GetInt(2, true, true)
	if err != nil {
		return err
	}
	p.CharsetForm, err = sess.GetInt(1, false, false)
	if err != nil {
		return err
	}
	p.MaxCharLen, err = sess.GetInt(4, true, true)
	if err != nil {
		return err
	}
	num1, err := sess.GetInt(1, false, false)
	if err != nil {
		return err
	}
	p.AllowNull = num1 > 0
	_, err = sess.GetByte() //  session.GetInt(1, false, false)
	if err != nil {
		return err
	}
	bName, err := sess.GetDlc()
	if err != nil {
		return err
	}
	p.Name = string(bName)
	_, err = sess.GetDlc()
	bName, err = sess.GetDlc()
	if err != nil {
		return err
	}
	if strings.ToUpper(string(bName)) == "XMLTYPE" {
		p.DataType = TypeXMLType
		p.IsXmlType = true
	}
	if sess.TTCVersion < 3 {
		return nil
	}
	_, err = sess.GetInt(2, true, true)
	if sess.TTCVersion < 6 {
		return nil
	}
	_, err = sess.GetInt(4, true, true)
	return nil
}

func (p *Param) write(sess *network.Session) error {
	sess.PutBytes(uint8(p.DataType), p.Flag, p.Precision, p.Scale)
	// session.PutUint64(int(par.DataType), 1, false, false)
	// session.PutUint64(par.Flag, 1, false, false)
	// session.PutUint64(par.Precision, 1, false, false)
	// session.PutUint64(par.Scale, 1, false, false)
	sess.PutUint64(uint64(p.MaxLen), 4, true, true)
	sess.PutInt64(int64(p.MaxNoOfArrayElements), 4, true, true)
	sess.PutInt64(int64(p.ContFlag), 4, true, true)
	if p.ToID == nil {
		sess.PutBytes(0)
		// session.PutInt64(0, 1, false, false)
	} else {
		sess.PutInt64(int64(len(p.ToID)), 4, true, true)
		sess.PutClr(p.ToID)
	}
	sess.PutUint64(uint64(p.Version), 2, true, true)
	sess.PutUint64(uint64(p.CharsetID), 2, true, true)
	sess.PutBytes(uint8(p.CharsetForm))
	// session.PutUint64(par.CharsetForm, 1, false, false)
	sess.PutUint64(uint64(p.MaxCharLen), 4, true, true)
	return nil
}

//func NewIntegerParameter(name string, val int, direction ParameterDirection) *ParameterInfo {
//	ret := ParameterInfo{
//		Name:        name,
//		Direction:   direction,
//		flag:        3,
//		ContFlag:    0,
//		DataType:    NUMBER,
//		MaxCharLen:  22,
//		MaxLen:      22,
//		CharsetID:   871,
//		CharsetForm: 1,
//		Value:       conv.EncodeInt(val),
//	}
//	return &ret
//}
//func NewStringParameter(name string, val string, size int, direction ParameterDirection) *ParameterInfo {
//	ret := ParameterInfo{
//		Name:        name,
//		Direction:   direction,
//		flag:        3,
//		ContFlag:    16,
//		DataType:    NCHAR,
//		MaxCharLen:  size,
//		MaxLen:      size,
//		CharsetID:   871,
//		CharsetForm: 1,
//		Value:       []byte(val),
//	}
//	return &ret
//}
//func NewParamInfo(name string, parType ParameterType, size int, direction ParameterDirection) *ParameterInfo {
//	ret := new(ParameterInfo)
//	ret.Name = name
//	ret.Direction = direction
//	ret.flag = 3
//	//ret.DataType = dataType
//	switch parType {
//	case String:
//		ret.ContFlag = 16
//	default:
//		ret.ContFlag = 0
//	}
//	switch parType {
//	case Number:
//		ret.DataType = NUMBER
//		ret.MaxLen = 22
//	case String:
//		ret.CharsetForm = 1
//		ret.DataType = NCHAR
//		ret.MaxCharLen = size
//		ret.MaxLen = size
//	}
//	//ret.MaxCharLen = 0 // number of character to write
//	//ret.MaxLen = ret.MaxCharLen * 1 // number of character * byte per character
//	ret.CharsetID = 871
//	return ret
//	// if duplicateBind ret.flag = 128 else ret.flag = 3
//	// if collection type is assocative array ret.Flat |= 64
//
//	//num3 := 0
//	//switch dataType {
//	//case LONG:
//	//	fallthrough
//	//case LongRaw:
//	//	fallthrough
//	//case CHAR:
//	//	fallthrough
//	//case RAW:
//	//	fallthrough
//	//case NCHAR:
//	//	num3 = 1
//	//default:
//	//	num3 = 0
//	//}
//	//if num3 != 0 {
//	//
//	//}
//	//return ret
//}
