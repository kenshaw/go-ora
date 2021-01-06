package ora

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
	"time"

	"github.com/sijms/go-ora/conv"
	"github.com/sijms/go-ora/network"
	"github.com/sijms/go-ora/trace"
)

type State int

const (
	StateClosed State = iota
	StateOpened
)

type LogonMode int

const (
	LogonNoNewPass LogonMode = 0x1
	// WithNewPass LogonMode = 0x2
	LogonSysDba   LogonMode = 0x20 // no verify response from server
	LogonSysOper  LogonMode = 0x40 // no verify response from server
	LogonUserPass LogonMode = 0x100
	// PROXY       LogonMode = 0x400
)

type NLS struct {
	Calender        string
	Comp            string
	LengthSemantics string
	NCharConvExcep  string
	DateLang        string
	Sort            string
	Currency        string
	DateFormat      string
	IsoCurrency     string
	NumericChars    string
	DualCurrency    string
	Timestamp       string
	TimestampTZ     string
}

type Conn struct {
	State             State
	LogonMode         LogonMode
	autoCommit        bool
	Config            Config
	connOption        *network.ConnectionOption
	sess              *network.Session
	tcpNego           *TCPNego
	dataNego          *DataTypeNego
	authObject        *Auth
	SessionProperties map[string]string
	Version           Version
	sessionID         int
	serialID          int
	strConv           *conv.StringConverter
	NLS               NLS
}

// NewConn creates a new connection based on the passed urlstr.
func NewConn(urlstr string) (*Conn, error) {
	config, err := ConfigFromURL(urlstr)
	if err != nil {
		return nil, err
	}
	username := ""
	u, err := user.Current()
	if err == nil {
		username = u.Username
	}
	host, _ := os.Hostname()
	indexOfSlash := strings.LastIndex(os.Args[0], "/")
	indexOfSlash += 1
	if indexOfSlash < 0 {
		indexOfSlash = 0
	}
	// userName = "samy"
	// hostName = "LABMANAGER"
	connOption := &network.ConnectionOption{
		Port:                  config.Port,
		TransportConnectTo:    0xffff,
		SSLVersion:            "",
		WalletDict:            "",
		TransportDataUnitSize: 0xffff,
		SessionDataUnitSize:   0xffff,
		Protocol:              "tcp",
		Host:                  config.Host,
		UserID:                config.UserID,
		// IP:                    "",
		SID: config.SID,
		// Addr:                  "",
		// Server:                conn.conStr.Host,
		ServiceName:  config.ServiceName,
		InstanceName: config.InstanceName,
		ClientData: network.ClientData{
			ProgramPath: os.Args[0],
			ProgramName: os.Args[0][indexOfSlash:],
			UserName:    username,
			HostName:    host,
			DriverName:  "OracleClientGo",
			PID:         os.Getpid(),
		},
		// InAddrAny:             false,
	}
	if len(config.Trace) > 0 {
		f, err := os.Create(config.Trace)
		if err != nil {
			return nil, fmt.Errorf("Can't open trace file: %w", err)
		}
		connOption.Tracer = trace.NewTraceWriter(f)
	} else {
		connOption.Tracer = trace.NilTracer()
	}
	return &Conn{
		State:      StateClosed,
		Config:     config,
		connOption: connOption,
		autoCommit: true,
	}, nil
}

func (conn *Conn) GetNLS() (*NLS, error) {
	const sqlstr = `DECLARE
	err_code VARCHAR2(2000);
	err_msg  VARCHAR2(2000);
	BEGIN
		SELECT VALUE into :p_nls_calendar from nls_session_parameters where PARAMETER='NLS_CALENDAR';
		SELECT VALUE into :p_nls_comp from nls_session_parameters where PARAMETER='NLS_COMP';
		SELECT VALUE into :p_nls_length_semantics from nls_session_parameters where PARAMETER='NLS_LENGTH_SEMANTICS';
		SELECT VALUE into :p_nls_nchar_conv_excep from nls_session_parameters where PARAMETER='NLS_NCHAR_CONV_EXCP';
		SELECT VALUE into :p_nls_date_lang from nls_session_parameters where PARAMETER='NLS_DATE_LANGUAGE';
		SELECT VALUE into :p_nls_sort from nls_session_parameters where PARAMETER='NLS_SORT';
		SELECT VALUE into :p_nls_currency from nls_session_parameters where PARAMETER='NLS_CURRENCY';
		SELECT VALUE into :p_nls_date_format from nls_session_parameters where PARAMETER='NLS_DATE_FORMAT';
		SELECT VALUE into :p_nls_iso_currency from nls_session_parameters where PARAMETER='NLS_ISO_CURRENCY';
		SELECT VALUE into :p_nls_numeric_chars from nls_session_parameters where PARAMETER='NLS_NUMERIC_CHARACTERS';
		SELECT VALUE into :p_nls_dual_currency from nls_session_parameters where PARAMETER='NLS_DUAL_CURRENCY';
		SELECT VALUE into :p_nls_timestamp from nls_session_parameters where PARAMETER='NLS_TIMESTAMP_FORMAT';
		SELECT VALUE into :p_nls_timestamp_tz from nls_session_parameters where PARAMETER='NLS_TIMESTAMP_TZ_FORMAT';
		SELECT '0' into :p_err_code from dual;
		SELECT '0' into :p_err_msg from dual;
	END;`
	stmt := NewStmt(sqlstr, conn)
	stmt.AddParam("p_nls_calendar", "", 40, BindOutput)
	stmt.AddParam("p_nls_comp", "", 40, BindOutput)
	stmt.AddParam("p_nls_length_semantics", "", 40, BindOutput)
	stmt.AddParam("p_nls_nchar_conv_excep", "", 40, BindOutput)
	stmt.AddParam("p_nls_date_lang", "", 40, BindOutput)
	stmt.AddParam("p_nls_sort", "", 40, BindOutput)
	stmt.AddParam("p_nls_currency", "", 40, BindOutput)
	stmt.AddParam("p_nls_date_format", "", 40, BindOutput)
	stmt.AddParam("p_nls_iso_currency", "", 40, BindOutput)
	stmt.AddParam("p_nls_numeric_chars", "", 40, BindOutput)
	stmt.AddParam("p_nls_dual_currency", "", 40, BindOutput)
	stmt.AddParam("p_nls_timestamp", "", 48, BindOutput)
	stmt.AddParam("p_nls_timestamp_tz", "", 56, BindOutput)
	stmt.AddParam("p_err_code", "", 2000, BindOutput)
	stmt.AddParam("p_err_msg", "", 2000, BindOutput)
	defer stmt.Close()
	// fmt.Println(stmt.Pars)
	_, err := stmt.Exec(nil)
	if err != nil {
		return nil, err
	}
	for _, par := range stmt.params {
		switch par.Name {
		case "p_nls_calendar":
			conn.NLS.Calender = conn.strConv.Decode(par.Value)
		case "p_nls_comp":
			conn.NLS.Comp = conn.strConv.Decode(par.Value)
		case "p_nls_length_semantics":
			conn.NLS.LengthSemantics = conn.strConv.Decode(par.Value)
		case "p_nls_nchar_conv_excep":
			conn.NLS.NCharConvExcep = conn.strConv.Decode(par.Value)
		case "p_nls_date_lang":
			conn.NLS.DateLang = conn.strConv.Decode(par.Value)
		case "p_nls_sort":
			conn.NLS.Sort = conn.strConv.Decode(par.Value)
		case "p_nls_currency":
			conn.NLS.Currency = conn.strConv.Decode(par.Value)
		case "p_nls_date_format":
			conn.NLS.DateFormat = conn.strConv.Decode(par.Value)
		case "p_nls_iso_currency":
			conn.NLS.IsoCurrency = conn.strConv.Decode(par.Value)
		case "p_nls_numeric_chars":
			conn.NLS.NumericChars = conn.strConv.Decode(par.Value)
		case "p_nls_dual_currency":
			conn.NLS.DualCurrency = conn.strConv.Decode(par.Value)
		case "p_nls_timestamp":
			conn.NLS.Timestamp = conn.strConv.Decode(par.Value)
		case "p_nls_timestamp_tz":
			conn.NLS.TimestampTZ = conn.strConv.Decode(par.Value)
		}
	}
	return &conn.NLS, nil
}

func (conn *Conn) Prepare(query string) (driver.Stmt, error) {
	conn.connOption.Tracer.Print("Prepare\n", query)
	return NewStmt(query, conn), nil
}

func (conn *Conn) Ping(ctx context.Context) error {
	conn.connOption.Tracer.Print("Ping")
	conn.sess.ResetBuffer()
	return (&simpleObject{
		session:     conn.sess,
		operationID: 0x93,
		data:        nil,
	}).write().read()
}

func (conn *Conn) Logoff() error {
	conn.connOption.Tracer.Print("Logoff")
	session := conn.sess
	session.ResetBuffer()
	session.PutBytes(
		0x11, 0x87, 0, 0, 0, 0x2, 0x1,
		0x11, 0x1,
		0, 0, 0, 0x1,
		0, 0, 0, 0, 0, 0x1,
		0, 0, 0, 0, 0, 3, 9, 0,
	)
	err := session.Write()
	if err != nil {
		return err
	}
loop:
	for {
		msg, err := session.GetByte()
		if err != nil {
			return err
		}
		switch msg {
		case 4:
			session.Summary, err = network.NewSummary(session)
			if err != nil {
				return err
			}
			break loop
		case 9:
			if session.HasEOSCapability {
				if session.Summary == nil {
					session.Summary = new(network.SummaryObject)
				}
				session.Summary.EndOfCallStatus, err = session.GetInt(4, true, true)
				if err != nil {
					return err
				}
			}
			if session.HasFSAPCapability {
				if session.Summary == nil {
					session.Summary = new(network.SummaryObject)
				}
				session.Summary.EndToEndECIDSequence, err = session.GetInt(2, true, true)
				if err != nil {
					return err
				}
			}
			break loop
		default:
			return fmt.Errorf("message code error: received code %d and expected code is 4, 9", msg)
		}
	}
	if session.HasError() {
		return errors.New(session.GetError())
	}
	return nil
}

func (conn *Conn) Open() error {
	conn.connOption.Tracer.Print("Open :", conn.connOption.ConnectionData())
	switch conn.Config.Privilege {
	case PrivilegeSYSDBA:
		conn.LogonMode |= LogonSysDba
	case PrivilegeSYSOPER:
		conn.LogonMode |= LogonSysOper
	default:
		conn.LogonMode = 0
	}
	conn.sess = network.NewSession(*conn.connOption)
	err := conn.sess.Connect()
	if err != nil {
		return err
	}
	conn.tcpNego, err = NewTCPNego(conn.sess)
	if err != nil {
		return err
	}
	// create string converter object
	conn.strConv = conv.NewStringConverter(conn.tcpNego.ServerCharset)
	conn.sess.StrConv = conn.strConv
	conn.dataNego, err = buildTypeNego(conn.tcpNego, conn.sess)
	if err != nil {
		return err
	}
	conn.sess.TTCVersion = conn.dataNego.CompileTimeCaps[7]
	if conn.tcpNego.ServerCompileTimeCaps[7] < conn.sess.TTCVersion {
		conn.sess.TTCVersion = conn.tcpNego.ServerCompileTimeCaps[7]
	}
	// if (((int) this.m_serverCompiletimeCapabilities[15] & 1) != 0)
	//	this.m_marshallingEngine.HasEOCSCapability = true;
	// if (((int) this.m_serverCompiletimeCapabilities[16] & 16) != 0)
	//	this.m_marshallingEngine.HasFSAPCapability = true;
	err = conn.doAuth()
	if err != nil {
		return err
	}
	conn.State = StateOpened
	conn.Version, err = GetVersion(conn.sess)
	if err != nil {
		return err
	}
	sessionID, err := strconv.ParseUint(conn.SessionProperties["AUTH_SESSION_ID"], 10, 32)
	if err != nil {
		return err
	}
	conn.sessionID = int(sessionID)
	serialNum, err := strconv.ParseUint(conn.SessionProperties["AUTH_SERIAL_NUM"], 10, 32)
	if err != nil {
		return err
	}
	conn.serialID = int(serialNum)
	conn.connOption.InstanceName = conn.SessionProperties["AUTH_SC_INSTANCE_NAME"]
	conn.connOption.Host = conn.SessionProperties["AUTH_SC_SERVER_HOST"]
	conn.connOption.ServiceName = conn.SessionProperties["AUTH_SC_SERVICE_NAME"]
	conn.connOption.DomainName = conn.SessionProperties["AUTH_SC_DB_DOMAIN"]
	conn.connOption.DBName = conn.SessionProperties["AUTH_SC_DBUNIQUE_NAME"]
	_, err = conn.GetNLS()
	return err
}

func (conn *Conn) Begin() (driver.Tx, error) {
	conn.connOption.Tracer.Print("Begin transaction")
	conn.autoCommit = false
	return &Transaction{conn: conn}, nil
}

func (conn *Conn) Close() error {
	conn.connOption.Tracer.Print("Close")
	// var err error = nil
	if conn.sess != nil {
		// err = conn.Logoff()
		conn.sess.Disconnect()
		conn.sess = nil
	}
	conn.connOption.Tracer.Print("Connection Closed")
	conn.connOption.Tracer.Close()
	return nil
}

func (conn *Conn) doAuth() error {
	conn.connOption.Tracer.Print("doAuth")
	conn.sess.ResetBuffer()
	conn.sess.PutBytes(3, 118, 0, 1)
	conn.sess.PutUint64(uint64(len(conn.Config.UserID)), 4, true, true)
	conn.LogonMode = conn.LogonMode | LogonNoNewPass
	conn.sess.PutUint64(uint64(conn.LogonMode), 4, true, true)
	conn.sess.PutBytes(1, 1, 5, 1, 1)
	conn.sess.PutBytes([]byte(conn.Config.UserID)...)
	conn.sess.PutKeyValString("AUTH_TERMINAL", conn.connOption.ClientData.HostName, 0)
	conn.sess.PutKeyValString("AUTH_PROGRAM_NM", conn.connOption.ClientData.ProgramName, 0)
	conn.sess.PutKeyValString("AUTH_MACHINE", conn.connOption.ClientData.HostName, 0)
	conn.sess.PutKeyValString("AUTH_PID", fmt.Sprintf("%d", conn.connOption.ClientData.PID), 0)
	conn.sess.PutKeyValString("AUTH_SID", conn.connOption.ClientData.UserName, 0)
	err := conn.sess.Write()
	if err != nil {
		return err
	}
	conn.authObject, err = NewAuth(conn.Config.UserID, conn.Config.Password, conn.tcpNego, conn.sess)
	if err != nil {
		return err
	}
	// if proxyAuth ==> mode |= PROXY
	err = conn.authObject.Write(conn.connOption, conn.LogonMode, conn.sess)
	if err != nil {
		return err
	}
	stop := false
	for !stop {
		msg, err := conn.sess.GetInt(1, false, false)
		if err != nil {
			return err
		}
		switch msg {
		case 4:
			conn.sess.Summary, err = network.NewSummary(conn.sess)
			if err != nil {
				return err
			}
			if conn.sess.HasError() {
				return errors.New(conn.sess.GetError())
			}
			stop = true
		case 8:
			dictLen, err := conn.sess.GetInt(4, true, true)
			if err != nil {
				return err
			}
			conn.SessionProperties = make(map[string]string, dictLen)
			for x := 0; x < dictLen; x++ {
				key, val, _, err := conn.sess.GetKeyVal()
				if err != nil {
					return err
				}
				conn.SessionProperties[string(key)] = string(val)
			}
		case 15:
			warning, err := network.NewWarningObject(conn.sess)
			if err != nil {
				return err
			}
			if warning != nil {
				fmt.Println(warning)
			}
			stop = true
		default:
			return fmt.Errorf("message code error: received code %d and expected code is 8", msg)
		}
	}
	// if verifyResponse == true
	// conn.authObject.VerifyResponse(conn.SessionProperties["AUTH_SVR_RESPONSE"])
	return nil
}

type TCPNego struct {
	MessageCode           uint8
	ProtocolServerVersion uint8
	ProtocolServerString  string
	OracleVersion         int
	ServerCharset         int
	ServerFlags           uint8
	CharsetElem           int
	ServernCharset        int
	ServerCompileTimeCaps []byte
	ServerRuntimeCaps     []byte
}

func NewTCPNego(session *network.Session) (*TCPNego, error) {
	session.ResetBuffer()
	session.PutBytes(1, 6, 0)
	session.PutBytes([]byte("OracleClientGo\x00")...)
	err := session.Write()
	if err != nil {
		return nil, err
	}
	result := TCPNego{}
	result.MessageCode, err = session.GetByte()
	if err != nil {
		return nil, err
	}
	if result.MessageCode != 1 {
		return nil, fmt.Errorf("message code error: received code %d and expected code is 1", result.MessageCode)
	}
	result.ProtocolServerVersion, err = session.GetByte()
	if err != nil {
		return nil, err
	}
	switch result.ProtocolServerVersion {
	case 4:
		result.OracleVersion = 7230
	case 5:
		result.OracleVersion = 8030
	case 6:
		result.OracleVersion = 8100
	default:
		return nil, errors.New("unsupported server version")
	}
	_, _ = session.GetByte()
	result.ProtocolServerString, err = session.GetNullTermString(50)
	if err != nil {
		return nil, err
	}
	result.ServerCharset, err = session.GetInt(2, false, false)
	if err != nil {
		return nil, err
	}
	result.ServerFlags, err = session.GetByte()
	if err != nil {
		return nil, err
	}
	result.CharsetElem, err = session.GetInt(2, false, false)
	if err != nil {
		return nil, err
	}
	if result.CharsetElem > 0 {
		_, _ = session.GetBytes(result.CharsetElem * 5)
	}
	len1, err := session.GetInt(2, false, true)
	if err != nil {
		return nil, err
	}
	numArray, err := session.GetBytes(len1)
	if err != nil {
		return nil, err
	}
	num3 := int(6 + (numArray[5]) + (numArray[6]))
	result.ServernCharset = int(binary.BigEndian.Uint16(numArray[(num3 + 3):(num3 + 5)]))
	len2, err := session.GetByte()
	if err != nil {
		return nil, err
	}
	result.ServerCompileTimeCaps, err = session.GetBytes(int(len2))
	if err != nil {
		return nil, err
	}
	len3, err := session.GetByte()
	if err != nil {
		return nil, err
	}
	result.ServerRuntimeCaps, err = session.GetBytes(int(len3))
	if err != nil {
		return nil, err
	}
	if result.ServerCompileTimeCaps[15]&1 != 0 {
		session.HasEOSCapability = true
	}
	if result.ServerCompileTimeCaps[16]&1 != 0 {
		session.HasFSAPCapability = true
	}
	return &result, nil
}

type Version struct {
	Info   string
	Text   string
	Number uint16
	Major  int
	Minor  int
	Patch  int
}

func GetVersion(session *network.Session) (Version, error) {
	session.ResetBuffer()
	session.PutBytes(3, 0x3B, 0)
	session.PutUint64(1, 1, false, false)
	session.PutUint64(0x100, 2, true, true)
	session.PutUint64(1, 1, false, false)
	session.PutUint64(1, 1, false, false)
	err := session.Write()
	if err != nil {
		return Version{}, err
	}
	msg, err := session.GetInt(1, false, false)
	if msg != 8 {
		return Version{}, fmt.Errorf("message code error: received code %d and expected code is 8", msg)
	}
	length, err := session.GetInt(2, true, true)
	if err != nil {
		return Version{}, err
	}
	info, err := session.GetBytes(int(length))
	if err != nil {
		return Version{}, err
	}
	number, err := session.GetInt(4, true, true)
	if err != nil {
		return Version{}, err
	}
	version := (number>>24&0xff)*1000 + (number>>20&0xf)*100 + (number>>12&0xf)*10 + (number >> 8 & 0xf)
	text := fmt.Sprintf(
		"%d.%d.%d.%d.%d",
		number>>24&0xFF, number>>20&0xF,
		number>>12&0xF, number>>8&0xF, number&0xFF,
	)
	major, minor, patch := int(number>>24&0xFF), int(number>>20&0xF), int(number>>8&0xF)
	return Version{
		Info:   string(info),
		Text:   text,
		Number: uint16(version),
		Major:  major,
		Minor:  minor,
		Patch:  patch,
	}, nil
}

func (v Version) Is10G() bool {
	return v.Major > 10 || (v.Major == 10 && v.Minor >= 2)
}

func (v Version) Is11G() bool {
	return v.Major > 11 || (v.Major == 11 && v.Minor >= 1)
}

// E infront of the variable means encrypted
type Auth struct {
	EServerSessKey string
	EClientSessKey string
	EPassword      string
	ServerSessKey  []byte
	ClientSessKey  []byte
	KeyHash        []byte
	Salt           string
	VerifierType   int
	tcpNego        *TCPNego
}

func NewAuth(username string, password string, tcpNego *TCPNego, session *network.Session) (*Auth, error) {
	auth := new(Auth)
	auth.tcpNego = tcpNego
	loop := true
	for loop {
		code, err := session.GetByte()
		if err != nil {
			return nil, err
		}
		switch code {
		case 4:
			session.Summary, err = network.NewSummary(session)
			if err != nil {
				return nil, err
			}
			if session.HasError() {
				return nil, errors.New(session.GetError())
			}
			loop = false
		case 8:
			dictLen, err := session.GetInt(4, true, true)
			if err != nil {
				return nil, err
			}
			for x := 0; x < dictLen; x++ {
				key, val, num, err := session.GetKeyVal()
				if err != nil {
					return nil, err
				}
				if bytes.Compare(key, []byte("AUTH_SESSKEY")) == 0 {
					auth.EServerSessKey = string(val)
				} else if bytes.Compare(key, []byte("AUTH_VFR_DATA")) == 0 {
					auth.Salt = string(val)
					auth.VerifierType = num
				}
			}
		default:
			return nil, fmt.Errorf("message code error: received code %d and expected code is 8", code)
		}
	}
	var key []byte
	padding := false
	var err error
	if auth.VerifierType == 2361 {
		key, err = getKeyFromUserNameAndPassword(username, password)
		if err != nil {
			return nil, err
		}
	} else if auth.VerifierType == 6949 {
		if auth.tcpNego.ServerCompileTimeCaps[4]&2 == 0 {
			padding = true
		}
		result, err := HexStringToBytes(auth.Salt)
		if err != nil {
			return nil, err
		}
		result = append([]byte(password), result...)
		hash := sha1.New()
		_, err = hash.Write(result)
		if err != nil {
			return nil, err
		}
		key = hash.Sum(nil)           // 20 byte key
		key = append(key, 0, 0, 0, 0) // 24 byte key
	} else {
		return nil, errors.New("unsupported verifier type")
	}
	// get the server session key
	auth.ServerSessKey, err = decryptSessionKey(padding, key, auth.EServerSessKey)
	if err != nil {
		return nil, err
	}
	// generate new key for client
	auth.ClientSessKey = make([]byte, len(auth.ServerSessKey))
	for {
		_, err = rand.Read(auth.ClientSessKey)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(auth.ClientSessKey, auth.ServerSessKey) {
			break
		}
	}
	// encrypt the client key
	auth.EClientSessKey, err = EncryptSessionKey(padding, key, auth.ClientSessKey)
	if err != nil {
		return nil, err
	}
	// get the hash key form server and client session key
	auth.KeyHash, err = CalculateKeysHash(auth.VerifierType, auth.ServerSessKey[16:], auth.ClientSessKey[16:])
	if err != nil {
		return nil, err
	}
	// encrypt the password
	auth.EPassword, err = EncryptPassword(password, auth.KeyHash)
	if err != nil {
		return nil, err
	}
	return auth, nil
}

func (auth *Auth) Write(connOption *network.ConnectionOption, mode LogonMode, session *network.Session) error {
	session.ResetBuffer()
	keyValSize := 22
	session.PutBytes(3, 0x73, 0)
	if len(connOption.UserID) > 0 {
		session.PutInt64(1, 1, false, false)
		session.PutInt64(int64(len(connOption.UserID)), 4, true, true)
	} else {
		session.PutBytes(0, 0)
	}
	if len(connOption.UserID) > 0 && len(auth.EPassword) > 0 {
		mode |= LogonUserPass
	}
	session.PutUint64(uint64(mode), 4, true, true)
	session.PutUint64(1, 1, false, false)
	session.PutUint64(uint64(keyValSize), 4, true, true)
	session.PutBytes(1, 1)
	if len(connOption.UserID) > 0 {
		session.PutBytes([]byte(connOption.UserID)...)
	}
	index := 0
	if len(auth.EClientSessKey) > 0 {
		session.PutKeyValString("AUTH_SESSKEY", auth.EClientSessKey, 1)
		index++
	}
	if len(auth.EPassword) > 0 {
		session.PutKeyValString("AUTH_PASSWORD", auth.EPassword, 0)
		index++
	}
	// if newpassword encrypt and add {
	//	session.PutKeyValString("AUTH_NEWPASSWORD", ENewPassword, 0)
	//	index ++
	//}
	session.PutKeyValString("AUTH_TERMINAL", connOption.ClientData.HostName, 0)
	index++
	session.PutKeyValString("AUTH_PROGRAM_NM", connOption.ClientData.ProgramName, 0)
	index++
	session.PutKeyValString("AUTH_MACHINE", connOption.ClientData.HostName, 0)
	index++
	session.PutKeyValString("AUTH_PID", fmt.Sprintf("%d", connOption.ClientData.PID), 0)
	index++
	session.PutKeyValString("AUTH_SID", connOption.ClientData.UserName, 0)
	index++
	session.PutKeyValString("AUTH_CONNECT_STRING", connOption.ConnectionData(), 0)
	index++
	session.PutKeyValString("SESSION_CLIENT_CHARSET", strconv.Itoa(int(auth.tcpNego.ServerCharset)), 0)
	index++
	session.PutKeyValString("SESSION_CLIENT_LIB_TYPE", "0", 0)
	index++
	session.PutKeyValString("SESSION_CLIENT_DRIVER_NAME", connOption.ClientData.DriverName, 0)
	index++
	session.PutKeyValString("SESSION_CLIENT_VERSION", "1.0.0.0", 0)
	index++
	session.PutKeyValString("SESSION_CLIENT_LOBATTR", "1", 0)
	index++
	_, offset := time.Now().Zone()
	tz := ""
	if offset == 0 {
		tz = "00:00"
	} else {
		hours := int8(offset / 3600)
		minutes := int8((offset / 60) % 60)
		if minutes < 0 {
			minutes = minutes * -1
		}
		tz = fmt.Sprintf("%+03d:%02d", hours, minutes)
	}
	//if !strings.Contains(tz, ":") {
	//	tz += ":00"
	//}
	//session.PutKeyValString("AUTH_ALTER_SESSION",
	//	fmt.Sprintf("ALTER SESSION SET NLS_LANGUAGE='ARABIC' NLS_TERRITORY='SAUDI ARABIA'  TIME_ZONE='%s'\x00", tz), 1)
	session.PutKeyValString(
		"AUTH_ALTER_SESSION",
		fmt.Sprintf("ALTER SESSION SET NLS_LANGUAGE='AMERICAN' NLS_TERRITORY='AMERICA'  TIME_ZONE='%s'\x00", tz),
		1,
	)
	index++
	//if (!string.IsNullOrEmpty(proxyClientName))
	//{
	//	keys[index1] = this.m_authProxyClientName;
	//	values[index1++] = this.m_marshallingEngine.m_dbCharSetConv.ConvertStringToBytes(proxyClientName, 0, proxyClientName.Length, true);
	//}
	//if (sessionId != -1)
	//{
	//	keys[index1] = this.m_authSessionId;
	//	values[index1++] = this.m_marshallingEngine.m_dbCharSetConv.ConvertStringToBytes(sessionId.ToString(), 0, sessionId.ToString().Length, true);
	//}
	//if (serialNum != -1)
	//{
	//	keys[index1] = this.m_authSerialNum;
	//	values[index1++] = this.m_marshallingEngine.m_dbCharSetConv.ConvertStringToBytes(serialNum.ToString(), 0, serialNum.ToString().Length, true);
	//}
	// fill remaining values with zeros
	for index < keyValSize {
		session.PutKeyVal(nil, nil, 0)
		index++
	}
	return session.Write()
}

func (auth *Auth) VerifyResponse(response string) bool {
	key, err := decryptSessionKey(true, auth.KeyHash, response)
	if err != nil {
		fmt.Println(err)
		return false
	}
	// fmt.Printf("%#v\n", key)
	return bytes.Compare(key[16:], []byte{83, 69, 82, 86, 69, 82, 95, 84, 79, 95, 67, 76, 73, 69, 78, 84}) == 0
	//KZSR_SVR_RESPONSE = new byte[16]{ (byte) 83, (byte) 69, (byte) 82, (byte) 86, (byte) 69, (byte) 82, (byte) 95, (byte) 84, (byte) 79,
	//(byte) 95, (byte) 67, (byte) 76, (byte) 73, (byte) 69, (byte) 78, (byte) 84 };
}

func getKeyFromUserNameAndPassword(username string, password string) ([]byte, error) {
	username = strings.ToUpper(username)
	password = strings.ToUpper(password)
	extendString := func(str string) []byte {
		ret := make([]byte, len(str)*2)
		for index, char := range []byte(str) {
			ret[index*2] = 0
			ret[index*2+1] = char
		}
		return ret
	}
	buffer := append(extendString(username), extendString(password)...)
	if len(buffer)%8 > 0 {
		buffer = append(buffer, make([]byte, 8-len(buffer)%8)...)
	}
	key := []byte{1, 35, 69, 103, 137, 171, 205, 239}
	DesEnc := func(input []byte, key []byte) ([]byte, error) {
		ret := make([]byte, 8)
		enc, err := des.NewCipher(key)
		if err != nil {
			return nil, err
		}
		for x := 0; x < len(input)/8; x++ {
			for y := 0; y < 8; y++ {
				ret[y] = uint8(int(ret[y]) ^ int(input[x*8+y]))
			}
			output := make([]byte, 8)
			enc.Encrypt(output, ret)
			copy(ret, output)
		}
		return ret, nil
	}
	key1, err := DesEnc(buffer, key)
	if err != nil {
		return nil, err
	}
	key2, err := DesEnc(buffer, key1)
	if err != nil {
		return nil, err
	}
	// function OSLogonHelper.Method1_bytearray (DecryptSessionKey)
	return append(key2, make([]byte, 8)...), nil
}

func PKCS5Padding(cipherText []byte, blockSize int) []byte {
	padding := blockSize - len(cipherText)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(cipherText, padtext...)
}

func HexStringToBytes(input string) ([]byte, error) {
	result := make([]byte, len(input)/2)
	for x := 0; x < len(input); x += 2 {
		num, err := strconv.ParseUint(input[x:x+2], 16, 8)
		if err != nil {
			return nil, err
		}
		result[x/2] = uint8(num)
	}
	return result, nil
}

func decryptSessionKey(padding bool, encKey []byte, sessionKey string) ([]byte, error) {
	result, err := HexStringToBytes(sessionKey)
	if err != nil {
		return nil, err
	}
	blk, err := aes.NewCipher(encKey)
	if err != nil {
		return nil, err
	}
	//if padding {
	//	result = PKCS5Padding(result, blk.BlockSize())
	//}
	enc := cipher.NewCBCDecrypter(blk, make([]byte, 16))
	output := make([]byte, len(result))
	enc.CryptBlocks(output, result)
	cutLen := 0
	if padding {
		num := int(output[len(output)-1])
		if num < enc.BlockSize() {
			apply := true
			for x := len(output) - num; x < len(output); x++ {
				if output[x] != uint8(num) {
					apply = false
					break
				}
			}
			if apply {
				cutLen = int(output[len(output)-1])
			}
		}
	}
	return output[:len(output)-cutLen], nil
}

func EncryptSessionKey(padding bool, encKey []byte, sessionKey []byte) (string, error) {
	blk, err := aes.NewCipher(encKey)
	if err != nil {
		return "", err
	}
	enc := cipher.NewCBCEncrypter(blk, make([]byte, 16))
	if padding {
		sessionKey = PKCS5Padding(sessionKey, blk.BlockSize())
	}
	output := make([]byte, len(sessionKey))
	enc.CryptBlocks(output, sessionKey)
	return fmt.Sprintf("%X", output), nil
}

func EncryptPassword(password string, key []byte) (string, error) {
	buf := make([]byte, 16)
	_, err := rand.Read(buf)
	// buf = []byte{109, 250, 127, 252, 157, 165, 29, 6, 165, 174, 50, 93, 165, 202, 192, 100}
	if err != nil {
		return "", nil
	}
	return EncryptSessionKey(true, key, append(buf, []byte(password)...))
}

func CalculateKeysHash(verifierType int, key1, key2 []byte) ([]byte, error) {
	hash := md5.New()
	switch verifierType {
	case 2361:
		buf := make([]byte, 16)
		for x := 0; x < 16; x++ {
			buf[x] = key1[x] ^ key2[x]
		}
		_, err := hash.Write(buf)
		if err != nil {
			return nil, err
		}
		return hash.Sum(nil), nil
	case 6949:
		buffer := make([]byte, 24)
		for x := 0; x < 24; x++ {
			buffer[x] = key1[x] ^ key2[x]
		}
		_, err := hash.Write(buffer[:16])
		if err != nil {
			return nil, err
		}
		ret := hash.Sum(nil)
		hash.Reset()
		_, err = hash.Write(buffer[16:])
		if err != nil {
			return nil, err
		}
		ret = append(ret, hash.Sum(nil)...)
		return ret[:24], nil
	}
	return nil, nil
}
