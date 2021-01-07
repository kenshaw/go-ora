package ora

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"strconv"
	"strings"
)

func init() {
	sql.Register("oracle", &Driver{})
}

// Driver is the oracle driver.
type Driver struct{}

func (Driver) Open(dsn string) (driver.Conn, error) {
	conn, err := NewConn(dsn)
	if err != nil {
		return nil, err
	}
	return conn, conn.Open()
}

// Config is the oracle driver config.
type Config struct {
	DataSource            string
	Host                  string
	Port                  int
	SID                   string
	ServiceName           string
	InstanceName          string
	Privilege             PrivilegeType
	Enlist                EnlistType
	ConnectionLifeTime    int // FIXME: this should be time.Duration
	IncrPoolSize          int
	DecrPoolSize          int
	MaxPoolSize           int
	MinPoolSize           int
	Password              string
	PasswordSecurityInfo  bool
	Pooling               bool
	ConnectionTimeOut     int
	UserID                string
	PromotableTransaction PromotableType
	ProxyUserID           string
	ProxyPassword         string
	ValidateConnection    bool
	StmtCacheSize         int
	StmtCachePurge        bool
	HaEvent               bool
	LoadBalance           bool
	MetadataBooling       bool
	ContextConnection     bool
	SelfTuning            bool
	ApplicationEdition    string
	PoolReglator          int
	ConnectionPoolTimeout int
	PasswordlessConString string
	Trace                 string // Trace file
}

// DefaultConfig returns the default config.
func DefaultConfig() Config {
	return Config{
		Port:                  1521,
		Privilege:             PrivilegeNONE,
		Enlist:                EnlistTRUE,
		IncrPoolSize:          5,
		DecrPoolSize:          5,
		MaxPoolSize:           100,
		MinPoolSize:           1,
		ConnectionTimeOut:     15,
		PromotableTransaction: PromotablePromotable,
		StmtCacheSize:         20,
		MetadataBooling:       true,
		SelfTuning:            true,
		PoolReglator:          100,
		ConnectionPoolTimeout: 15,
	}
}

// ConfigFromString returns the config as parsed from the passed string.
func ConfigFromString(s string) (Config, error) {
	config := DefaultConfig()
	for _, f := range strings.Split(strings.ToUpper(s), ";") {
		fields := strings.Split(f, "=")
		if len(fields) != 2 {
			return Config{}, errors.New("error in connection string")
		}
		key, val := fields[0], fields[1]
		switch key {
		case "DATA SOURCE":
			config.DataSource = val
		case "DBA PRIVILEGE":
			config.Privilege = PrivilegeFromString(val)
		case "ENLIST":
			config.Enlist = EnlistFromString(val)
		case "CONNECT TIMEOUT", "CONNECTION TIMEOUT":
			var err error
			config.ConnectionTimeOut, err = strconv.Atoi(val)
			if err != nil {
				return Config{}, errors.New("CONNECTION TIMEOUT value must be an integer")
			}
		case "INC POOL SIZE":
			var err error
			config.IncrPoolSize, err = strconv.Atoi(val)
			if err != nil {
				return Config{}, errors.New("INC POOL SIZE value must be an integer")
			}
		case "DECR POOL SIZE":
			var err error
			config.DecrPoolSize, err = strconv.Atoi(val)
			if err != nil {
				return Config{}, errors.New("DECR POOL SIZE value must be an integer")
			}
		case "MAX POOL SIZE":
			var err error
			config.MaxPoolSize, err = strconv.Atoi(val)
			if err != nil {
				return Config{}, errors.New("MAX POOL SIZE value must be an integer")
			}
		case "MIN POOL SIZE":
			var err error
			config.MinPoolSize, err = strconv.Atoi(val)
			if err != nil {
				return Config{}, errors.New("MIN POOL SIZE value must be an integer")
			}
		case "POOL REGULATOR":
			var err error
			config.PoolReglator, err = strconv.Atoi(val)
			if err != nil {
				return Config{}, errors.New("POOL REGULATOR value must be an integer")
			}
		case "STATEMENT CACHE SIZE":
			var err error
			config.StmtCacheSize, err = strconv.Atoi(val)
			if err != nil {
				return Config{}, errors.New("STATEMENT CACHE SIZE value must be an integer")
			}
		case "CONNECTION POOL TIMEOUT":
			var err error
			config.ConnectionPoolTimeout, err = strconv.Atoi(val)
			if err != nil {
				return Config{}, errors.New("CONNECTION POOL TIMEOUT value must be an integer")
			}
		case "CONNECTION LIFETIME":
		case "PERSIST SECURITY INFO":
			config.PasswordSecurityInfo = val == "TRUE"
		case "POOLING":
			config.Pooling = val == "TRUE"
		case "VALIDATE CONNECTION":
			config.ValidateConnection = val == "TRUE"
		case "STATEMENT CACHE PURGE":
			config.StmtCachePurge = val == "TRUE"
		case "HA EVENTS":
			config.HaEvent = val == "TRUE"
		case "LOAD BALANCING":
			config.LoadBalance = val == "TRUE"
		case "METADATA POOLING":
			config.MetadataBooling = val == "TRUE"
		case "SELF TUNING":
			config.SelfTuning = val == "TRUE"
		case "CONTEXT CONNECTION":
			config.ContextConnection = val == "TRUE"
		case "PROMOTABLE TRANSACTION":
			if val == "PROMOTABLE" {
				config.PromotableTransaction = PromotablePromotable
			} else {
				config.PromotableTransaction = PromotableLocal
			}
		case "APPLICATION EDITION":
			config.ApplicationEdition = val
		case "USER ID":
			val = strings.Trim(val, "'")
			config.UserID = strings.Trim(val, "\"")
			if config.UserID == "\\" {
				// get os user and password
			}
		case "PROXY USER ID":
			val = strings.Trim(val, "'")
			config.ProxyUserID = strings.Trim(val, "\"")
		case "PASSWORD":
			val = strings.Trim(val, "'")
			config.Password = strings.Trim(val, "\"")
		case "PROXY PASSWORD":
			val = strings.Trim(val, "'")
			config.ProxyPassword = strings.Trim(val, "\"")
		}
	}
	// validate
	if !config.Pooling {
		config.MaxPoolSize, config.MinPoolSize = -1, 0
		config.IncrPoolSize, config.DecrPoolSize = -1, 0
		config.PoolReglator = 0
	}
	return config, nil
}

func ConfigFromURL(urlstr string) (Config, error) {
	u, err := url.Parse(urlstr)
	if err != nil {
		return Config{}, err
	}
	q := u.Query()
	p := u.Port()
	config := DefaultConfig()
	config.UserID = u.User.Username()
	config.Password, _ = u.User.Password()
	if p != "" {
		config.Port, err = strconv.Atoi(p)
		if err != nil {
			return Config{}, errors.New("Port must be a number")
		}
	}
	config.Host = u.Host
	config.ServiceName = strings.Trim(u.Path, "/")
	if len(config.UserID) == 0 {
		return Config{}, errors.New("empty user name")
	}
	if len(config.Password) == 0 {
		return Config{}, errors.New("empty password")
	}
	if len(config.Host) == 0 {
		return Config{}, errors.New("empty host name (server name)")
	}
	if q != nil {
		for key, val := range q {
			switch strings.ToUpper(key) {
			// case "DATA SOURCE":
			//	conStr.DataSource = val
			case "SERVICE NAME":
				config.ServiceName = val[0]
			case "SID":
				config.SID = val[0]
			case "INSTANCE NAME":
				config.InstanceName = val[0]
			case "DBA PRIVILEGE":
				config.Privilege = PrivilegeFromString(val[0])
			case "ENLIST":
				config.Enlist = EnlistFromString(val[0])
			case "CONNECT TIMEOUT", "CONNECTION TIMEOUT":
				config.ConnectionTimeOut, err = strconv.Atoi(val[0])
				if err != nil {
					return Config{}, errors.New("CONNECTION TIMEOUT value must be an integer")
				}
			case "INC POOL SIZE":
				config.IncrPoolSize, err = strconv.Atoi(val[0])
				if err != nil {
					return Config{}, errors.New("INC POOL SIZE value must be an integer")
				}
			case "DECR POOL SIZE":
				config.DecrPoolSize, err = strconv.Atoi(val[0])
				if err != nil {
					return Config{}, errors.New("DECR POOL SIZE value must be an integer")
				}
			case "MAX POOL SIZE":
				config.MaxPoolSize, err = strconv.Atoi(val[0])
				if err != nil {
					return Config{}, errors.New("MAX POOL SIZE value must be an integer")
				}
			case "MIN POOL SIZE":
				config.MinPoolSize, err = strconv.Atoi(val[0])
				if err != nil {
					return Config{}, errors.New("MIN POOL SIZE value must be an integer")
				}
			case "POOL REGULATOR":
				config.PoolReglator, err = strconv.Atoi(val[0])
				if err != nil {
					return Config{}, errors.New("POOL REGULATOR value must be an integer")
				}
			case "STATEMENT CACHE SIZE":
				config.StmtCacheSize, err = strconv.Atoi(val[0])
				if err != nil {
					return Config{}, errors.New("STATEMENT CACHE SIZE value must be an integer")
				}
			case "CONNECTION POOL TIMEOUT":
				config.ConnectionPoolTimeout, err = strconv.Atoi(val[0])
				if err != nil {
					return Config{}, errors.New("CONNECTION POOL TIMEOUT value must be an integer")
				}
			// case "CONNECTION LIFETIME":
			case "PERSIST SECURITY INFO":
				config.PasswordSecurityInfo = val[0] == "TRUE"
			case "POOLING":
				config.Pooling = val[0] == "TRUE"
			case "VALIDATE CONNECTION":
				config.ValidateConnection = val[0] == "TRUE"
			case "STATEMENT CACHE PURGE":
				config.StmtCachePurge = val[0] == "TRUE"
			case "HA EVENTS":
				config.HaEvent = val[0] == "TRUE"
			case "LOAD BALANCING":
				config.LoadBalance = val[0] == "TRUE"
			case "METADATA POOLING":
				config.MetadataBooling = val[0] == "TRUE"
			case "SELF TUNING":
				config.SelfTuning = val[0] == "TRUE"
			case "CONTEXT CONNECTION":
				config.ContextConnection = val[0] == "TRUE"
			case "PROMOTABLE TRANSACTION":
				if val[0] == "PROMOTABLE" {
					config.PromotableTransaction = PromotablePromotable
				} else {
					config.PromotableTransaction = PromotableLocal
				}
			case "APPLICATION EDITION":
				config.ApplicationEdition = val[0]
			// case "USER ID":
			//	val = strings.Trim(val, "'")
			//	conStr.UserID = strings.Trim(val, "\"")
			//	if conStr.UserID == "\\" {
			//		// get os user and password
			//	}
			case "PROXY USER ID":
				config.ProxyUserID = val[0]
			// case "PASSWORD":
			//	val = strings.Trim(val, "'")
			//	conStr.Password = strings.Trim(val, "\"")
			case "PROXY PASSWORD":
				config.ProxyPassword = val[0]
			case "TRACE FILE":
				config.Trace = val[0]
			}
		}
	}
	if len(config.SID) == 0 && len(config.ServiceName) == 0 {
		return Config{}, errors.New("empty SID and service name")
	}
	// validate
	if !config.Pooling {
		config.MaxPoolSize, config.MinPoolSize = -1, 0
		config.IncrPoolSize, config.DecrPoolSize = -1, 0
		config.PoolReglator = 0
	}
	return config, nil
}

type PromotableType int

const (
	PromotableLocal      PromotableType = 0
	PromotablePromotable PromotableType = 1
)

type PrivilegeType int

const (
	PrivilegeNONE    PrivilegeType = 0
	PrivilegeSYSDBA  PrivilegeType = 0x20
	PrivilegeSYSOPER PrivilegeType = 0x40
)

func PrivilegeFromString(s string) PrivilegeType {
	switch strings.ToUpper(s) {
	case "SYSDBA":
		return PrivilegeSYSDBA
	case "SYSOPER":
		return PrivilegeSYSOPER
	}
	return PrivilegeNONE
}

type EnlistType int

const (
	EnlistFALSE   EnlistType = 0
	EnlistTRUE    EnlistType = 1
	EnlistDYNAMIC EnlistType = 2
)

func EnlistFromString(s string) EnlistType {
	switch strings.ToUpper(s) {
	case "TRUE":
		return EnlistTRUE
	case "DYNAMIC":
		return EnlistDYNAMIC
	}
	return EnlistFALSE
}
