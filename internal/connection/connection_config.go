package connection

type AuthConfig struct {
	connectType          string
	accessKey            string
	secretKey            string
	connectionString     string
	connectionProperties Properties
}

type Properties struct {
	IsMainInstance bool
	SaveEncrypted  bool
	SaveCompressed bool
}

func NewAuthConfig() *AuthConfig {
	return &AuthConfig{}
}

func (a *AuthConfig) GetConnectType() string {
	return a.connectType
}

func (a *AuthConfig) GetAccessKey() string {
	return a.accessKey
}

func (a *AuthConfig) GetSecretKey() string {
	return a.secretKey
}

func (a *AuthConfig) GetConnectionString() string {
	return a.connectionString
}

func (a *AuthConfig) SetConnectType(connectType string) {
	a.connectType = connectType
}

func (a *AuthConfig) SetAccessKey(accessKey string) {
	a.accessKey = accessKey
}

func (a *AuthConfig) SetSecretKey(secretKey string) {
	a.secretKey = secretKey
}

func (a *AuthConfig) SetConnectionString(connectionString string) {
	a.connectionString = connectionString
}

func (a *AuthConfig) GetProperties() Properties {
	return a.connectionProperties
}

func (a *AuthConfig) SetProperties(properties Properties) {
	a.connectionProperties = properties
}
