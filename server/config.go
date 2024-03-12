package server

// Config represents the config object for the server.
type Config struct {
	Enabled bool
	HTTP    HTTP
}

// HTTP represents the http config object for the http server.
type HTTP struct {
	Host string // optional, empty corresponds to "0.0.0.0"
	Port uint64
}
