package config

import "os"
import "strconv"

type Config struct {
	GRPCPort   string
	BufferSize int
}


func Load() Config {
	bufferSize, _ := strconv.Atoi(os.Getenv("HERMES_BUFFER_SIZE"))
	if bufferSize == 0 {
		bufferSize = 1000
	}

	port := os.Getenv("HERMES_PORT")
	if port == "" {
		port = ":50051"
	}

	return Config{
		GRPCPort:   port,
		BufferSize: bufferSize,
	}
}