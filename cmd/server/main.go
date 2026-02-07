package server

import (
	"fmt"
	"net"
)

func main() {
	port := 50051
	


	address := fmt.Sprintf(":%d", port)
	_, err := net.Listen("tcp", address)

	if err != nil {
		fmt.Errorf(err.Error())
	}
}