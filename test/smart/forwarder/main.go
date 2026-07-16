// Command forwarder is a minimal TCP relay used by the smart-group
// integration tests: it exposes a second "node" entry that tunnels to a real
// node, so intra-region primary/backup failover can be exercised by killing
// this process.
package main

import (
	"flag"
	"io"
	"log"
	"net"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8443", "listen address")
	target := flag.String("target", "", "target host:port")
	flag.Parse()
	if *target == "" {
		log.Fatal("missing -target")
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("forwarding %s -> %s", *listen, *target)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			defer conn.Close()
			upstream, err := net.Dial("tcp", *target)
			if err != nil {
				return
			}
			defer upstream.Close()
			go io.Copy(upstream, conn)
			io.Copy(conn, upstream)
		}()
	}
}
