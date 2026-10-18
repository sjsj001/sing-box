//go:build with_naive_outbound

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// A client → relay 1 (naive in → naive out) → relay 2 (naive in → direct) →
// echo chain, over h2 and over h3, with a plain naive client and with a smart
// client, whose deferred response takes the responder path through both
// relays. Nothing else here runs a naive inbound behind a naive outbound.
func TestNaiveChain(t *testing.T) {
	for _, quic := range []bool{false, true} {
		t.Run(fmt.Sprint("quic=", quic), func(t *testing.T) {
			caPem, certPem, keyPem := createSelfSignedCertificate(t, "example.org")
			caPemContent, err := os.ReadFile(caPem)
			require.NoError(t, err)
			caJSON, _ := json.Marshal(string(caPemContent))
			network := "tcp"
			if quic {
				network = "udp"
			}
			inbound := func(tag string, port uint16) string {
				return fmt.Sprintf(`{"type":"naive","tag":%q,"listen":"127.0.0.1","listen_port":%d,"network":%q,
					"users":[{"username":"sekai","password":"password"}],
					"tls":{"enabled":true,"server_name":"example.org","certificate_path":%q,"key_path":%q}}`, tag, port, network, certPem, keyPem)
			}
			outbound := func(tag string, port uint16) string {
				return fmt.Sprintf(`{"type":"naive","tag":%q,"server":"127.0.0.1","server_port":%d,"quic":%v,
					"username":"sekai","password":"password","tls":{"enabled":true,"server_name":"example.org","certificate":[%s]}}`, tag, port, quic, caJSON)
			}
			relay1, relay2 := c26Port(t), c26Port(t)
			startC26Box(t, fmt.Sprintf(`{"log":{"level":"warn"},"inbounds":[%s],"outbounds":[{"type":"direct","tag":"direct"}],"route":{"final":"direct"}}`,
				inbound("in2", relay2)))
			startC26Box(t, fmt.Sprintf(`{"log":{"level":"warn"},"inbounds":[%s],"outbounds":[%s,{"type":"direct","tag":"direct"}],"route":{"final":"next"}}`,
				inbound("in1", relay1), outbound("next", relay2)))
			plainPort, smartPort := c26Port(t), c26Port(t)
			startC26Box(t, fmt.Sprintf(`{"log":{"level":"warn"},"inbounds":[
					{"type":"mixed","tag":"m-plain","listen":"127.0.0.1","listen_port":%d},
					{"type":"mixed","tag":"m-smart","listen":"127.0.0.1","listen_port":%d}],
				"outbounds":[%s,%s,{"type":"smart","tag":"smart","groups":[{"tag":"g","outbounds":["n-smart"]}]},{"type":"direct","tag":"direct"}],
				"route":{"rules":[{"inbound":["m-plain"],"action":"route","outbound":"n-plain"},{"inbound":["m-smart"],"action":"route","outbound":"smart"}],"final":"direct"}}`,
				plainPort, smartPort, outbound("n-plain", relay1), outbound("n-smart", relay1)))
			t.Run("plain", func(t *testing.T) { testTCP(t, plainPort, testPort) })
			t.Run("smart", func(t *testing.T) { testTCP(t, smartPort, testPort) })
		})
	}
}
