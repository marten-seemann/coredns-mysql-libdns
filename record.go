package coredns_mysql_libdns

import (
	"encoding/json"
	"fmt"
	"github.com/cloud66-oss/coredns_mysql"
	"github.com/libdns/libdns"
	"net"
)

func marshalRecord(rec libdns.Record) (string, error) {
	switch rec.Type {
	case "A":
		ip := net.ParseIP(rec.Value)
		if ip == nil {
			return "", fmt.Errorf("failed to parse IP: %s", rec.Value)
		}
		data, err := json.Marshal(coredns_mysql.ARecord{Ip: ip})
		if err != nil {
			return "", err
		}
		return string(data), nil
	case "TXT":
		data, err := json.Marshal(coredns_mysql.TXTRecord{Text: rec.Value})
		if err != nil {
			return "", err
		}
		return string(data), err
	default:
		return "", fmt.Errorf("unsupported record type: %s", rec.Type)
	}
}

func unmarshalARecord(data []byte) (string, error) {
	var rec coredns_mysql.ARecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return "", err
	}
	return rec.Ip.String(), nil
}

func unmarshalTXTRecord(data []byte) (string, error) {
	var rec coredns_mysql.TXTRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return "", err
	}
	return rec.Text, nil
}
