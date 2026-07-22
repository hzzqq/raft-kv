package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"
)

// testTLSCert 在内存中生成一张自签名证书（无需文件/联网）。
func testTLSCert(t *testing.T) tls.Certificate {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// TestTLSEndToEnd 验证客户端经 TLS 与 ServeTLS 服务端完成真实握手并正常 RPC。
func TestTLSEndToEnd(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Echo",
		Methods: map[string]MethodHandler{
			"Ping": func(ctx context.Context, req []byte) ([]byte, error) {
				return req, nil
			},
		},
	})
	go srv.ServeTLS(lis, testTLSCert(t))
	defer srv.Stop()

	cc := DialTLS(lis.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	defer cc.Close()
	if _, err := cc.Invoke(context.Background(), "/Echo/Ping", []byte("secure")); err != nil {
		t.Fatalf("tls invoke: %v", err)
	}

	// 明文客户端连 TLS 端口应失败（握手不匹配），证明链路确实加密。
	plain := Dial(lis.Addr().String())
	defer plain.Close()
	if _, err := plain.Invoke(context.Background(), "/Echo/Ping", []byte("x")); err == nil {
		t.Fatalf("plaintext client unexpectedly succeeded against TLS server")
	}
}

// TestTLSReuseAcrossCalls 验证 TLS 连接同样走连接池复用。
func TestTLSReuseAcrossCalls(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Echo",
		Methods: map[string]MethodHandler{
			"Ping": func(ctx context.Context, req []byte) ([]byte, error) {
				return req, nil
			},
		},
	})
	go srv.ServeTLS(lis, testTLSCert(t))
	defer srv.Stop()

	cc := DialTLS(lis.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	defer cc.Close()
	for i := 0; i < 10; i++ {
		if _, err := cc.Invoke(context.Background(), "/Echo/Ping", []byte("x")); err != nil {
			t.Fatalf("tls invoke %d: %v", i, err)
		}
	}
	if st := cc.Stats(); st.Dials != 1 {
		t.Fatalf("expected 1 dial (TLS pool reuse), got %d", st.Dials)
	}
}
