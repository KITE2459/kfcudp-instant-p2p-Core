package quicbridge

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"sync"
	"time"
)

// 인증서를 한 번만 생성하고 재사용
var (
	cachedCert     *tls.Certificate
	cachedCertOnce sync.Once
	cachedCertErr  error
)

func generateSelfSigned() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "openfriend-quic"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * 365 * time.Hour),
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}, nil
}

func getCachedCert() (*tls.Certificate, error) {
	cachedCertOnce.Do(func() {
		cert, err := generateSelfSigned()
		if err != nil {
			cachedCertErr = err
			return
		}
		cachedCert = &cert
	})
	return cachedCert, cachedCertErr
}

// ServerTLSConfig - 서버용 TLS 설정 (인증서 재사용)
// CipherSuites 미지정 - Go가 ARM에서 자동으로 ChaCha20 선택
func ServerTLSConfig() (*tls.Config, error) {
	cert, err := getCachedCert()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{*cert},
		NextProtos:   []string{"openfriend-quic"},
	}, nil
}

// ClientTLSConfig - 클라이언트용 TLS 설정 (인증서 검증 스킵)
func ClientTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"openfriend-quic"},
	}
}