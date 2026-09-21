// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGenerateWebhookCerts(t *testing.T) {
	scheme := runtime.NewScheme()
	err := corev1.AddToScheme(scheme)
	require.NoError(t, err)

	serviceName := "test-service"
	namespace := "test-namespace"
	clusterDomain := "cluster.local"
	secretName := "sandbox-webhook-certs"

	t.Run("successfully generates new certs when Secret does not exist", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "webhook-certs-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

		caPEM, err := generateWebhookCerts(t.Context(), fakeClient, tempDir, serviceName, namespace, clusterDomain)
		require.NoError(t, err)
		require.NotEmpty(t, caPEM)

		certPath := filepath.Join(tempDir, "tls.crt")
		keyPath := filepath.Join(tempDir, "tls.key")
		assert.FileExists(t, certPath)
		assert.FileExists(t, keyPath)

		certBytes, err := os.ReadFile(certPath)
		require.NoError(t, err)
		certBlock, _ := pem.Decode(certBytes)
		require.NotNil(t, certBlock)
		cert, err := x509.ParseCertificate(certBlock.Bytes)
		require.NoError(t, err)

		expectedDNSNames := []string{
			"test-service",
			"test-service.test-namespace",
			"test-service.test-namespace.svc",
			"test-service.test-namespace.svc.cluster.local",
		}
		assert.ElementsMatch(t, expectedDNSNames, cert.DNSNames)

		secret := &corev1.Secret{}
		err = fakeClient.Get(t.Context(), types.NamespacedName{Name: secretName, Namespace: namespace}, secret)
		require.NoError(t, err)
		assert.Equal(t, caPEM, secret.Data["ca.crt"])
		assert.NotEmpty(t, secret.Data["tls.crt"])
		assert.NotEmpty(t, secret.Data["tls.key"])
	})

	t.Run("successfully loads existing certs from Secret when it exists", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "webhook-certs-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		existingCA := []byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----")
		existingCert := []byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----")
		existingKey := []byte("-----BEGIN EC PRIVATE KEY-----\nMIIB\n-----END EC PRIVATE KEY-----")

		secret := &corev1.Secret{
			Name:      secretName,
			Namespace: namespace,
			Data: map[string][]byte{
				"ca.crt":  existingCA,
				"tls.crt": existingCert,
				"tls.key": existingKey,
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

		caPEM, err := generateWebhookCerts(t.Context(), fakeClient, tempDir, serviceName, namespace, clusterDomain)
		require.NoError(t, err)
		assert.Equal(t, existingCA, caPEM)

		certPath := filepath.Join(tempDir, "tls.crt")
		keyPath := filepath.Join(tempDir, "tls.key")

		certBytes, err := os.ReadFile(certPath)
		require.NoError(t, err)
		assert.Equal(t, existingCert, certBytes)

		keyBytes, err := os.ReadFile(keyPath)
		require.NoError(t, err)
		assert.Equal(t, existingKey, keyBytes)
	})

	t.Run("returns error when existing Secret has invalid certificate data", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "webhook-certs-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		secret := &corev1.Secret{
			Name:      secretName,
			Namespace: namespace,
			Data: map[string][]byte{
				"ca.crt":  []byte("invalid-ca-pem"),
				"tls.crt": []byte("invalid-cert-pem"),
				"tls.key": []byte("invalid-key-pem"),
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

		caPEM, err := generateWebhookCerts(t.Context(), fakeClient, tempDir, serviceName, namespace, clusterDomain)
		require.Error(t, err)
		assert.Nil(t, caPEM)
		assert.Contains(t, err.Error(), "has invalid certificate data")
	})
}

func TestGenerateWebhookCertsRenewsAnExpiringSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	for name, notAfter := range map[string]time.Time{
		"expired":       time.Now().Add(-24 * time.Hour),
		"expiring soon": time.Now().Add(certRenewBefore / 2),
	} {
		t.Run(name, func(t *testing.T) {
			certDir := t.TempDir()
			stale := &corev1.Secret{
				Name: "sandbox-webhook-certs", Namespace: "test-namespace",
				Data: certPairEndingAt(t, notAfter),
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stale).Build()

			caPEM, err := generateWebhookCerts(t.Context(), fakeClient, certDir, "test-service", "test-namespace", "cluster.local")
			require.NoError(t, err)
			assert.NotEqual(t, stale.Data["ca.crt"], caPEM, "an expiring pair must be replaced, not adopted")

			renewed := &corev1.Secret{}
			require.NoError(t, fakeClient.Get(t.Context(), types.NamespacedName{Name: "sandbox-webhook-certs", Namespace: "test-namespace"}, renewed))
			assert.Equal(t, caPEM, renewed.Data["ca.crt"], "the renewed CA must be the one shared through the Secret")
			served, err := os.ReadFile(filepath.Join(certDir, "tls.crt"))
			require.NoError(t, err)
			assert.Equal(t, renewed.Data["tls.crt"], served)
			assert.True(t, parseCert(t, served).NotAfter.After(time.Now().Add(certRenewBefore)), "the served cert must outlive the renewal window")
		})
	}
}

func TestGenerateWebhookCertsAdoptsAValidSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	certDir := t.TempDir()
	valid := &corev1.Secret{
		Name: "sandbox-webhook-certs", Namespace: "test-namespace",
		Data: certPairEndingAt(t, time.Now().Add(certValidity)),
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(valid).Build()

	caPEM, err := generateWebhookCerts(t.Context(), fakeClient, certDir, "test-service", "test-namespace", "cluster.local")
	require.NoError(t, err)
	assert.Equal(t, valid.Data["ca.crt"], caPEM)
	served, err := os.ReadFile(filepath.Join(certDir, "tls.crt"))
	require.NoError(t, err)
	assert.Equal(t, valid.Data["tls.crt"], served)
}

func TestPatchCRDs(t *testing.T) {
	scheme := runtime.NewScheme()
	err := apiextensionsv1.AddToScheme(scheme)
	require.NoError(t, err)

	makeCRD := func(name string, hasWebhook bool) *apiextensionsv1.CustomResourceDefinition {
		crd := &apiextensionsv1.CustomResourceDefinition{
			Name: name,
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Group: "agents.x-k8s.io",
				Names: apiextensionsv1.CustomResourceDefinitionNames{
					Kind: "Sandbox",
				},
				Scope: apiextensionsv1.NamespaceScoped,
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
					{
						Name:    "v1beta1",
						Served:  true,
						Storage: true,
					},
					{
						Name:    "v1alpha1",
						Served:  true,
						Storage: false,
					},
				},
			},
		}
		if hasWebhook {
			crd.Spec.Conversion = &apiextensionsv1.CustomResourceConversion{
				Strategy: apiextensionsv1.WebhookConverter,
				Webhook: &apiextensionsv1.WebhookConversion{
					ConversionReviewVersions: []string{"v1", "v1beta1"},
					ClientConfig: &apiextensionsv1.WebhookClientConfig{
						Service: &apiextensionsv1.ServiceReference{
							Name:      "old-service",
							Namespace: "old-namespace",
						},
						CABundle: []byte("old-ca"),
					},
				},
			}
		} else {
			crd.Spec.Conversion = &apiextensionsv1.CustomResourceConversion{
				Strategy: apiextensionsv1.NoneConverter,
			}
		}
		return crd
	}

	t.Run("successfully patches CRDs with Webhook strategy", func(t *testing.T) {
		crd1 := makeCRD("sandboxes.agents.x-k8s.io", true)
		crd2 := makeCRD("sandboxclaims.extensions.agents.x-k8s.io", true)

		crd4 := makeCRD("sandboxwarmpools.extensions.agents.x-k8s.io", false)

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(crd1, crd2, crd4).
			Build()

		caPEM := []byte("new-ca-pem")
		serviceName := "new-service"
		namespace := "new-namespace"

		err := patchCRDs(t.Context(), fakeClient, caPEM, serviceName, namespace, true)
		require.NoError(t, err)

		patchedCRD1 := &apiextensionsv1.CustomResourceDefinition{}
		err = fakeClient.Get(t.Context(), types.NamespacedName{Name: "sandboxes.agents.x-k8s.io"}, patchedCRD1)
		require.NoError(t, err)
		require.NotNil(t, patchedCRD1.Spec.Conversion)
		require.NotNil(t, patchedCRD1.Spec.Conversion.Webhook)
		assert.Equal(t, serviceName, patchedCRD1.Spec.Conversion.Webhook.ClientConfig.Service.Name)
		assert.Equal(t, namespace, patchedCRD1.Spec.Conversion.Webhook.ClientConfig.Service.Namespace)
		assert.Equal(t, "/convert", *patchedCRD1.Spec.Conversion.Webhook.ClientConfig.Service.Path)
		assert.Equal(t, caPEM, patchedCRD1.Spec.Conversion.Webhook.ClientConfig.CABundle)

		patchedCRD2 := &apiextensionsv1.CustomResourceDefinition{}
		err = fakeClient.Get(t.Context(), types.NamespacedName{Name: "sandboxclaims.extensions.agents.x-k8s.io"}, patchedCRD2)
		require.NoError(t, err)
		assert.Equal(t, serviceName, patchedCRD2.Spec.Conversion.Webhook.ClientConfig.Service.Name)
		assert.Equal(t, caPEM, patchedCRD2.Spec.Conversion.Webhook.ClientConfig.CABundle)

		patchedCRD4 := &apiextensionsv1.CustomResourceDefinition{}
		err = fakeClient.Get(t.Context(), types.NamespacedName{Name: "sandboxwarmpools.extensions.agents.x-k8s.io"}, patchedCRD4)
		require.NoError(t, err)
		assert.Equal(t, apiextensionsv1.NoneConverter, patchedCRD4.Spec.Conversion.Strategy)
		assert.Nil(t, patchedCRD4.Spec.Conversion.Webhook)
	})

	t.Run("skips extension CRDs when extensions disabled", func(t *testing.T) {
		crd1 := makeCRD("sandboxes.agents.x-k8s.io", true)
		crd2 := makeCRD("sandboxclaims.extensions.agents.x-k8s.io", true)

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(crd1, crd2).
			Build()

		err := patchCRDs(t.Context(), fakeClient, []byte("ca"), "svc", "ns", false)
		require.NoError(t, err)

		patchedCRD1 := &apiextensionsv1.CustomResourceDefinition{}
		require.NoError(t, fakeClient.Get(t.Context(), types.NamespacedName{Name: "sandboxes.agents.x-k8s.io"}, patchedCRD1))
		require.NotNil(t, patchedCRD1.Spec.Conversion.Webhook)
		assert.Equal(t, "svc", patchedCRD1.Spec.Conversion.Webhook.ClientConfig.Service.Name)

		untouchedCRD2 := &apiextensionsv1.CustomResourceDefinition{}
		require.NoError(t, fakeClient.Get(t.Context(), types.NamespacedName{Name: "sandboxclaims.extensions.agents.x-k8s.io"}, untouchedCRD2))
		assert.Equal(t, []byte("old-ca"), untouchedCRD2.Spec.Conversion.Webhook.ClientConfig.CABundle)
		assert.Equal(t, "old-service", untouchedCRD2.Spec.Conversion.Webhook.ClientConfig.Service.Name)
	})
}

func certPairEndingAt(t *testing.T, notAfter time.Time) map[string][]byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-service.test-namespace.svc"},
		NotBefore:             notAfter.Add(-certValidity),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return map[string][]byte{
		"ca.crt":  certPEM,
		"tls.crt": certPEM,
		"tls.key": pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}

func parseCert(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	return cert
}
