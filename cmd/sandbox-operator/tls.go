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
	"cmp"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	webhookSecretName = "sandbox-webhook-certs"
	certValidity      = 365 * 24 * time.Hour

	caCertKey     = "ca.crt"
	tlsCertKey    = "tls.crt"
	tlsPrivateKey = "tls.key"
)

// generateWebhookCerts generates a self-signed CA and a server certificate signed by that CA,
// or loads them from a shared Kubernetes Secret if it already exists.
// It writes the server certificate (tls.crt) and key (tls.key) to the certDir.
// It returns the PEM-encoded CA certificate, which is the caBundle to patch into the CRDs.
func generateWebhookCerts(ctx context.Context, c client.Client, certDir string, serviceName, namespace, clusterDomain string) ([]byte, error) {
	secret := &corev1.Secret{}
	getErr := c.Get(ctx, types.NamespacedName{Name: webhookSecretName, Namespace: namespace}, secret)
	if getErr == nil {
		setupLog.Info("Found existing shared webhook certificates in Secret", "secret", webhookSecretName)
		return adoptSecretCerts(secret, certDir)
	}
	if !errors.IsNotFound(getErr) {
		return nil, fmt.Errorf("check for existing shared Secret: %w", getErr)
	}

	setupLog.Info("No shared webhook certificates found; generating new ones")
	caPEM, serverPEM, serverKeyPEM, err := issueSelfSignedPair(serviceName, namespace, clusterDomain)
	if err != nil {
		return nil, err
	}
	if err := writeCertFiles(certDir, serverPEM, serverKeyPEM); err != nil {
		return nil, fmt.Errorf("write certificate files locally: %w", err)
	}
	return publishSharedSecret(ctx, c, namespace, certDir, caPEM, serverPEM, serverKeyPEM)
}

// adoptSecretCerts validates the shared Secret's material and installs it in certDir.
func adoptSecretCerts(secret *corev1.Secret, certDir string) ([]byte, error) {
	caPEM, serverPEM, serverKeyPEM := secret.Data[caCertKey], secret.Data[tlsCertKey], secret.Data[tlsPrivateKey]
	if err := validatePEMBytes(caPEM, serverPEM, serverKeyPEM); err != nil {
		return nil, fmt.Errorf("shared Secret %s has invalid certificate data: %w", webhookSecretName, err)
	}
	if err := writeCertFiles(certDir, serverPEM, serverKeyPEM); err != nil {
		return nil, fmt.Errorf("write certificate files locally: %w", err)
	}
	return caPEM, nil
}

// issueSelfSignedPair mints a CA and a server certificate for the webhook service.
func issueSelfSignedPair(serviceName, namespace, clusterDomain string) (caPEM, serverPEM, serverKeyPEM []byte, err error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate CA private key: %w", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "sandbox-conversion-webhook-ca"},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(certValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	caBytes, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create CA certificate: %w", err)
	}
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caBytes})

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate server private key: %w", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: fmt.Sprintf("%s.%s.svc", serviceName, namespace)},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(certValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames: []string{
			serviceName,
			fmt.Sprintf("%s.%s", serviceName, namespace),
			fmt.Sprintf("%s.%s.svc", serviceName, namespace),
			fmt.Sprintf("%s.%s.svc.%s", serviceName, namespace, clusterDomain),
		},
	}
	serverBytes, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create server certificate: %w", err)
	}
	serverPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverBytes})

	serverKeyBytes, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal server private key: %w", err)
	}
	return caPEM, serverPEM, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyBytes}), nil
}

// publishSharedSecret stores the freshly issued material so every replica agrees
// on one CA. A concurrent replica winning the create is normal: adopt its certs.
func publishSharedSecret(ctx context.Context, c client.Client, namespace, certDir string, caPEM, serverPEM, serverKeyPEM []byte) ([]byte, error) {
	setupLog.Info("Creating shared webhook certificates Secret", "secret", webhookSecretName)
	err := c.Create(ctx, &corev1.Secret{
		Name: webhookSecretName, Namespace: namespace,
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			caCertKey:     caPEM,
			tlsCertKey:    serverPEM,
			tlsPrivateKey: serverKeyPEM,
		},
	})
	if err == nil {
		return caPEM, nil
	}
	if !errors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create shared Secret: %w", err)
	}

	setupLog.Info("Shared Secret was created concurrently by another replica; loading it", "secret", webhookSecretName)
	winner := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: webhookSecretName, Namespace: namespace}, winner); err != nil {
		return nil, fmt.Errorf("get concurrently created Secret: %w", err)
	}
	return adoptSecretCerts(winner, certDir)
}

// validatePEMBytes verifies that the provided certificate slices contain valid PEM blocks.
func validatePEMBytes(caPEM, serverPEM, serverKeyPEM []byte) error {
	if len(caPEM) == 0 || len(serverPEM) == 0 || len(serverKeyPEM) == 0 {
		return fmt.Errorf("missing certificate or key data")
	}
	if p, _ := pem.Decode(caPEM); p == nil {
		return fmt.Errorf("invalid CA certificate PEM data")
	}
	if p, _ := pem.Decode(serverPEM); p == nil {
		return fmt.Errorf("invalid server certificate PEM data")
	}
	if p, _ := pem.Decode(serverKeyPEM); p == nil {
		return fmt.Errorf("invalid server private key PEM data")
	}
	return nil
}

// writeCertFiles writes the server certificate and key to the local certDir.
func writeCertFiles(certDir string, serverPEM, serverKeyPEM []byte) error {
	if err := os.MkdirAll(certDir, 0o750); err != nil {
		return err
	}

	certPath := filepath.Join(certDir, tlsCertKey)
	keyPath := filepath.Join(certDir, tlsPrivateKey)

	if err := os.WriteFile(certPath, serverPEM, 0o600); err != nil {
		return err
	}

	if err := os.WriteFile(keyPath, serverKeyPEM, 0o600); err != nil {
		return err
	}

	return nil
}

// patchCRDs patches the CRDs in the cluster with the generated CA certificate and service details using a merge patch.
// The extension conversion webhooks are only registered when extensions are enabled, so their CRD caBundles must not be
// patched otherwise: patching them would point the apiserver at a /convert endpoint this process never serves, breaking
// every v1alpha1<->v1beta1 read/write of the extension CRs.
func patchCRDs(ctx context.Context, c client.Client, caPEM []byte, serviceName, namespace string, extensions bool) error {
	crdNames := []string{
		"sandboxes.agents.x-k8s.io",
	}
	if extensions {
		crdNames = append(crdNames,
			"sandboxclaims.extensions.agents.x-k8s.io",
			"sandboxtemplates.extensions.agents.x-k8s.io",
			"sandboxwarmpools.extensions.agents.x-k8s.io",
		)
	}

	for _, name := range crdNames {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		err := c.Get(ctx, types.NamespacedName{Name: name}, crd)
		if err != nil {
			if errors.IsNotFound(err) {
				setupLog.Info("CRD not found, skipping patch", "crd", name)
				continue
			}
			return fmt.Errorf("get CRD %s: %w", name, err)
		}

		if crd.Spec.Conversion == nil || crd.Spec.Conversion.Strategy != apiextensionsv1.WebhookConverter {
			setupLog.Info("CRD does not use Webhook conversion strategy, skipping patch", "crd", name)
			continue
		}

		original := crd.DeepCopy()

		webhook := crd.Spec.Conversion.Webhook
		webhook = cmp.Or(webhook, &apiextensionsv1.WebhookConversion{})

		if len(webhook.ConversionReviewVersions) == 0 {
			webhook.ConversionReviewVersions = []string{"v1", "v1beta1"}
		}

		webhook.ClientConfig = cmp.Or(webhook.ClientConfig, &apiextensionsv1.WebhookClientConfig{})

		webhook.ClientConfig.Service = cmp.Or(webhook.ClientConfig.Service, &apiextensionsv1.ServiceReference{})

		webhook.ClientConfig.Service.Name = serviceName
		webhook.ClientConfig.Service.Namespace = namespace
		path := "/convert"
		webhook.ClientConfig.Service.Path = &path
		webhook.ClientConfig.CABundle = caPEM

		crd.Spec.Conversion.Webhook = webhook

		// Use Patch with MergeFrom to avoid write conflicts and managedFields issues
		if err := c.Patch(ctx, crd, client.MergeFrom(original)); err != nil {
			return fmt.Errorf("patch CRD %s: %w", name, err)
		}

		setupLog.Info("Successfully patched CRD with webhook configuration", "crd", name)
	}

	return nil
}
