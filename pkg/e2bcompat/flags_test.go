package e2bcompat

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerOptionsRefuseBuildsThatCouldNeverRun(t *testing.T) {
	secret := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(secret, []byte(testEnvdSecret), 0o600))
	for name, tc := range map[string]struct {
		parallel int
		timeout  time.Duration
		stores   bool
		ok       bool
	}{
		"defaults":          {2, 30 * time.Minute, false, true},
		"no slot":           {0, 30 * time.Minute, false, false},
		"no timeout":        {2, 0, false, false},
		"two upload stores": {2, 30 * time.Minute, true, false},
	} {
		f := NewFlags()
		f.EnvdSecretFile, f.Builds, f.BuildParallel, f.BuildTimeout = secret, true, tc.parallel, tc.timeout
		if tc.stores {
			f.BuildDir, f.BuildS3.Bucket = t.TempDir(), "b"
		}
		opts, err := f.ServerOptions(t.Context(), nil)
		if tc.ok {
			require.NoError(t, err, name)
			assert.Equal(t, BuildOptions{Parallel: 2, Timeout: 30 * time.Minute, LogLines: 10000}, opts.Builds, name)
		} else {
			assert.Error(t, err, name)
		}
	}
}
