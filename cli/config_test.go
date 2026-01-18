package cli

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigFile(t *testing.T) {
	// Create a temporary directory for test config files
	tempDir := t.TempDir()

	tests := []struct {
		name        string
		configYAML  string
		env         string
		wantErr     bool
		errContains string
		wantDriver  string
		wantDSN     string
		wantTable   string
	}{
		{
			name: "locked config",
			configYAML: `config_locked: true
development:
  driver: postgres
  dsn: postgres://localhost/dev
`,
			wantErr:     true,
			errContains: "config file is locked",
		},
		{
			name: "unlocked config with environment",
			configYAML: `config_locked: false
development:
  driver: postgres
  dsn: postgres://localhost/dev
  table: custom_migrations
`,
			env:        "development",
			wantDriver: "postgres",
			wantDSN:    "postgres://localhost/dev",
			wantTable:  "custom_migrations",
		},
		{
			name: "missing environment",
			configYAML: `config_locked: false
development:
  driver: postgres
  dsn: postgres://localhost/dev
`,
			env:         "production",
			wantErr:     true,
			errContains: "environment 'production' not found",
		},
		{
			name: "environment requires unlock",
			configYAML: `config_locked: false
production:
  driver: postgres
  dsn: postgres://localhost/prod
  require_explicit_unlock: true
`,
			env:         "production",
			wantErr:     true,
			errContains: "requires --unlock-production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Change to temp directory
			oldWd, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}
			defer os.Chdir(oldWd)

			testDir := filepath.Join(tempDir, tt.name)
			if err := os.MkdirAll(testDir, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chdir(testDir); err != nil {
				t.Fatal(err)
			}

			// Write config file
			if err := os.WriteFile(".queen.yaml", []byte(tt.configYAML), 0644); err != nil {
				t.Fatal(err)
			}

			// Create app with config
			app := &App{
				config: &Config{
					UseConfig: true,
					Env:       tt.env,
					Table:     "queen_migrations", // default
				},
			}

			err = app.loadConfigFile()

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errContains)
					return
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if tt.wantDriver != "" && app.config.Driver != tt.wantDriver {
				t.Errorf("driver = %q, want %q", app.config.Driver, tt.wantDriver)
			}
			if tt.wantDSN != "" && app.config.DSN != tt.wantDSN {
				t.Errorf("dsn = %q, want %q", app.config.DSN, tt.wantDSN)
			}
			if tt.wantTable != "" && app.config.Table != tt.wantTable {
				t.Errorf("table = %q, want %q", app.config.Table, tt.wantTable)
			}
		})
	}
}

func TestLoadEnv(t *testing.T) {
	// Save and restore environment
	oldDriver := os.Getenv("QUEEN_DRIVER")
	oldDSN := os.Getenv("QUEEN_DSN")
	oldTable := os.Getenv("QUEEN_TABLE")
	defer func() {
		os.Setenv("QUEEN_DRIVER", oldDriver)
		os.Setenv("QUEEN_DSN", oldDSN)
		os.Setenv("QUEEN_TABLE", oldTable)
	}()

	t.Run("loads from environment", func(t *testing.T) {
		os.Setenv("QUEEN_DRIVER", "postgres")
		os.Setenv("QUEEN_DSN", "postgres://localhost/test")
		os.Setenv("QUEEN_TABLE", "custom_table")

		app := &App{
			config: &Config{
				Table: "queen_migrations", // default
			},
		}

		app.loadEnv()

		if app.config.Driver != "postgres" {
			t.Errorf("driver = %q, want %q", app.config.Driver, "postgres")
		}
		if app.config.DSN != "postgres://localhost/test" {
			t.Errorf("dsn = %q, want %q", app.config.DSN, "postgres://localhost/test")
		}
		if app.config.Table != "custom_table" {
			t.Errorf("table = %q, want %q", app.config.Table, "custom_table")
		}
	})

	t.Run("flags override env", func(t *testing.T) {
		os.Setenv("QUEEN_DRIVER", "postgres")
		os.Setenv("QUEEN_DSN", "postgres://localhost/test")

		app := &App{
			config: &Config{
				Driver: "mysql",                     // set by flag
				DSN:    "mysql://localhost/test",    // set by flag
				Table:  "queen_migrations",          // default
			},
		}

		app.loadEnv()

		// Flags should not be overwritten
		if app.config.Driver != "mysql" {
			t.Errorf("driver = %q, want %q (flag should win)", app.config.Driver, "mysql")
		}
		if app.config.DSN != "mysql://localhost/test" {
			t.Errorf("dsn = %q, want %q (flag should win)", app.config.DSN, "mysql://localhost/test")
		}
	})
}

func TestRequiresConfirmation(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
		want   bool
	}{
		{
			name: "yes flag skips confirmation",
			config: &Config{
				Yes: true,
				Env: "production",
				configFile: &ConfigFile{
					Environments: map[string]*Environment{
						"production": {RequireConfirmation: true},
					},
				},
			},
			want: false,
		},
		{
			name: "no config file",
			config: &Config{
				Env: "production",
			},
			want: false,
		},
		{
			name: "environment requires confirmation",
			config: &Config{
				Env: "staging",
				configFile: &ConfigFile{
					Environments: map[string]*Environment{
						"staging": {RequireConfirmation: true},
					},
				},
			},
			want: true,
		},
		{
			name: "environment does not require confirmation",
			config: &Config{
				Env: "development",
				configFile: &ConfigFile{
					Environments: map[string]*Environment{
						"development": {RequireConfirmation: false},
					},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &App{config: tt.config}
			got := app.requiresConfirmation()
			if got != tt.want {
				t.Errorf("requiresConfirmation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetEnvironmentName(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
		want   string
	}{
		{
			name:   "returns environment name",
			config: &Config{Env: "production"},
			want:   "production",
		},
		{
			name:   "returns custom when no environment",
			config: &Config{},
			want:   "custom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &App{config: tt.config}
			got := app.getEnvironmentName()
			if got != tt.want {
				t.Errorf("getEnvironmentName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEnvironmentLockTimeout(t *testing.T) {
	tempDir := t.TempDir()

	configYAML := `config_locked: false
development:
  driver: postgres
  dsn: postgres://localhost/dev
  lock_timeout: 1h30m
`

	// Change to temp directory
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldWd)

	if err := os.Chdir(tempDir); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(".queen.yaml", []byte(configYAML), 0644); err != nil {
		t.Fatal(err)
	}

	app := &App{
		config: &Config{
			UseConfig: true,
			Env:       "development",
			Table:     "queen_migrations",
		},
	}

	if err := app.loadConfigFile(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := 90 * time.Minute
	if app.config.LockTimeout != expected {
		t.Errorf("lock_timeout = %v, want %v", app.config.LockTimeout, expected)
	}
}

// contains checks if s contains substr
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
