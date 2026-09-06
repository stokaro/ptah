package oci

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"

	"ptah.run/cmd/internal/cmdutil"
	"ptah.run/cmd/internal/dbcli"
	"ptah.run/internal/ocicredentials"
)

type loginOptions struct {
	username      string
	passwordStdin bool
	plainHTTP     bool
}

// newLoginCommand builds `ptah oci login`.
//
// There is deliberately no --password flag, and there will not be one: a
// credential passed on a command line lands in shell history and in the process
// list of every user on the machine. That rule is why Ptah had no login verb at
// all, and it is the wrong conclusion to draw from it -- a prompt and a stdin
// pipe put the secret in neither place (stokaro/ptah#2241).
func newLoginCommand() *cobra.Command {
	opts := loginOptions{}
	cmd := &cobra.Command{
		Use:   "login <registry>",
		Short: "Store a credential for a registry",
		Long: `Store a credential for a registry, after checking it works.

The credential is verified against the registry before anything is written, so a
typo fails here rather than at the next push.

Ptah writes to its own credential file, in Docker's format, under ` + "`~/.ptah`" + `.
A platform credential helper is used when one is available; otherwise the
credential is written to the file and this command says so. Credentials placed
by ` + "`docker login`" + ` keep working and are still read, so nothing that
authenticates today stops working.

The password is never taken from the command line. It is read from the terminal,
or from standard input with --password-stdin, so it does not reach shell history
or the process list. For a CI runner that should not have a login step at all,
set ` + ocicredentials.UsernameEnv + ` and ` + ocicredentials.PasswordEnv +
			` (or ` + ocicredentials.TokenEnv + `) instead.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLogin(cmd, args[0], opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.username, "username", "u", "", "Registry username (prompted for when omitted)")
	flags.BoolVar(&opts.passwordStdin, "password-stdin", false, "Read the password from standard input")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))
	return cmd
}

func newLogoutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logout <registry>",
		Short: "Remove the credential Ptah stored for a registry",
		Long: `Remove the credential Ptah stored for a registry.

Only Ptah's own store is touched. A credential placed by ` + "`docker login`" + ` is
left alone -- removing it is ` + "`docker logout`" + `'s job, and taking it away
here would log the user out of something they did not ask about.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLogout(cmd, args[0])
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))
	return cmd
}

func runLogin(cmd *cobra.Command, registryName string, opts loginOptions) error {
	if environmentAnswersFor(registryName) {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"Note: %s is set, and the environment answers before the stored credential. "+
				"The credential stored here will not be used until it is unset.\n",
			environmentSourceName())
	}

	cred, err := readCredential(cmd, opts)
	if err != nil {
		return err
	}

	// Validated before anything is written, so a typo fails here rather than at
	// the next push. The validation and the store are separate steps rather
	// than credentials.Login, because a store that refuses -- a keychain a
	// headless session cannot reach -- has to fall back to the file rather than
	// fail the login, and the two failures need different answers.
	if err := validateCredential(cmd.Context(), registryName, opts.plainHTTP, cred); err != nil {
		return fmt.Errorf("log in to %s: %w", registryName, err)
	}

	address := credentials.ServerAddressFromRegistry(registryName)
	storage, err := ocicredentials.Save(cmd.Context(), ocicredentials.Options{}, address, cred)
	if err != nil {
		return fmt.Errorf("log in to %s: %w", registryName, err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Logged in to %s\n", registryName)
	if storage.HelperError != nil {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"Warning: the platform credential helper refused the credential (%v), so it was written to the file instead.\n",
			storage.HelperError)
	}
	if storage.Plaintext {
		fmt.Fprintf(cmd.OutOrStdout(),
			"Credential stored in plaintext at %s, because no platform credential helper accepted it.\n",
			storage.Path)
		return nil
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Credential stored in a platform credential helper (%s).\n", storage.Path)
	return nil
}

// validateCredential is the check credentials.Login performs before storing,
// lifted out so a store failure and a bad password are different outcomes.
//
// The probe is built rather than copied from the caller's registry: a
// remote.Registry carries a mutex, so copying one is a vet finding and, on a
// registry that had already been used, a copied lock.
func validateCredential(ctx context.Context, registryName string, plainHTTP bool, cred auth.Credential) error {
	probe, err := remote.NewRegistry(registryName)
	if err != nil {
		return fmt.Errorf("read registry %q: %w", registryName, err)
	}
	probe.PlainHTTP = plainHTTP

	client := *auth.DefaultClient
	// No cache: the point of this call is to ask the registry now, and a cached
	// token from an earlier credential would answer for the new one.
	client.Cache = nil
	client.Credential = auth.StaticCredential(probe.Reference.Registry, cred)
	probe.Client = &client

	if err := probe.Ping(ctx); err != nil {
		return fmt.Errorf("the registry did not accept the credential: %w", err)
	}
	return nil
}

func runLogout(cmd *cobra.Command, registryName string) error {
	store, err := ocicredentials.PtahStore(ocicredentials.Options{})
	if err != nil {
		return err
	}
	if err := credentials.Logout(cmd.Context(), store, registryName); err != nil {
		return fmt.Errorf("log out of %s: %w", registryName, err)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Logged out of %s\n", registryName)
	return nil
}

// readCredential collects the username and password without either reaching argv.
func readCredential(cmd *cobra.Command, opts loginOptions) (auth.Credential, error) {
	username := strings.TrimSpace(opts.username)
	in := cmd.InOrStdin()
	out := cmd.ErrOrStderr()

	if opts.passwordStdin {
		if username == "" {
			return auth.Credential{}, fmt.Errorf("--password-stdin requires --username")
		}
		password, err := io.ReadAll(in)
		if err != nil {
			return auth.Credential{}, fmt.Errorf("read the password from standard input: %w", err)
		}
		// A piped secret usually arrives with the trailing newline of whatever
		// produced it, and a password with a newline glued on fails against the
		// registry in a way that reads like a wrong password.
		return auth.Credential{
			Username: username,
			Password: strings.TrimRight(string(password), "\r\n"),
		}, nil
	}

	if username == "" {
		prompted, err := promptLine(out, in, "Username: ")
		if err != nil {
			return auth.Credential{}, err
		}
		username = strings.TrimSpace(prompted)
		if username == "" {
			return auth.Credential{}, fmt.Errorf("a username is required")
		}
	}

	password, err := promptPassword(out, in)
	if err != nil {
		return auth.Credential{}, err
	}
	if password == "" {
		return auth.Credential{}, fmt.Errorf("a password is required")
	}
	return auth.Credential{Username: username, Password: password}, nil
}

func promptLine(out io.Writer, in io.Reader, label string) (string, error) {
	fmt.Fprint(out, label)
	var line string
	if _, err := fmt.Fscanln(in, &line); err != nil {
		return "", fmt.Errorf("read %s: %w", strings.TrimSuffix(strings.ToLower(label), ": "), err)
	}
	return line, nil
}

// promptPassword reads the secret without echoing it.
//
// A terminal gets the no-echo read. Anything else -- a pipe, a test -- gets a
// plain read, because there is no terminal to turn echo off on; that path is
// also what makes this command testable without a pty.
func promptPassword(out io.Writer, in io.Reader) (string, error) {
	if file, ok := in.(*os.File); ok && term.IsTerminal(int(file.Fd())) {
		fmt.Fprint(out, "Password: ")
		secret, err := term.ReadPassword(int(file.Fd()))
		fmt.Fprintln(out)
		if err != nil {
			return "", fmt.Errorf("read the password: %w", err)
		}
		return string(secret), nil
	}
	line, err := promptLine(out, in, "Password: ")
	if err != nil {
		return "", err
	}
	return line, nil
}

// environmentAnswersFor reports whether the environment would answer for this
// registry ahead of anything this command stores.
//
// It asks the environment source rather than the whole chain. A chain lookup
// answers from the Ptah or Docker store when the environment is scoped to a
// different registry, and the note below would then describe a source that has
// nothing to do with this registry.
func environmentAnswersFor(registryName string) bool {
	answers, err := ocicredentials.EnvironmentAnswersFor(
		ocicredentials.Options{}, credentials.ServerAddressFromRegistry(registryName))
	return err == nil && answers
}

func environmentSourceName() string {
	if strings.TrimSpace(os.Getenv(ocicredentials.TokenEnv)) != "" {
		return ocicredentials.TokenEnv
	}
	return ocicredentials.UsernameEnv
}
