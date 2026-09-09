//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

const (
	// composeFile is resolved relative to the package directory, which is
	// where `go test` runs.
	composeFile = "docker-compose.e2e.yml"
	// dockerTimeout bounds any single docker call. Image build and broker
	// start are the slow ones.
	dockerTimeout = 10 * time.Minute
)

// project namespaces the whole stack. Fixed rather than random so that a run
// killed halfway can be cleaned up by hand with `make e2e-clean`; the cost is
// that two runs cannot go at once.
func project() string {
	if p := os.Getenv("WEB_E2E_PROJECT"); p != "" {
		return p
	}
	return "web-e2e"
}

type stack struct {
	t *testing.T
}

// startStack brings up the broker and hands back a handle to the stack. The web
// service is deliberately NOT started here: a regex consumer only looks for new
// topics when it refreshes metadata, so a test that creates its topic first and
// starts the app second gets discovery immediately instead of up to
// MetadataMaxAge later.
func startStack(t *testing.T) *stack {
	t.Helper()
	requireDocker(t)
	s := &stack{t: t}

	// Registered first, so it runs last: everything else unwinds before the
	// stack is destroyed.
	t.Cleanup(func() {
		if t.Failed() {
			s.dumpLogs()
		}
		s.compose("down", "-v", "--remove-orphans", "--timeout", "10")
	})

	// A previous run killed with ctrl-C leaves containers behind, and a stale
	// broker would serve stale topic IDs into this run.
	s.compose("down", "-v", "--remove-orphans", "--timeout", "10")
	s.compose("up", "-d", "--wait", "kafka")
	return s
}

func requireDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed; skipping e2e")
	}
	if out, err := exec.Command("docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil {
		t.Skipf("docker daemon is not reachable, skipping e2e: %v: %s", err, out)
	}
}

// compose runs a docker compose subcommand against this stack and fails the
// test if it errors. Every docker call in this package goes through here or
// through docker(), so failures always carry the command and its output.
func (s *stack) compose(args ...string) string {
	s.t.Helper()
	full := append([]string{"compose", "-p", project(), "-f", composeFile}, args...)
	return s.docker(full...)
}

func (s *stack) docker(args ...string) string {
	s.t.Helper()
	out, err := s.dockerErr(args...)
	if err != nil {
		s.t.Fatalf("docker %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// dockerErr is the variant for calls whose failure is a legitimate outcome —
// describing a topic that should no longer exist, for instance.
func (s *stack) dockerErr(args ...string) (string, error) {
	s.t.Helper()
	cmd := exec.Command("docker", args...)
	done := make(chan struct{})
	timer := time.AfterFunc(dockerTimeout, func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		close(done)
	})
	out, err := cmd.CombinedOutput()
	if timer.Stop() {
		close(done)
	}
	<-done
	return string(out), err
}

// containerID resolves a compose service to the container docker commands take.
// The compose file sets no container_name precisely so that these are
// project-scoped and can never be the real `kafka` or `orderbook-web`.
func (s *stack) containerID(service string) string {
	s.t.Helper()
	id, err := s.containerIDErr(service)
	if err != nil {
		s.t.Fatal(err)
	}
	return id
}

// containerIDErr is the non-fatal variant, for the paths that run while the
// stack may be half-built. Cleanup and log-dumping must never call Fatal: doing
// so replaces the real failure with a confusing second one, which is exactly
// what hid a broken compose file the first time this ran.
func (s *stack) containerIDErr(service string) (string, error) {
	s.t.Helper()
	out, err := s.dockerErr("compose", "-p", project(), "-f", composeFile, "ps", "-q", service)
	if err != nil {
		return "", fmt.Errorf("resolving container for %q: %w: %s", service, err, out)
	}
	if id := strings.TrimSpace(out); id != "" {
		return id, nil
	}
	return "", fmt.Errorf("service %q has no container; is the stack up?", service)
}

func (s *stack) startWeb() {
	s.t.Helper()
	s.compose("up", "-d", "--build", "web")
	s.waitForLog("web", time.Time{}, "Order book UI:", 5*time.Minute)
}

func (s *stack) pause(service string)   { s.t.Helper(); s.docker("pause", s.containerID(service)) }
func (s *stack) unpause(service string) { s.t.Helper(); s.docker("unpause", s.containerID(service)) }

// unpauseQuietly is the cleanup form: it must not fail a test that is already
// failing, and it must cope with a container that was never paused or never
// created.
func (s *stack) unpauseQuietly(service string) {
	s.t.Helper()
	id, err := s.containerIDErr(service)
	if err != nil {
		return
	}
	_, _ = s.dockerErr("unpause", id)
}

// logsSince returns a service's output since a moment. A zero time means all of
// it. Docker's --since takes whole seconds, so callers who need to separate two
// phases of a test must leave more than a second between them — every caller
// here is separated by the pause window, which is far longer.
func (s *stack) logsSince(service string, since time.Time) string {
	s.t.Helper()
	args := []string{"logs"}
	if !since.IsZero() {
		args = append(args, "--since", since.UTC().Format(time.RFC3339))
	}
	// Failure is normal here: the container may not exist yet, a paused one can
	// refuse, and every caller is either polling or dumping diagnostics.
	id, err := s.containerIDErr(service)
	if err != nil {
		return ""
	}
	out, _ := s.dockerErr(append(args, id)...)
	return out
}

// waitForLog polls a service's logs until want appears, and fails with the log
// tail if it never does. This is used instead of sleeps throughout: a fixed
// sleep is how an e2e test becomes flaky.
func (s *stack) waitForLog(service string, since time.Time, want string, timeout time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(s.logsSince(service, since), want) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.t.Fatalf("%q never appeared in %s logs within %s\n--- logs ---\n%s",
		want, service, timeout, tail(s.logsSince(service, since), 40))
}

// requireLogged asserts on logs already collected, for the lines that must have
// happened on the way to a state rather than being the state itself.
func (s *stack) requireLogged(logs, want, why string) {
	s.t.Helper()
	if !strings.Contains(logs, want) {
		s.t.Fatalf("expected %q in the logs — %s\n--- logs ---\n%s", want, why, tail(logs, 40))
	}
}

func (s *stack) dumpLogs() {
	s.t.Helper()
	for _, svc := range []string{"web", "kafka"} {
		s.t.Logf("=== %s logs (tail) ===\n%s", svc, tail(s.logsSince(svc, time.Time{}), 60))
	}
}

// --- Kafka, driven exactly as the manual reproduction drove it ---

func (s *stack) kafkaTopics(args ...string) (string, error) {
	s.t.Helper()
	full := append([]string{
		"exec", s.containerID("kafka"),
		"kafka-topics", "--bootstrap-server", "localhost:9092",
	}, args...)
	return s.dockerErr(full...)
}

func (s *stack) createTopic(topic string) {
	s.t.Helper()
	out, err := s.kafkaTopics("--create", "--topic", topic,
		"--partitions", "1", "--replication-factor", "1",
		"--config", "retention.ms=3600000")
	if err != nil {
		s.t.Fatalf("creating %s failed: %v\n%s", topic, err, out)
	}
}

func (s *stack) deleteTopic(topic string) {
	s.t.Helper()
	if out, err := s.kafkaTopics("--delete", "--topic", topic); err != nil {
		s.t.Fatalf("deleting %s failed: %v\n%s", topic, err, out)
	}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		out, err := s.kafkaTopics("--list")
		if err == nil && !strings.Contains(out, topic) {
			return
		}
		time.Sleep(time.Second)
	}
	s.t.Fatalf("%s still listed 90s after delete", topic)
}

var topicIDRe = regexp.MustCompile(`TopicId:\s*(\S+)`)

// topicID is the whole point of these tests: it is what franz-go pins at cursor
// creation and refuses to re-adopt.
func (s *stack) topicID(topic string) string {
	s.t.Helper()
	out, err := s.kafkaTopics("--describe", "--topic", topic)
	if err != nil {
		s.t.Fatalf("describing %s failed: %v\n%s", topic, err, out)
	}
	m := topicIDRe.FindStringSubmatch(out)
	if m == nil {
		s.t.Fatalf("no TopicId in describe output for %s:\n%s", topic, out)
	}
	return m[1]
}

func (s *stack) produce(topic, msg string) error {
	s.t.Helper()
	cmd := exec.Command("docker", "exec", "-i", s.containerID("kafka"),
		"kafka-console-producer", "--bootstrap-server", "localhost:9092", "--topic", topic)
	cmd.Stdin = strings.NewReader(msg + "\n")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, out)
	}
	return nil
}

// produceUntilLog keeps writing a record until want shows up in the web app's
// logs. Producing ONCE is not enough and this is the subtlest part of the whole
// test: the consumers read from the LATEST offset, so a record written before a
// cursor exists is never seen. After a purge the cursor is rebuilt at the end of
// the topic, so recovery can only be observed by writing again afterwards.
func (s *stack) produceUntilLog(topic, msg string, since time.Time, want string, timeout time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(s.logsSince("web", since), want) {
			return
		}
		if err := s.produce(topic, msg); err != nil {
			s.t.Logf("produce to %s failed (retrying): %v", topic, err)
		}
		time.Sleep(2 * time.Second)
	}
	s.t.Fatalf("%q never appeared in web logs within %s despite producing to %s\n--- logs ---\n%s",
		want, timeout, topic, tail(s.logsSince("web", since), 40))
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
