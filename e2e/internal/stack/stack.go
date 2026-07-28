// Package stack boots the five containers an integration test needs —
// postgres, kafka, schema-registry, jobmanager, taskmanager — on a private
// Docker network, and reports the addresses they ended up on.
//
// The suite owns its stack: it is the only writer of the raw topics, so a live
// feed cannot corrupt a scenario. Nothing here is shared with the dev compose
// stack and no port is fixed except Kafka's, which cannot be helped (see
// startKafka).
package stack

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"orderbook-e2e/internal/domain"
	"orderbook-e2e/internal/flink"
)

// Images are pinned to the versions the dev stack runs, so the pipeline is
// exercised against the brokers and registry it will meet in production.
const (
	kafkaImage    = "confluentinc/cp-kafka:7.8.0"
	registryImage = "confluentinc/cp-schema-registry:7.6.1"
	postgresImage = "postgres:18"

	flinkImageRepo = "tibobit-e2e-flink"
	flinkImageTag  = "latest"

	startupTimeout = 3 * time.Minute
	taskSlots      = "8"
)

// Container network aliases. Every job self-configures from env with in-network
// defaults (kafka:29092, http://schema-registry:8082,
// jdbc:postgresql://postgres:5432/markets), so these names are a contract:
// change one and every job breaks silently. They double as the keys Logs takes.
const (
	Postgres       = "postgres"
	Kafka          = "kafka"
	SchemaRegistry = "schema-registry"
	JobManager     = "jobmanager"
	TaskManager    = "taskmanager"
)

// Stack is a running environment. Terminate it when the scenario is done.
type Stack struct {
	Endpoints domain.Endpoints

	network    *testcontainers.DockerNetwork
	containers []named
}

// named pairs a container with its network alias so its logs can be found
// again. Without the name, a failed run can only report "one of these five
// containers said something".
type named struct {
	name string
	c    testcontainers.Container
}

// Start brings the whole stack up in dependency order and returns once the
// task manager has registered with the job manager.
func Start(ctx context.Context, repoRoot string) (*Stack, error) {
	network, err := tcnetwork.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create network: %w", err)
	}
	s := &Stack{network: network}

	if err := s.start(ctx, repoRoot, network.Name); err != nil {
		_ = s.Terminate(context.WithoutCancel(ctx))
		return nil, err
	}
	return s, nil
}

func (s *Stack) start(ctx context.Context, repoRoot, network string) error {
	// Postgres first: the jobs' lookups read it, and its init scripts run
	// during startup so the seeded precisions and rebases are in place before
	// anything can query them.
	if err := s.startPostgres(ctx, repoRoot, network); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}

	broker, err := s.startKafka(ctx, network)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	s.Endpoints.KafkaBroker = broker

	registryURL, err := s.startSchemaRegistry(ctx, network)
	if err != nil {
		return fmt.Errorf("schema-registry: %w", err)
	}
	s.Endpoints.SchemaRegistryURL = registryURL

	flinkAPI, err := s.startJobManager(ctx, repoRoot, network)
	if err != nil {
		return fmt.Errorf("jobmanager: %w", err)
	}
	s.Endpoints.FlinkAPI = flinkAPI

	if err := s.startTaskManager(ctx, repoRoot, network); err != nil {
		return fmt.Errorf("taskmanager: %w", err)
	}

	// The job manager serves REST long before it has any slots; submitting
	// against a slotless cluster just leaves jobs unscheduled.
	if err := flink.New(flinkAPI).WaitForTaskManagers(ctx, 1, startupTimeout); err != nil {
		return fmt.Errorf("taskmanager registration: %w", err)
	}
	return nil
}

func (s *Stack) startPostgres(ctx context.Context, repoRoot, network string) error {
	initDir := filepath.Join(repoRoot, "postgres")
	files := []testcontainers.ContainerFile{}
	for _, name := range []string{"01_schema.sql", "02_seed.sql"} {
		files = append(files, testcontainers.ContainerFile{
			HostFilePath:      filepath.Join(initDir, name),
			ContainerFilePath: "/docker-entrypoint-initdb.d/" + name,
			FileMode:          0o644,
		})
	}

	_, err := s.run(ctx, Postgres, testcontainers.ContainerRequest{
		Image:          postgresImage,
		Hostname:       Postgres,
		Networks:       []string{network},
		NetworkAliases: map[string][]string{network: {Postgres}},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			// 01_schema.sql creates and populates the `markets` database that
			// the jobs actually connect to.
			"POSTGRES_DB": "postgres",
		},
		Files: files,
		// Postgres logs this once for the temporary server that runs the init
		// scripts and again for the real one; the second means the seed landed.
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(startupTimeout),
	})
	return err
}

// startKafka publishes the broker on a free host port chosen up front.
//
// A Kafka listener has to advertise the address clients will reach it on, and
// that has to be baked into the container's environment before it starts — so
// unlike every other service here, the port cannot be left to Docker to pick.
func (s *Stack) startKafka(ctx context.Context, network string) (string, error) {
	hostPort, err := freePort()
	if err != nil {
		return "", err
	}

	_, err = s.run(ctx, Kafka, testcontainers.ContainerRequest{
		Image:          kafkaImage,
		Hostname:       Kafka,
		Networks:       []string{network},
		NetworkAliases: map[string][]string{network: {Kafka}},
		ExposedPorts:   []string{fmt.Sprintf("%d:9092/tcp", hostPort)},
		Env: map[string]string{
			"KAFKA_NODE_ID":       "1",
			"KAFKA_PROCESS_ROLES": "broker,controller",
			"KAFKA_LISTENERS":     "PLAINTEXT://kafka:29092,CONTROLLER://kafka:9093,PLAINTEXT_HOST://0.0.0.0:9092",
			"KAFKA_ADVERTISED_LISTENERS": fmt.Sprintf(
				"PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:%d", hostPort),
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@kafka:9093",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"CLUSTER_ID":                                     "MkU3OEVBNTcwNTJENDM2Qg==",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("9092/tcp"),
			wait.ForLog("Kafka Server started"),
		).WithStartupTimeoutDefault(startupTimeout),
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("localhost:%d", hostPort), nil
}

func (s *Stack) startSchemaRegistry(ctx context.Context, network string) (string, error) {
	c, err := s.run(ctx, SchemaRegistry, testcontainers.ContainerRequest{
		Image:          registryImage,
		Hostname:       SchemaRegistry,
		Networks:       []string{network},
		NetworkAliases: map[string][]string{network: {SchemaRegistry}},
		ExposedPorts:   []string{"8082/tcp"},
		Env: map[string]string{
			"SCHEMA_REGISTRY_HOST_NAME":                    SchemaRegistry,
			"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": "kafka:29092",
			"SCHEMA_REGISTRY_LISTENERS":                    "http://0.0.0.0:8082",
		},
		WaitingFor: wait.ForHTTP("/subjects").WithPort("8082/tcp").WithStartupTimeout(startupTimeout),
	})
	if err != nil {
		return "", err
	}
	return hostURL(ctx, c, "8082/tcp")
}

func (s *Stack) startJobManager(ctx context.Context, repoRoot, network string) (string, error) {
	c, err := s.run(ctx, JobManager, testcontainers.ContainerRequest{
		FromDockerfile: clusterImage(repoRoot),
		Hostname:       JobManager,
		Networks:       []string{network},
		NetworkAliases: map[string][]string{network: {JobManager}},
		Cmd:            []string{"jobmanager"}, // Flink's entrypoint arg, not the alias
		ExposedPorts:   []string{"8081/tcp"},
		Env:            map[string]string{"FLINK_PROPERTIES": "jobmanager.rpc.address: jobmanager\n"},
		WaitingFor:     wait.ForHTTP("/overview").WithPort("8081/tcp").WithStartupTimeout(startupTimeout),
	})
	if err != nil {
		return "", err
	}
	return hostURL(ctx, c, "8081/tcp")
}

func (s *Stack) startTaskManager(ctx context.Context, repoRoot, network string) error {
	_, err := s.run(ctx, TaskManager, testcontainers.ContainerRequest{
		FromDockerfile: clusterImage(repoRoot),
		Hostname:       TaskManager,
		Networks:       []string{network},
		NetworkAliases: map[string][]string{network: {TaskManager}},
		Cmd:            []string{"taskmanager"}, // Flink's entrypoint arg, not the alias
		Env: map[string]string{
			"FLINK_PROPERTIES": "jobmanager.rpc.address: jobmanager\n" +
				"taskmanager.numberOfTaskSlots: " + taskSlots + "\n",
		},
		// Readiness is "registered with the job manager", which the container
		// cannot report; Start polls the job manager for it instead.
	})
	return err
}

// clusterImage is flink/normalizer/Dockerfile — NOT the stock flink image. It
// layers the Kafka connector, flink-avro and the Confluent registry client
// into /opt/flink/lib, without which every job fails at runtime.
func clusterImage(repoRoot string) testcontainers.FromDockerfile {
	return testcontainers.FromDockerfile{
		Context:    filepath.Join(repoRoot, "flink", "normalizer"),
		Dockerfile: "Dockerfile",
		Repo:       flinkImageRepo,
		Tag:        flinkImageTag,
		KeepImage:  true,
	}
}

// run starts a container and records it under its network alias for teardown
// and log collection, whether or not it came up cleanly. A container that
// failed its wait strategy is exactly the one whose logs are worth reading.
func (s *Stack) run(ctx context.Context, name string, req testcontainers.ContainerRequest) (testcontainers.Container, error) {
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if c != nil {
		s.containers = append(s.containers, named{name: name, c: c})
	}
	return c, err
}

// Logs returns the named containers' logs, keyed by network alias. Unknown
// names are skipped and a container that cannot be read reports the read error
// in place of its logs — diagnostics must never be the reason a run fails.
func (s *Stack) Logs(ctx context.Context, names ...string) map[string]string {
	wanted := make(map[string]bool, len(names))
	for _, name := range names {
		wanted[name] = true
	}

	out := make(map[string]string, len(names))
	for _, c := range s.containers {
		if !wanted[c.name] {
			continue
		}

		reader, err := c.c.Logs(ctx)
		if err != nil {
			out[c.name] = fmt.Sprintf("(could not read logs: %v)", err)
			continue
		}
		body, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			out[c.name] = fmt.Sprintf("(could not read logs: %v)", err)
			continue
		}
		out[c.name] = string(body)
	}
	return out
}

// Terminate tears the stack down, newest first, and always removes the network.
func (s *Stack) Terminate(ctx context.Context) error {
	var firstErr error
	for i := len(s.containers) - 1; i >= 0; i-- {
		if err := s.containers[i].c.Terminate(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.containers = nil

	if s.network != nil {
		if err := s.network.Remove(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		s.network = nil
	}
	return firstErr
}

// hostURL is the http:// address a container's port is reachable on from the
// test process — a port Docker picked, never a fixed one.
func hostURL(ctx context.Context, c testcontainers.Container, port nat.Port) (string, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", err
	}
	mapped, err := c.MappedPort(ctx, port)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("http://%s:%s", host, mapped.Port()), nil
}

// freePort asks the kernel for an unused port. There is a small window between
// releasing it and Docker binding it; nothing can close that window while the
// advertised listener must be known before startup.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
