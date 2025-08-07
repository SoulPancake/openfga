package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mongoImage = "mongo:latest"
)

type mongoTestContainer struct {
	addr     string
	version  int64
	username string
	password string
}

// NewMongoTestContainer returns an implementation of the DatastoreTestContainer interface
// for MongoDB.
func NewMongoTestContainer() *mongoTestContainer {
	return &mongoTestContainer{}
}

func (m *mongoTestContainer) GetDatabaseSchemaVersion() int64 {
	return m.version
}

func (m *mongoTestContainer) GetConnectionURI(includeCredentials bool) string {
	if includeCredentials && m.username != "" && m.password != "" {
		return fmt.Sprintf("mongodb://%s:%s@%s/openfga_test", m.username, m.password, m.addr)
	}
	return fmt.Sprintf("mongodb://%s/openfga_test", m.addr)
}

func (m *mongoTestContainer) GetUsername() string {
	return m.username
}

func (m *mongoTestContainer) GetPassword() string {
	return m.password
}

// RunMongoTestDatabase runs a MongoDB container, connects to it, and returns a
// bootstrapped implementation of the DatastoreTestContainer interface wired up for the
// MongoDB datastore engine.
func (m *mongoTestContainer) RunMongoTestDatabase(t testing.TB) DatastoreTestContainer {
	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	require.NoError(t, err)

	ctx := context.Background()

	// Pull the MongoDB image
	t.Logf("Pulling MongoDB image: %s", mongoImage)
	reader, err := dockerClient.ImagePull(ctx, mongoImage, image.PullOptions{})
	require.NoError(t, err)
	defer reader.Close()
	
	// Read the response to ensure the pull completes
	_, err = io.ReadAll(reader)
	require.NoError(t, err)
	t.Logf("MongoDB image pulled successfully")

	// Create and start the container
	containerName := fmt.Sprintf("openfga-mongo-test-%s", strings.ReplaceAll(ulid.Make().String(), "_", ""))

	// Set up MongoDB configuration
	containerConfig := &container.Config{
		Image: mongoImage,
		ExposedPorts: nat.PortSet{
			"27017/tcp": struct{}{},
		},
	}

	hostConfig := &container.HostConfig{
		PublishAllPorts: true,
		AutoRemove:      true,
	}

	resp, err := dockerClient.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, containerName)
	require.NoError(t, err)

	err = dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{})
	require.NoError(t, err)

	// Clean up container when test finishes
	t.Cleanup(func() {
		if err := dockerClient.ContainerStop(ctx, resp.ID, container.StopOptions{}); err != nil {
			t.Logf("failed to stop mongo container: %v", err)
		}
	})

	// Get the container's port mapping
	containerJSON, err := dockerClient.ContainerInspect(ctx, resp.ID)
	require.NoError(t, err)

	port := containerJSON.NetworkSettings.Ports["27017/tcp"][0]
	m.addr = fmt.Sprintf("%s:%s", port.HostIP, port.HostPort)

	// Wait for MongoDB to be ready
	mongoURI := m.GetConnectionURI(false)
	err = backoff.Retry(func() error {
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI).SetServerSelectionTimeout(5*time.Second))
		if err != nil {
			return err
		}
		defer client.Disconnect(ctx)

		return client.Ping(ctx, nil)
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 10))
	require.NoError(t, err, "MongoDB container failed to become ready")

	// MongoDB doesn't need migrations since it's schema-less, but set version for consistency
	m.version = 1

	return m
}
