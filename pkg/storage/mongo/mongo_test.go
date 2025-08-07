package mongo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/test"
)

const testDatabase = "openfga_test"

func TestMongoDBDatastore(t *testing.T) {
	// Skip if we don't have MongoDB running
	if testing.Short() {
		t.Skip("MongoDB integration tests skipped in short mode")
	}

	ctx := context.Background()
	
	// Connect to MongoDB (assumes MongoDB is running locally)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	
	// Test connection
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}
	
	defer client.Disconnect(ctx)
	
	// Drop test database to start clean
	database := client.Database(testDatabase)
	err = database.Drop(ctx)
	require.NoError(t, err)

	cfg := &Config{
		URI:                    "mongodb://localhost:27017",
		Database:               testDatabase,
		Logger:                 logger.NewNoopLogger(),
		MaxTuplesPerWriteField: 100,
		MaxTypesPerModelField:  100,
	}

	datastore, err := New(cfg.URI, cfg)
	require.NoError(t, err)
	require.NotNil(t, datastore)

	defer datastore.Close()

	// Test that datastore implements the interface
	var _ storage.OpenFGADatastore = datastore

	// Test IsReady
	status, err := datastore.IsReady(ctx)
	require.NoError(t, err)
	require.True(t, status.IsReady)
	require.Contains(t, status.Message, "ready")
}

func TestMongoDBDatastoreWithTestSuite(t *testing.T) {
	// Skip if we don't have MongoDB running
	if testing.Short() {
		t.Skip("MongoDB integration tests skipped in short mode")
	}

	ctx := context.Background()
	
	// Connect to MongoDB (assumes MongoDB is running locally)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	
	// Test connection
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}
	
	defer client.Disconnect(ctx)

	datastoreTestFunc := func(t *testing.T) storage.OpenFGADatastore {
		// Drop test database to start clean
		database := client.Database(testDatabase)
		err = database.Drop(ctx)
		require.NoError(t, err)

		cfg := &Config{
			URI:                    "mongodb://localhost:27017",
			Database:               testDatabase,
			Logger:                 logger.NewNoopLogger(),
			MaxTuplesPerWriteField: 100,
			MaxTypesPerModelField:  100,
		}

		datastore, err := New(cfg.URI, cfg)
		require.NoError(t, err)
		require.NotNil(t, datastore)

		return datastore
	}

	test.RunAllTests(t, datastoreTestFunc(t))
}

func TestMongoDBTupleOperations(t *testing.T) {
	// Skip if we don't have MongoDB running
	if testing.Short() {
		t.Skip("MongoDB integration tests skipped in short mode")
	}

	ctx := context.Background()
	
	// Connect to MongoDB (assumes MongoDB is running locally)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	
	// Test connection
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}
	
	defer client.Disconnect(ctx)
	
	// Drop test database to start clean
	database := client.Database(testDatabase)
	err = database.Drop(ctx)
	require.NoError(t, err)

	cfg := &Config{
		URI:                    "mongodb://localhost:27017",
		Database:               testDatabase,
		Logger:                 logger.NewNoopLogger(),
		MaxTuplesPerWriteField: 100,
		MaxTypesPerModelField:  100,
	}

	datastore, err := New(cfg.URI, cfg)
	require.NoError(t, err)
	require.NotNil(t, datastore)

	defer datastore.Close()

	store := "test-store"
	
	// Test writing tuples
	tuples := []*openfgav1.TupleKey{
		{
			Object:   "document:doc1",
			Relation: "viewer",
			User:     "user:alice",
		},
		{
			Object:   "document:doc1",
			Relation: "editor",
			User:     "user:bob",
		},
		{
			Object:   "document:doc2",
			Relation: "viewer",
			User:     "user:charlie",
		},
	}

	err = datastore.Write(ctx, store, []*openfgav1.TupleKeyWithoutCondition{}, tuples)
	require.NoError(t, err)

	// Test reading tuples
	iter, err := datastore.Read(ctx, store, nil, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	
	// Collect all tuples
	var readTuples []*openfgav1.Tuple
	for {
		tuple, err := iter.Next(ctx)
		if err != nil {
			if err == storage.ErrIteratorDone {
				break
			}
			require.NoError(t, err)
		}
		readTuples = append(readTuples, tuple)
	}
	require.Len(t, readTuples, 3)

	// Test reading specific tuple
	tuple, err := datastore.ReadUserTuple(ctx, store, &openfgav1.TupleKey{
		Object:   "document:doc1",
		Relation: "viewer",
		User:     "user:alice",
	}, storage.ReadUserTupleOptions{})
	require.NoError(t, err)
	require.Equal(t, "document:doc1", tuple.Key.Object)
	require.Equal(t, "viewer", tuple.Key.Relation)
	require.Equal(t, "user:alice", tuple.Key.User)

	// Test deleting a tuple
	err = datastore.Write(ctx, store, []*openfgav1.TupleKeyWithoutCondition{
		{
			Object:   "document:doc1",
			Relation: "viewer",
			User:     "user:alice",
		},
	}, []*openfgav1.TupleKey{})
	require.NoError(t, err)

	// Verify tuple was deleted
	_, err = datastore.ReadUserTuple(ctx, store, &openfgav1.TupleKey{
		Object:   "document:doc1",
		Relation: "viewer",
		User:     "user:alice",
	}, storage.ReadUserTupleOptions{})
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestMongoDBStoreOperations(t *testing.T) {
	// Skip if we don't have MongoDB running
	if testing.Short() {
		t.Skip("MongoDB integration tests skipped in short mode")
	}

	ctx := context.Background()
	
	// Connect to MongoDB (assumes MongoDB is running locally)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	
	// Test connection
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}
	
	defer client.Disconnect(ctx)
	
	// Drop test database to start clean
	database := client.Database(testDatabase)
	err = database.Drop(ctx)
	require.NoError(t, err)

	cfg := &Config{
		URI:                    "mongodb://localhost:27017",
		Database:               testDatabase,
		Logger:                 logger.NewNoopLogger(),
		MaxTuplesPerWriteField: 100,
		MaxTypesPerModelField:  100,
	}

	datastore, err := New(cfg.URI, cfg)
	require.NoError(t, err)
	require.NotNil(t, datastore)

	defer datastore.Close()

	// Test creating a store
	testStore := &openfgav1.Store{
		Id:   "store1",
		Name: "Test Store",
	}

	createdStore, err := datastore.CreateStore(ctx, testStore)
	require.NoError(t, err)
	require.Equal(t, testStore.Id, createdStore.Id)
	require.Equal(t, testStore.Name, createdStore.Name)
	require.NotNil(t, createdStore.CreatedAt)
	require.NotNil(t, createdStore.UpdatedAt)

	// Test getting the store
	retrievedStore, err := datastore.GetStore(ctx, testStore.Id)
	require.NoError(t, err)
	require.Equal(t, testStore.Id, retrievedStore.Id)
	require.Equal(t, testStore.Name, retrievedStore.Name)

	// Test listing stores
	stores, _, err := datastore.ListStores(ctx, storage.ListStoresOptions{
		Pagination: storage.PaginationOptions{PageSize: 10},
	})
	require.NoError(t, err)
	require.Len(t, stores, 1)
	require.Equal(t, testStore.Id, stores[0].Id)

	// Test deleting the store
	err = datastore.DeleteStore(ctx, testStore.Id)
	require.NoError(t, err)

	// Verify store was deleted
	_, err = datastore.GetStore(ctx, testStore.Id)
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestMongoDBAuthorizationModelOperations(t *testing.T) {
	// Skip if we don't have MongoDB running
	if testing.Short() {
		t.Skip("MongoDB integration tests skipped in short mode")
	}

	ctx := context.Background()
	
	// Connect to MongoDB (assumes MongoDB is running locally)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	
	// Test connection
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}
	
	defer client.Disconnect(ctx)
	
	// Drop test database to start clean
	database := client.Database(testDatabase)
	err = database.Drop(ctx)
	require.NoError(t, err)

	cfg := &Config{
		URI:                    "mongodb://localhost:27017",
		Database:               testDatabase,
		Logger:                 logger.NewNoopLogger(),
		MaxTuplesPerWriteField: 100,
		MaxTypesPerModelField:  100,
	}

	datastore, err := New(cfg.URI, cfg)
	require.NoError(t, err)
	require.NotNil(t, datastore)

	defer datastore.Close()

	store := "test-store"
	modelID := "01HVMMBCQZH0Q8Q9A3XCZ8G4SN"

	// Test writing an authorization model
	model := &openfgav1.AuthorizationModel{
		Id:            modelID,
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": {
						Userset: &openfgav1.Userset_This{},
					},
					"editor": {
						Userset: &openfgav1.Userset_This{},
					},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
							},
						},
						"editor": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
							},
						},
					},
				},
			},
		},
	}

	err = datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(t, err)

	// Test reading the model
	retrievedModel, err := datastore.ReadAuthorizationModel(ctx, store, modelID)
	require.NoError(t, err)
	require.Equal(t, model.Id, retrievedModel.Id)
	require.Equal(t, model.SchemaVersion, retrievedModel.SchemaVersion)
	require.Len(t, retrievedModel.TypeDefinitions, 2)

	// Test finding latest model
	latestModel, err := datastore.FindLatestAuthorizationModel(ctx, store)
	require.NoError(t, err)
	require.Equal(t, model.Id, latestModel.Id)

	// Test reading models (pagination)
	models, _, err := datastore.ReadAuthorizationModels(ctx, store, storage.ReadAuthorizationModelsOptions{
		Pagination: storage.PaginationOptions{PageSize: 10},
	})
	require.NoError(t, err)
	require.Len(t, models, 1)
	require.Equal(t, model.Id, models[0].Id)
}

func TestMongoDBConfiguration(t *testing.T) {
	cfg := &Config{
		URI:                    "mongodb://localhost:27017",
		Database:               testDatabase,
		Username:               "testuser",
		Password:               "testpass",
		Logger:                 logger.NewNoopLogger(),
		MaxTuplesPerWriteField: 200,
		MaxTypesPerModelField:  150,
		MaxOpenConns:           50,
		ConnMaxIdleTime:        10 * time.Minute,
		ConnMaxLifetime:        time.Hour,
		ExportMetrics:          true,
	}

	// Test configuration options
	require.Equal(t, "mongodb://localhost:27017", cfg.URI)
	require.Equal(t, testDatabase, cfg.Database)
	require.Equal(t, "testuser", cfg.Username)
	require.Equal(t, "testpass", cfg.Password)
	require.Equal(t, 200, cfg.MaxTuplesPerWriteField)
	require.Equal(t, 150, cfg.MaxTypesPerModelField)
	require.Equal(t, 50, cfg.MaxOpenConns)
	require.Equal(t, 10*time.Minute, cfg.ConnMaxIdleTime)
	require.Equal(t, time.Hour, cfg.ConnMaxLifetime)
	require.True(t, cfg.ExportMetrics)
}

func TestConfigOptions(t *testing.T) {
	cfg := &Config{}

	// Test configuration options
	WithDatabase("test_db")(cfg)
	require.Equal(t, "test_db", cfg.Database)

	WithUsername("user")(cfg)
	require.Equal(t, "user", cfg.Username)

	WithPassword("pass")(cfg)
	require.Equal(t, "pass", cfg.Password)

	WithMaxTuplesPerWrite(500)(cfg)
	require.Equal(t, 500, cfg.MaxTuplesPerWriteField)

	WithMaxTypesPerModel(200)(cfg)
	require.Equal(t, 200, cfg.MaxTypesPerModelField)

	WithMaxOpenConns(100)(cfg)
	require.Equal(t, 100, cfg.MaxOpenConns)

	WithConnMaxIdleTime(5 * time.Minute)(cfg)
	require.Equal(t, 5*time.Minute, cfg.ConnMaxIdleTime)

	WithConnMaxLifetime(2 * time.Hour)(cfg)
	require.Equal(t, 2*time.Hour, cfg.ConnMaxLifetime)

	WithExportMetrics(true)(cfg)
	require.True(t, cfg.ExportMetrics)

	logger := logger.NewNoopLogger()
	WithLogger(logger)(cfg)
	require.Equal(t, logger, cfg.Logger)
}

func TestDocumentConversion(t *testing.T) {
	store := "test-store"
	
	// Test tuple to document conversion
	tupleKey := &openfgav1.TupleKey{
		Object:   "document:doc1",
		Relation: "viewer",
		User:     "user:alice",
	}

	doc, err := tupleKeyToDoc(store, tupleKey)
	require.NoError(t, err)
	require.Equal(t, store, doc.Store)
	require.Equal(t, "document", doc.ObjectType)
	require.Equal(t, "doc1", doc.ObjectID)
	require.Equal(t, "viewer", doc.Relation)
	require.Equal(t, "user:alice", doc.User)
	require.NotEmpty(t, doc.ULID)

	// Test document to tuple conversion
	tuple := docToTuple(doc)
	require.Equal(t, tupleKey.Object, tuple.Key.Object)
	require.Equal(t, tupleKey.Relation, tuple.Key.Relation)
	require.Equal(t, tupleKey.User, tuple.Key.User)
	require.NotNil(t, tuple.Timestamp)
}

func TestTupleFilter(t *testing.T) {
	store := "test-store"
	
	// Test empty filter
	filter := buildTupleFilter(store, nil)
	require.Equal(t, store, filter["store"])
	require.Len(t, filter, 1)

	// Test complete filter
	tupleKey := &openfgav1.TupleKey{
		Object:   "document:doc1",
		Relation: "viewer",
		User:     "user:alice",
	}
	
	filter = buildTupleFilter(store, tupleKey)
	require.Equal(t, store, filter["store"])
	require.Equal(t, "document", filter["object_type"])
	require.Equal(t, "doc1", filter["object_id"])
	require.Equal(t, "viewer", filter["relation"])
	require.Equal(t, "user:alice", filter["user"])
	require.Len(t, filter, 5)

	// Test partial filter
	partialKey := &openfgav1.TupleKey{
		Object:   "document:doc1",
		Relation: "viewer",
	}
	
	filter = buildTupleFilter(store, partialKey)
	require.Equal(t, store, filter["store"])
	require.Equal(t, "document", filter["object_type"])
	require.Equal(t, "doc1", filter["object_id"])
	require.Equal(t, "viewer", filter["relation"])
	require.Len(t, filter, 4)
}