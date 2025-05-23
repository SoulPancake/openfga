package mongo

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/openfga/openfga/pkg/storage"

	sq "github.com/Masterminds/squirrel"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/prometheus/client_golang/prometheus/collectors"
	"time"
)

// Datastore provides a MongoDB based implementation of [storage.OpenFGADatastore].
type Datastore struct {
	database               *mongo.Database
	client                 *mongo.Client
	stbl                   sq.StatementBuilderType
	db                     *sql.DB
	dbInfo                 *sqlcommon.DBInfo
	logger                 logger.Logger
	dbStatsCollector       prometheus.Collector
	maxTuplesPerWriteField int
	maxTypesPerModelField  int
}

func (d *Datastore) Read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadOptions) (storage.TupleIterator, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) ReadPage(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter, options storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) ReadStartingWithUser(ctx context.Context, store string, filter storage.ReadStartingWithUserFilter, options storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) MaxTuplesPerWrite() int {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgav1.AuthorizationModel, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) ReadAuthorizationModels(ctx context.Context, store string, options storage.ReadAuthorizationModelsOptions) ([]*openfgav1.AuthorizationModel, string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) MaxTypesPerAuthorizationModel() int {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) CreateStore(ctx context.Context, store *openfgav1.Store) (*openfgav1.Store, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) DeleteStore(ctx context.Context, id string) error {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) GetStore(ctx context.Context, id string) (*openfgav1.Store, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) ListStores(ctx context.Context, options storage.ListStoresOptions) ([]*openfgav1.Store, string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) ReadChanges(ctx context.Context, store string, filter storage.ReadChangesFilter, options storage.ReadChangesOptions) ([]*openfgav1.TupleChange, string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) IsReady(ctx context.Context) (storage.ReadinessStatus, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) Close() {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgav1.Assertion) error {
	//TODO implement me
	panic("implement me")
}

func (d *Datastore) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgav1.Assertion, error) {
	//TODO implement me
	panic("implement me")
}

func New(uri string, cfg *sqlcommon.Config) (*Datastore, error) {
	// Set client options
	clientOptions := options.Client().ApplyURI(uri)

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("initialize mongodb connection: %w", err)
	}

	// Ping the MongoDB server to verify connection
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to connect to mongodb: %w", err)
	}

	// Setup database
	database := client.Database("openfga")

	var collector prometheus.Collector
	if cfg.ExportMetrics {
		// For MongoDB monitoring
		collector = collectors.NewGoCollector()
		if err := prometheus.Register(collector); err != nil {
			return nil, fmt.Errorf("initialize metrics: %w", err)
		}
	}

	return &Datastore{
		client:                 client,
		database:               database,
		logger:                 cfg.Logger,
		dbStatsCollector:       collector,
		maxTuplesPerWriteField: cfg.MaxTuplesPerWriteField,
		maxTypesPerModelField:  cfg.MaxTypesPerModelField,
	}, nil
}
