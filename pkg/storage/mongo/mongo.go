package mongo

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/openfga/openfga/pkg/storage"
	"go.mongodb.org/mongo-driver/bson"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"sync"

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

// mongoTupleIterator implements storage.TupleIterator for MongoDB
type mongoTupleIterator struct {
	ctx       context.Context
	cursor    *mongo.Cursor
	once      *sync.Once
	lastTuple *openfgav1.Tuple
}

var tracer = otel.Tracer("openfga/pkg/storage/mysql")

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

func (d *Datastore) Write(ctx context.Context, store string, deletes storage.Deletes, wr storage.Writes) error {
	//TODO implement me
	panic("implement me")
}

func startTrace(ctx context.Context, name string) (context.Context, trace.Span) {
	return tracer.Start(ctx, "mongo."+name)
}

func (d *Datastore) Read(
	ctx context.Context,
	store string,
	tupleKey *openfgav1.TupleKey,
	_ storage.ReadOptions) (storage.TupleIterator, error) {
	ctx, span := startTrace(ctx, "Read")
	defer span.End()
	// Build the filter based on the tupleKey
	filter := bson.M{"store": store}

	if tupleKey != nil {
		if tupleKey.GetObject() != "" {
			filter["object"] = tupleKey.GetObject()
		}

		if tupleKey.GetRelation() != "" {
			filter["relation"] = tupleKey.GetRelation()
		}

		if tupleKey.GetUser() != "" {
			filter["user"] = tupleKey.GetUser()
		}
	}

	// Execute the query
	coll := d.database.Collection("tuples")
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("error querying tuples: %w", err)
	}

	return newMongoTupleIterator(ctx, cursor), nil
}

func newMongoTupleIterator(ctx context.Context, cursor *mongo.Cursor) storage.TupleIterator {
	return &mongoTupleIterator{
		ctx:    ctx,
		cursor: cursor,
		once:   &sync.Once{},
	}
}

func (it *mongoTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	if it.cursor.Next(ctx) {
		var doc struct {
			Object   string `bson:"object"`
			Relation string `bson:"relation"`
			User     string `bson:"user"`
		}

		if err := it.cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("error decoding tuple: %w", err)
		}

		tuple := &openfgav1.Tuple{
			Key: &openfgav1.TupleKey{
				Object:   doc.Object,
				Relation: doc.Relation,
				User:     doc.User,
			},
		}

		it.lastTuple = tuple
		return tuple, nil
	}

	if err := it.cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return nil, storage.ErrIteratorDone
}

func (it *mongoTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	if it.lastTuple != nil {
		return it.lastTuple, nil
	}

	tuple, err := it.Next(ctx)
	if err != nil {
		return nil, err
	}

	it.lastTuple = tuple
	return tuple, nil
}

func (it *mongoTupleIterator) Stop() {
	it.once.Do(func() {
		if it.cursor != nil {
			_ = it.cursor.Close(it.ctx)
		}
	})
}

func (d *Datastore) ReadPage(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, opts storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	ctx, span := startTrace(ctx, "ReadPage")
	defer span.End()

	// Build the filter based on the tupleKey
	filter := bson.M{"store": store}

	if tupleKey != nil {
		if tupleKey.GetObject() != "" {
			filter["object"] = tupleKey.GetObject()
		}

		if tupleKey.GetRelation() != "" {
			filter["relation"] = tupleKey.GetRelation()
		}

		if tupleKey.GetUser() != "" {
			filter["user"] = tupleKey.GetUser()
		}
	}

	// Configure find opts for pagination
	findOptions := options.FindOptions{}

	// Set page size
	if opts.Pagination.PageSize > 0 {
		findOptions.SetLimit(int64(opts.Pagination.PageSize + 1)) // +1 to check if there are more results
	}

	// Apply continuation token if provided
	if opts.Pagination.From != "" {
		// Assuming continuation token is the MongoDB ObjectID of the last document
		findOptions.SetSkip(1) // Skip the last document we've seen
		filter["_id"] = bson.M{"$gt": opts.Pagination.From}
	}

	// Set sorting (assuming we sort by _id for continuation)
	findOptions.SetSort(bson.M{"_id": 1})

	// Execute the query
	coll := d.database.Collection("tuples")
	cursor, err := coll.Find(ctx, filter, &findOptions)
	if err != nil {
		return nil, "", fmt.Errorf("error querying tuples: %w", err)
	}
	defer cursor.Close(ctx)

	var tuples []*openfgav1.Tuple
	var lastID string

	// Fetch results
	for cursor.Next(ctx) {
		var doc struct {
			ID       string `bson:"_id"`
			Object   string `bson:"object"`
			Relation string `bson:"relation"`
			User     string `bson:"user"`
		}

		if err := cursor.Decode(&doc); err != nil {
			return nil, "", fmt.Errorf("error decoding tuple: %w", err)
		}

		lastID = doc.ID

		tuples = append(tuples, &openfgav1.Tuple{
			Key: &openfgav1.TupleKey{
				Object:   doc.Object,
				Relation: doc.Relation,
				User:     doc.User,
			},
		})
	}

	if err := cursor.Err(); err != nil {
		return nil, "", fmt.Errorf("cursor error: %w", err)
	}

	// Handle pagination
	continuationToken := ""
	if len(tuples) > opts.Pagination.PageSize {
		// Remove the extra item we retrieved
		continuationToken = lastID
		tuples = tuples[:opts.Pagination.PageSize]
	}

	return tuples, continuationToken, nil
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
