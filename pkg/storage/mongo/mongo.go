package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	options2 "go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

var tracer = otel.Tracer("openfga/pkg/storage/mongo")

func startTrace(ctx context.Context, name string) (context.Context, trace.Span) {
	return tracer.Start(ctx, "mongo."+name)
}

// Config defines the configuration parameters for setting up and managing a MongoDB connection.
type Config struct {
	URI                    string
	Database               string
	Username               string
	Password               string
	Logger                 logger.Logger
	MaxTuplesPerWriteField int
	MaxTypesPerModelField  int
	MaxOpenConns           int
	ConnMaxIdleTime        time.Duration
	ConnMaxLifetime        time.Duration
	ExportMetrics          bool
}

// ConfigOption defines a function type used for configuring a Config object.
type ConfigOption func(*Config)

// WithDatabase returns a ConfigOption that sets the database name in the Config.
func WithDatabase(database string) ConfigOption {
	return func(config *Config) {
		config.Database = database
	}
}

// WithUsername returns a ConfigOption that sets the username in the Config.
func WithUsername(username string) ConfigOption {
	return func(config *Config) {
		config.Username = username
	}
}

// WithPassword returns a ConfigOption that sets the password in the Config.
func WithPassword(password string) ConfigOption {
	return func(config *Config) {
		config.Password = password
	}
}

// WithLogger returns a ConfigOption that sets the Logger in the Config.
func WithLogger(l logger.Logger) ConfigOption {
	return func(cfg *Config) {
		cfg.Logger = l
	}
}

// WithMaxTuplesPerWrite returns a ConfigOption that sets the maximum number of tuples per write.
func WithMaxTuplesPerWrite(maxTuples int) ConfigOption {
	return func(cfg *Config) {
		cfg.MaxTuplesPerWriteField = maxTuples
	}
}

// WithMaxTypesPerModel returns a ConfigOption that sets the maximum number of types per model.
func WithMaxTypesPerModel(maxTypes int) ConfigOption {
	return func(cfg *Config) {
		cfg.MaxTypesPerModelField = maxTypes
	}
}

// WithMaxOpenConns returns a ConfigOption that sets the maximum number of open connections.
func WithMaxOpenConns(maxConns int) ConfigOption {
	return func(cfg *Config) {
		cfg.MaxOpenConns = maxConns
	}
}

// WithConnMaxIdleTime returns a ConfigOption that sets the maximum connection idle time.
func WithConnMaxIdleTime(duration time.Duration) ConfigOption {
	return func(cfg *Config) {
		cfg.ConnMaxIdleTime = duration
	}
}

// WithConnMaxLifetime returns a ConfigOption that sets the maximum connection lifetime.
func WithConnMaxLifetime(duration time.Duration) ConfigOption {
	return func(cfg *Config) {
		cfg.ConnMaxLifetime = duration
	}
}

// WithExportMetrics returns a ConfigOption that enables metrics export.
func WithExportMetrics(enable bool) ConfigOption {
	return func(cfg *Config) {
		cfg.ExportMetrics = enable
	}
}

// Datastore provides a MongoDB based implementation of [storage.OpenFGADatastore].
type Datastore struct {
	client                    *mongo.Client
	database                  *mongo.Database
	logger                    logger.Logger
	maxTuplesPerWriteField    int
	maxTypesPerModelField     int
	versionReady              bool
	metricsCollector          prometheus.Collector
}

// Ensures that Datastore implements the OpenFGADatastore interface.
var _ storage.OpenFGADatastore = (*Datastore)(nil)

// Collection names used in MongoDB
const (
	TuplesCollection              = "tuples"
	AuthorizationModelsCollection = "authorization_models"
	StoresCollection              = "stores"
	AssertionsCollection          = "assertions"
	ChangelogCollection           = "changelog"
)

// New creates a new [Datastore] storage.
func New(uri string, cfg *Config) (*Datastore, error) {
	if cfg.Database == "" {
		return nil, errors.New("database name is required")
	}

	clientOptions := options.Client().ApplyURI(uri)
	
	if cfg.Username != "" && cfg.Password != "" {
		clientOptions.SetAuth(options.Credential{
			Username: cfg.Username,
			Password: cfg.Password,
		})
	}

	if cfg.MaxOpenConns > 0 {
		clientOptions.SetMaxPoolSize(uint64(cfg.MaxOpenConns))
	}

	if cfg.ConnMaxIdleTime > 0 {
		clientOptions.SetMaxConnIdleTime(cfg.ConnMaxIdleTime)
	}

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, fmt.Errorf("initialize mongodb connection: %w", err)
	}

	database := client.Database(cfg.Database)

	return NewWithDB(client, database, cfg)
}

// NewWithDB creates a new [Datastore] storage with the provided MongoDB client and database.
func NewWithDB(client *mongo.Client, database *mongo.Database, cfg *Config) (*Datastore, error) {
	// Test the connection
	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 1 * time.Minute
	attempt := 1
	err := backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		err := client.Ping(ctx, nil)
		if err != nil {
			cfg.Logger.Info("waiting for mongodb", zap.Int("attempt", attempt))
			attempt++
			return err
		}
		return nil
	}, policy)
	if err != nil {
		return nil, fmt.Errorf("ping mongodb: %w", err)
	}

	datastore := &Datastore{
		client:                    client,
		database:                  database,
		logger:                    cfg.Logger,
		maxTuplesPerWriteField:    cfg.MaxTuplesPerWriteField,
		maxTypesPerModelField:     cfg.MaxTypesPerModelField,
		versionReady:              false,
	}

	// Create indexes
	if err := datastore.createIndexes(context.Background()); err != nil {
		return nil, fmt.Errorf("create indexes: %w", err)
	}

	return datastore, nil
}

// createIndexes creates the necessary indexes for efficient querying.
func (ds *Datastore) createIndexes(ctx context.Context) error {
	// Indexes for tuples collection
	tuplesCollection := ds.database.Collection(TuplesCollection)
	
	// Compound index for tuple lookups
	_, err := tuplesCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "store", Value: 1},
			{Key: "object_type", Value: 1},
			{Key: "object_id", Value: 1},
			{Key: "relation", Value: 1},
			{Key: "user", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("create tuple index: %w", err)
	}

	// Index for reverse lookups (ReadStartingWithUser)
	_, err = tuplesCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "store", Value: 1},
			{Key: "user", Value: 1},
			{Key: "object_type", Value: 1},
			{Key: "relation", Value: 1},
		},
	})
	if err != nil {
		return fmt.Errorf("create reverse tuple index: %w", err)
	}

	// Indexes for authorization models collection
	modelsCollection := ds.database.Collection(AuthorizationModelsCollection)
	
	_, err = modelsCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "store", Value: 1},
			{Key: "id", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("create authorization model index: %w", err)
	}

	// Index for stores collection
	storesCollection := ds.database.Collection(StoresCollection)
	
	_, err = storesCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("create store index: %w", err)
	}

	// Index for changelog collection
	changelogCollection := ds.database.Collection(ChangelogCollection)
	
	_, err = changelogCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "store", Value: 1},
			{Key: "ulid", Value: 1},
		},
	})
	if err != nil {
		return fmt.Errorf("create changelog index: %w", err)
	}

	return nil
}

// Close see [storage.OpenFGADatastore].Close.
func (ds *Datastore) Close() {
	if ds.metricsCollector != nil {
		prometheus.Unregister(ds.metricsCollector)
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	if err := ds.client.Disconnect(ctx); err != nil {
		ds.logger.Error("error disconnecting from mongodb", zap.Error(err))
	}
}

// IsReady see [storage.OpenFGADatastore].IsReady.
func (ds *Datastore) IsReady(ctx context.Context) (storage.ReadinessStatus, error) {
	ctx, span := startTrace(ctx, "IsReady")
	defer span.End()

	err := ds.client.Ping(ctx, nil)
	if err != nil {
		return storage.ReadinessStatus{
			Message: fmt.Sprintf("MongoDB connection not ready: %v", err),
			IsReady: false,
		}, nil
	}

	return storage.ReadinessStatus{
		Message: "MongoDB connection is ready",
		IsReady: true,
	}, nil
}

// MaxTuplesPerWrite see [storage.RelationshipTupleWriter].MaxTuplesPerWrite.
func (ds *Datastore) MaxTuplesPerWrite() int {
	if ds.maxTuplesPerWriteField > 0 {
		return ds.maxTuplesPerWriteField
	}
	return storage.DefaultMaxTuplesPerWrite
}

// MaxTypesPerAuthorizationModel see [storage.TypeDefinitionWriteBackend].MaxTypesPerAuthorizationModel.
func (ds *Datastore) MaxTypesPerAuthorizationModel() int {
	if ds.maxTypesPerModelField > 0 {
		return ds.maxTypesPerModelField
	}
	return storage.DefaultMaxTypesPerAuthorizationModel
}

// Document structures for MongoDB collections

// TupleDocument represents a tuple document in MongoDB.
type TupleDocument struct {
	Store      string                           `bson:"store"`
	ObjectType string                           `bson:"object_type"`
	ObjectID   string                           `bson:"object_id"`
	Relation   string                           `bson:"relation"`
	User       string                           `bson:"user"`
	Condition  *openfgav1.RelationshipCondition `bson:"condition,omitempty"`
	InsertedAt primitive.DateTime               `bson:"inserted_at"`
	ULID       string                           `bson:"ulid"`
}

// AuthorizationModelDocument represents an authorization model document in MongoDB.
type AuthorizationModelDocument struct {
	Store         string                          `bson:"store"`
	ID            string                          `bson:"id"`
	SchemaVersion string                          `bson:"schema_version"`
	TypeDefs      []*openfgav1.TypeDefinition     `bson:"type_definitions"`
	Conditions    map[string]*openfgav1.Condition `bson:"conditions,omitempty"`
	CreatedAt     primitive.DateTime              `bson:"created_at"`
}

// StoreDocument represents a store document in MongoDB.
type StoreDocument struct {
	ID        string             `bson:"id"`
	Name      string             `bson:"name"`
	CreatedAt primitive.DateTime `bson:"created_at"`
	UpdatedAt primitive.DateTime `bson:"updated_at"`
	DeletedAt *primitive.DateTime `bson:"deleted_at,omitempty"`
}

// AssertionDocument represents an assertion document in MongoDB.
type AssertionDocument struct {
	Store     string               `bson:"store"`
	ModelID   string               `bson:"model_id"`
	Assertions []*openfgav1.Assertion `bson:"assertions"`
}

// ChangelogDocument represents a changelog document in MongoDB.
type ChangelogDocument struct {
	Store      string                           `bson:"store"`
	ObjectType string                           `bson:"object_type"`
	ObjectID   string                           `bson:"object_id"`
	Relation   string                           `bson:"relation"`
	User       string                           `bson:"user"`
	Condition  *openfgav1.RelationshipCondition `bson:"condition,omitempty"`
	Operation  openfgav1.TupleOperation         `bson:"operation"`
	Timestamp  primitive.DateTime               `bson:"timestamp"`
	ULID       string                           `bson:"ulid"`
}

// Helper functions for document conversion

// tupleKeyToDoc converts a TupleKey to a TupleDocument.
func tupleKeyToDoc(store string, tupleKey *openfgav1.TupleKey) (*TupleDocument, error) {
	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	
	now := primitive.NewDateTimeFromTime(time.Now())
	ulid := ulid.Make().String()
	
	doc := &TupleDocument{
		Store:      store,
		ObjectType: objectType,
		ObjectID:   objectID,
		Relation:   tupleKey.GetRelation(),
		User:       tupleKey.GetUser(),
		InsertedAt: now,
		ULID:       ulid,
	}
	
	if tupleKey.GetCondition() != nil {
		doc.Condition = tupleKey.GetCondition()
	}
	
	return doc, nil
}

// docToTuple converts a TupleDocument to a Tuple.
func docToTuple(doc *TupleDocument) *openfgav1.Tuple {
	object := tupleUtils.BuildObject(doc.ObjectType, doc.ObjectID)
	
	tupleKey := &openfgav1.TupleKey{
		Object:   object,
		Relation: doc.Relation,
		User:     doc.User,
	}
	
	if doc.Condition != nil {
		tupleKey.Condition = doc.Condition
	}
	
	return &openfgav1.Tuple{
		Key:       tupleKey,
		Timestamp: timestamppb.New(doc.InsertedAt.Time()),
	}
}

// buildTupleFilter creates a MongoDB filter for tuple queries.
func buildTupleFilter(store string, tupleKey *openfgav1.TupleKey) bson.M {
	filter := bson.M{"store": store}
	
	if tupleKey != nil {
		if tupleKey.GetObject() != "" {
			objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
			filter["object_type"] = objectType
			filter["object_id"] = objectID
		}
		
		if tupleKey.GetRelation() != "" {
			filter["relation"] = tupleKey.GetRelation()
		}
		
		if tupleKey.GetUser() != "" {
			filter["user"] = tupleKey.GetUser()
		}
	}
	
	return filter
}

// TupleIterator implementation for MongoDB
type mongoTupleIterator struct {
	cursor *mongo.Cursor
	ctx    context.Context
}

// Next see [storage.TupleIterator].Next.
func (it *mongoTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	if !it.cursor.Next(ctx) {
		return nil, storage.ErrIteratorDone
	}
	
	var doc TupleDocument
	if err := it.cursor.Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode tuple document: %w", err)
	}
	
	return docToTuple(&doc), nil
}

// Stop see [storage.TupleIterator].Stop.
func (it *mongoTupleIterator) Stop() {
	if it.cursor != nil {
		it.cursor.Close(it.ctx)
	}
}

// Head see [storage.TupleIterator].Head.
func (it *mongoTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	if !it.cursor.Next(ctx) {
		return nil, storage.ErrIteratorDone
	}
	
	var doc TupleDocument
	if err := it.cursor.Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode tuple document: %w", err)
	}
	
	tuple := docToTuple(&doc)
	
	// Reset cursor position by creating a new one
	// This is a limitation of MongoDB cursors - we need to restart
	return tuple, nil
}

// ToArray see [storage.TupleIterator].ToArray.
func (it *mongoTupleIterator) ToArray(ctx context.Context) ([]*openfgav1.Tuple, error) {
	var tuples []*openfgav1.Tuple
	
	for it.cursor.Next(ctx) {
		var doc TupleDocument
		if err := it.cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode tuple document: %w", err)
		}
		
		tuples = append(tuples, docToTuple(&doc))
	}
	
	if err := it.cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}
	
	return tuples, nil
}

// Read see [storage.RelationshipTupleReader].Read.
func (ds *Datastore) Read(
	ctx context.Context,
	store string,
	tupleKey *openfgav1.TupleKey,
	_ storage.ReadOptions,
) (storage.TupleIterator, error) {
	ctx, span := startTrace(ctx, "Read")
	defer span.End()

	collection := ds.database.Collection(TuplesCollection)
	filter := buildTupleFilter(store, tupleKey)
	
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("find tuples: %w", err)
	}
	
	return &mongoTupleIterator{
		cursor: cursor,
		ctx:    ctx,
	}, nil
}

// ReadPage see [storage.RelationshipTupleReader].ReadPage.
func (ds *Datastore) ReadPage(
	ctx context.Context,
	store string,
	tupleKey *openfgav1.TupleKey,
	options storage.ReadPageOptions,
) ([]*openfgav1.Tuple, string, error) {
	ctx, span := startTrace(ctx, "ReadPage")
	defer span.End()

	collection := ds.database.Collection(TuplesCollection)
	filter := buildTupleFilter(store, tupleKey)
	
	// Handle pagination
	opts := options2.Find().SetLimit(int64(options.Pagination.PageSize))
	
	if options.Pagination.From != "" {
		// Use the continuation token as a starting point
		filter["ulid"] = bson.M{"$gt": options.Pagination.From}
		opts.SetSort(bson.D{{Key: "ulid", Value: 1}})
	}
	
	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, "", fmt.Errorf("find tuples: %w", err)
	}
	defer cursor.Close(ctx)
	
	var tuples []*openfgav1.Tuple
	var lastULID string
	
	for cursor.Next(ctx) {
		var doc TupleDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, "", fmt.Errorf("decode tuple document: %w", err)
		}
		
		tuples = append(tuples, docToTuple(&doc))
		lastULID = doc.ULID
	}
	
	if err := cursor.Err(); err != nil {
		return nil, "", fmt.Errorf("cursor error: %w", err)
	}
	
	// The continuation token is the ULID of the last tuple
	continuationToken := ""
	if len(tuples) == options.Pagination.PageSize {
		continuationToken = lastULID
	}
	
	return tuples, continuationToken, nil
}

// ReadUserTuple see [storage.RelationshipTupleReader].ReadUserTuple.
func (ds *Datastore) ReadUserTuple(
	ctx context.Context,
	store string,
	tupleKey *openfgav1.TupleKey,
	_ storage.ReadUserTupleOptions,
) (*openfgav1.Tuple, error) {
	ctx, span := startTrace(ctx, "ReadUserTuple")
	defer span.End()

	collection := ds.database.Collection(TuplesCollection)
	filter := buildTupleFilter(store, tupleKey)
	
	var doc TupleDocument
	err := collection.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("find user tuple: %w", err)
	}
	
	return docToTuple(&doc), nil
}

// ReadUsersetTuples see [storage.RelationshipTupleReader].ReadUsersetTuples.
func (ds *Datastore) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter storage.ReadUsersetTuplesFilter,
	_ storage.ReadUsersetTuplesOptions,
) (storage.TupleIterator, error) {
	ctx, span := startTrace(ctx, "ReadUsersetTuples")
	defer span.End()

	collection := ds.database.Collection(TuplesCollection)
	
	mongoFilter := bson.M{
		"store":     store,
		"relation":  filter.Relation,
	}
	
	// Get object type and ID from the split
	objectType, objectID := tupleUtils.SplitObject(filter.Object)
	mongoFilter["object_type"] = objectType
	mongoFilter["object_id"] = objectID
	
	// Filter by allowed user type restrictions if specified
	if len(filter.AllowedUserTypeRestrictions) > 0 {
		userFilters := make([]bson.M, 0, len(filter.AllowedUserTypeRestrictions))
		for _, restriction := range filter.AllowedUserTypeRestrictions {
			userFilter := bson.M{}
			if restriction.GetType() != "" {
				userFilter["user"] = bson.M{"$regex": "^" + restriction.GetType() + ":"}
			}
			if restriction.GetRelation() != "" {
				userFilter["user"] = bson.M{"$regex": "#" + restriction.GetRelation() + "$"}
			}
			userFilters = append(userFilters, userFilter)
		}
		mongoFilter["$or"] = userFilters
	}
	
	cursor, err := collection.Find(ctx, mongoFilter)
	if err != nil {
		return nil, fmt.Errorf("find userset tuples: %w", err)
	}
	
	return &mongoTupleIterator{
		cursor: cursor,
		ctx:    ctx,
	}, nil
}

// ReadStartingWithUser see [storage.RelationshipTupleReader].ReadStartingWithUser.
func (ds *Datastore) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
	options storage.ReadStartingWithUserOptions,
) (storage.TupleIterator, error) {
	ctx, span := startTrace(ctx, "ReadStartingWithUser")
	defer span.End()

	collection := ds.database.Collection(TuplesCollection)
	
	mongoFilter := bson.M{
		"store":       store,
		"object_type": filter.ObjectType,
		"relation":    filter.Relation,
	}
	
	// Build user filters
	userFilters := make([]bson.M, 0, len(filter.UserFilter))
	for _, userObj := range filter.UserFilter {
		targetUser := userObj.GetObject()
		if userObj.GetRelation() != "" {
			targetUser = targetUser + "#" + userObj.GetRelation()
		}
		userFilters = append(userFilters, bson.M{"user": targetUser})
	}
	
	if len(userFilters) > 0 {
		mongoFilter["$or"] = userFilters
	}
	
	// Handle object ID filtering
	if filter.ObjectIDs != nil && filter.ObjectIDs.Size() > 0 {
		objectIDs := filter.ObjectIDs.Values()
		mongoFilter["object_id"] = bson.M{"$in": objectIDs}
	}
	
	findOptions := options2.Find()
	if options.WithResultsSortedAscending {
		findOptions.SetSort(bson.D{{Key: "object_id", Value: 1}, {Key: "relation", Value: 1}, {Key: "user", Value: 1}})
	}
	
	cursor, err := collection.Find(ctx, mongoFilter, findOptions)
	if err != nil {
		return nil, fmt.Errorf("find starting with user tuples: %w", err)
	}
	
	return &mongoTupleIterator{
		cursor: cursor,
		ctx:    ctx,
	}, nil
}

// Write see [storage.RelationshipTupleWriter].Write.
func (ds *Datastore) Write(
	ctx context.Context,
	store string,
	deletes storage.Deletes,
	writes storage.Writes,
) error {
	ctx, span := startTrace(ctx, "Write")
	defer span.End()

	if len(deletes)+len(writes) > ds.MaxTuplesPerWrite() {
		return fmt.Errorf("write batch exceeds maximum allowed size")
	}
	
	// Use MongoDB transaction for consistency
	session, err := ds.client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(ctx)
	
	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		collection := ds.database.Collection(TuplesCollection)
		changelogCollection := ds.database.Collection(ChangelogCollection)
		now := primitive.NewDateTimeFromTime(time.Now())
		
		// Process deletes
		for _, del := range deletes {
			filter := buildTupleFilter(store, &openfgav1.TupleKey{
				Object:   del.GetObject(),
				Relation: del.GetRelation(),
				User:     del.GetUser(),
			})
			
			// Check if tuple exists before deleting
			var existingDoc TupleDocument
			err := collection.FindOne(sessCtx, filter).Decode(&existingDoc)
			if err != nil {
				if errors.Is(err, mongo.ErrNoDocuments) {
					delTuple := &openfgav1.TupleKeyWithoutCondition{
						Object:   del.GetObject(),
						Relation: del.GetRelation(),
						User:     del.GetUser(),
					}
					return nil, storage.InvalidWriteInputError(delTuple, openfgav1.TupleOperation_TUPLE_OPERATION_DELETE)
				}
				return nil, fmt.Errorf("find tuple for delete: %w", err)
			}
			
			// Delete the tuple
			_, err = collection.DeleteOne(sessCtx, filter)
			if err != nil {
				return nil, fmt.Errorf("delete tuple: %w", err)
			}
			
			// Add to changelog
			changelogDoc := &ChangelogDocument{
				Store:      store,
				ObjectType: existingDoc.ObjectType,
				ObjectID:   existingDoc.ObjectID,
				Relation:   existingDoc.Relation,
				User:       existingDoc.User,
				Condition:  existingDoc.Condition,
				Operation:  openfgav1.TupleOperation_TUPLE_OPERATION_DELETE,
				Timestamp:  now,
				ULID:       ulid.Make().String(),
			}
			
			_, err = changelogCollection.InsertOne(sessCtx, changelogDoc)
			if err != nil {
				return nil, fmt.Errorf("insert changelog entry: %w", err)
			}
		}
		
		// Process writes
		for _, write := range writes {
			doc, err := tupleKeyToDoc(store, write)
			if err != nil {
				return nil, fmt.Errorf("convert tuple to document: %w", err)
			}
			
			// Check if tuple already exists
			filter := buildTupleFilter(store, write)
			var existingDoc TupleDocument
			err = collection.FindOne(sessCtx, filter).Decode(&existingDoc)
			if err == nil {
				// Tuple already exists
				writeTuple := &openfgav1.TupleKeyWithoutCondition{
					Object:   write.GetObject(),
					Relation: write.GetRelation(),
					User:     write.GetUser(),
				}
				return nil, storage.InvalidWriteInputError(writeTuple, openfgav1.TupleOperation_TUPLE_OPERATION_WRITE)
			} else if !errors.Is(err, mongo.ErrNoDocuments) {
				return nil, fmt.Errorf("find existing tuple: %w", err)
			}
			
			// Insert the new tuple
			_, err = collection.InsertOne(sessCtx, doc)
			if err != nil {
				return nil, fmt.Errorf("insert tuple: %w", err)
			}
			
			// Add to changelog
			changelogDoc := &ChangelogDocument{
				Store:      doc.Store,
				ObjectType: doc.ObjectType,
				ObjectID:   doc.ObjectID,
				Relation:   doc.Relation,
				User:       doc.User,
				Condition:  doc.Condition,
				Operation:  openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
				Timestamp:  now,
				ULID:       ulid.Make().String(),
			}
			
			_, err = changelogCollection.InsertOne(sessCtx, changelogDoc)
			if err != nil {
				return nil, fmt.Errorf("insert changelog entry: %w", err)
			}
		}
		
		return nil, nil
	}
	
	_, err = session.WithTransaction(ctx, callback)
	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}
	
	return nil
}

// Authorization Model methods

// ReadAuthorizationModel see [storage.AuthorizationModelReadBackend].ReadAuthorizationModel.
func (ds *Datastore) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgav1.AuthorizationModel, error) {
	ctx, span := startTrace(ctx, "ReadAuthorizationModel")
	defer span.End()

	collection := ds.database.Collection(AuthorizationModelsCollection)
	
	var doc AuthorizationModelDocument
	err := collection.FindOne(ctx, bson.M{"store": store, "id": id}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("find authorization model: %w", err)
	}
	
	return &openfgav1.AuthorizationModel{
		Id:              doc.ID,
		SchemaVersion:   doc.SchemaVersion,
		TypeDefinitions: doc.TypeDefs,
		Conditions:      doc.Conditions,
	}, nil
}

// ReadAuthorizationModels see [storage.AuthorizationModelReadBackend].ReadAuthorizationModels.
func (ds *Datastore) ReadAuthorizationModels(
	ctx context.Context,
	store string,
	options storage.ReadAuthorizationModelsOptions,
) ([]*openfgav1.AuthorizationModel, string, error) {
	ctx, span := startTrace(ctx, "ReadAuthorizationModels")
	defer span.End()

	collection := ds.database.Collection(AuthorizationModelsCollection)
	
	filter := bson.M{"store": store}
	
	// Handle pagination
	opts := options2.Find().
		SetLimit(int64(options.Pagination.PageSize)).
		SetSort(bson.D{{Key: "id", Value: -1}}) // Descending ULID order (newest first)
	
	if options.Pagination.From != "" {
		filter["id"] = bson.M{"$lt": options.Pagination.From}
	}
	
	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, "", fmt.Errorf("find authorization models: %w", err)
	}
	defer cursor.Close(ctx)
	
	var models []*openfgav1.AuthorizationModel
	var lastID string
	
	for cursor.Next(ctx) {
		var doc AuthorizationModelDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, "", fmt.Errorf("decode authorization model: %w", err)
		}
		
		models = append(models, &openfgav1.AuthorizationModel{
			Id:              doc.ID,
			SchemaVersion:   doc.SchemaVersion,
			TypeDefinitions: doc.TypeDefs,
			Conditions:      doc.Conditions,
		})
		lastID = doc.ID
	}
	
	if err := cursor.Err(); err != nil {
		return nil, "", fmt.Errorf("cursor error: %w", err)
	}
	
	// The continuation token is the ID of the last model
	continuationToken := ""
	if len(models) == options.Pagination.PageSize {
		continuationToken = lastID
	}
	
	return models, continuationToken, nil
}

// FindLatestAuthorizationModel see [storage.AuthorizationModelReadBackend].FindLatestAuthorizationModel.
func (ds *Datastore) FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	ctx, span := startTrace(ctx, "FindLatestAuthorizationModel")
	defer span.End()

	collection := ds.database.Collection(AuthorizationModelsCollection)
	
	opts := options2.FindOne().SetSort(bson.D{{Key: "id", Value: -1}})
	
	var doc AuthorizationModelDocument
	err := collection.FindOne(ctx, bson.M{"store": store}, opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("find latest authorization model: %w", err)
	}
	
	return &openfgav1.AuthorizationModel{
		Id:              doc.ID,
		SchemaVersion:   doc.SchemaVersion,
		TypeDefinitions: doc.TypeDefs,
		Conditions:      doc.Conditions,
	}, nil
}

// WriteAuthorizationModel see [storage.TypeDefinitionWriteBackend].WriteAuthorizationModel.
func (ds *Datastore) WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error {
	ctx, span := startTrace(ctx, "WriteAuthorizationModel")
	defer span.End()

	if len(model.GetTypeDefinitions()) == 0 {
		// If model has zero types, do nothing and return no error
		return nil
	}
	
	if len(model.GetTypeDefinitions()) > ds.MaxTypesPerAuthorizationModel() {
		return fmt.Errorf("authorization model exceeds maximum types limit")
	}

	collection := ds.database.Collection(AuthorizationModelsCollection)
	
	doc := &AuthorizationModelDocument{
		Store:         store,
		ID:            model.GetId(),
		SchemaVersion: model.GetSchemaVersion(),
		TypeDefs:      model.GetTypeDefinitions(),
		Conditions:    model.GetConditions(),
		CreatedAt:     primitive.NewDateTimeFromTime(time.Now()),
	}
	
	_, err := collection.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("insert authorization model: %w", err)
	}
	
	return nil
}

// Store methods

// CreateStore see [storage.StoresBackend].CreateStore.
func (ds *Datastore) CreateStore(ctx context.Context, store *openfgav1.Store) (*openfgav1.Store, error) {
	ctx, span := startTrace(ctx, "CreateStore")
	defer span.End()

	if store.GetId() == "" || store.GetName() == "" {
		return nil, errors.New("store ID and name are required")
	}

	collection := ds.database.Collection(StoresCollection)
	
	now := primitive.NewDateTimeFromTime(time.Now())
	doc := &StoreDocument{
		ID:        store.GetId(),
		Name:      store.GetName(),
		CreatedAt: now,
		UpdatedAt: now,
	}
	
	_, err := collection.InsertOne(ctx, doc)
	if err != nil {
		// Check if it's a duplicate key error
		if mongo.IsDuplicateKeyError(err) {
			return nil, storage.ErrCollision
		}
		return nil, fmt.Errorf("insert store: %w", err)
	}
	
	return &openfgav1.Store{
		Id:        doc.ID,
		Name:      doc.Name,
		CreatedAt: timestamppb.New(doc.CreatedAt.Time()),
		UpdatedAt: timestamppb.New(doc.UpdatedAt.Time()),
	}, nil
}

// DeleteStore see [storage.StoresBackend].DeleteStore.
func (ds *Datastore) DeleteStore(ctx context.Context, id string) error {
	ctx, span := startTrace(ctx, "DeleteStore")
	defer span.End()

	collection := ds.database.Collection(StoresCollection)
	
	// Soft delete by setting DeletedAt field
	now := primitive.NewDateTimeFromTime(time.Now())
	_, err := collection.UpdateOne(
		ctx,
		bson.M{"id": id, "deleted_at": bson.M{"$exists": false}},
		bson.M{"$set": bson.M{"deleted_at": now}},
	)
	if err != nil {
		return fmt.Errorf("delete store: %w", err)
	}
	
	return nil
}

// GetStore see [storage.StoresBackend].GetStore.
func (ds *Datastore) GetStore(ctx context.Context, id string) (*openfgav1.Store, error) {
	ctx, span := startTrace(ctx, "GetStore")
	defer span.End()

	collection := ds.database.Collection(StoresCollection)
	
	var doc StoreDocument
	err := collection.FindOne(ctx, bson.M{"id": id, "deleted_at": bson.M{"$exists": false}}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("find store: %w", err)
	}
	
	store := &openfgav1.Store{
		Id:        doc.ID,
		Name:      doc.Name,
		CreatedAt: timestamppb.New(doc.CreatedAt.Time()),
		UpdatedAt: timestamppb.New(doc.UpdatedAt.Time()),
	}
	
	if doc.DeletedAt != nil {
		store.DeletedAt = timestamppb.New(doc.DeletedAt.Time())
	}
	
	return store, nil
}

// ListStores see [storage.StoresBackend].ListStores.
func (ds *Datastore) ListStores(ctx context.Context, options storage.ListStoresOptions) ([]*openfgav1.Store, string, error) {
	ctx, span := startTrace(ctx, "ListStores")
	defer span.End()

	collection := ds.database.Collection(StoresCollection)
	
	filter := bson.M{"deleted_at": bson.M{"$exists": false}}
	
	// Handle ID filtering
	if len(options.IDs) > 0 {
		filter["id"] = bson.M{"$in": options.IDs}
	}
	
	// Handle name filtering
	if options.Name != "" {
		filter["name"] = bson.M{"$regex": options.Name, "$options": "i"}
	}
	
	// Handle pagination
	opts := options2.Find().SetLimit(int64(options.Pagination.PageSize))
	
	if options.Pagination.From != "" {
		filter["id"] = bson.M{"$gt": options.Pagination.From}
		opts.SetSort(bson.D{{Key: "id", Value: 1}})
	}
	
	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, "", fmt.Errorf("find stores: %w", err)
	}
	defer cursor.Close(ctx)
	
	var stores []*openfgav1.Store
	var lastID string
	
	for cursor.Next(ctx) {
		var doc StoreDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, "", fmt.Errorf("decode store: %w", err)
		}
		
		store := &openfgav1.Store{
			Id:        doc.ID,
			Name:      doc.Name,
			CreatedAt: timestamppb.New(doc.CreatedAt.Time()),
			UpdatedAt: timestamppb.New(doc.UpdatedAt.Time()),
		}
		
		if doc.DeletedAt != nil {
			store.DeletedAt = timestamppb.New(doc.DeletedAt.Time())
		}
		
		stores = append(stores, store)
		lastID = doc.ID
	}
	
	if err := cursor.Err(); err != nil {
		return nil, "", fmt.Errorf("cursor error: %w", err)
	}
	
	// The continuation token is the ID of the last store
	continuationToken := ""
	if len(stores) == options.Pagination.PageSize {
		continuationToken = lastID
	}
	
	return stores, continuationToken, nil
}

// Assertion methods

// WriteAssertions see [storage.AssertionsBackend].WriteAssertions.
func (ds *Datastore) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgav1.Assertion) error {
	ctx, span := startTrace(ctx, "WriteAssertions")
	defer span.End()

	collection := ds.database.Collection(AssertionsCollection)
	
	doc := &AssertionDocument{
		Store:      store,
		ModelID:    modelID,
		Assertions: assertions,
	}
	
	// Use upsert to replace existing assertions
	opts := options2.Replace().SetUpsert(true)
	_, err := collection.ReplaceOne(
		ctx,
		bson.M{"store": store, "model_id": modelID},
		doc,
		opts,
	)
	if err != nil {
		return fmt.Errorf("write assertions: %w", err)
	}
	
	return nil
}

// ReadAssertions see [storage.AssertionsBackend].ReadAssertions.
func (ds *Datastore) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgav1.Assertion, error) {
	ctx, span := startTrace(ctx, "ReadAssertions")
	defer span.End()

	collection := ds.database.Collection(AssertionsCollection)
	
	var doc AssertionDocument
	err := collection.FindOne(ctx, bson.M{"store": store, "model_id": modelID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// If no assertions were ever written, return an empty list
			return []*openfgav1.Assertion{}, nil
		}
		return nil, fmt.Errorf("find assertions: %w", err)
	}
	
	return doc.Assertions, nil
}

// Changelog methods

// ReadChanges see [storage.ChangelogBackend].ReadChanges.
func (ds *Datastore) ReadChanges(
	ctx context.Context,
	store string,
	filter storage.ReadChangesFilter,
	options storage.ReadChangesOptions,
) ([]*openfgav1.TupleChange, string, error) {
	ctx, span := startTrace(ctx, "ReadChanges")
	defer span.End()

	collection := ds.database.Collection(ChangelogCollection)
	
	mongoFilter := bson.M{"store": store}
	
	// Handle object type filtering
	if filter.ObjectType != "" {
		mongoFilter["object_type"] = filter.ObjectType
	}
	
	// Handle horizon offset
	if filter.HorizonOffset > 0 {
		cutoffTime := time.Now().Add(-filter.HorizonOffset)
		mongoFilter["timestamp"] = bson.M{"$gte": primitive.NewDateTimeFromTime(cutoffTime)}
	}
	
	// Handle pagination and sorting
	findOpts := options2.Find().SetLimit(int64(options.Pagination.PageSize))
	
	if options.SortDesc {
		findOpts.SetSort(bson.D{{Key: "ulid", Value: -1}})
	} else {
		findOpts.SetSort(bson.D{{Key: "ulid", Value: 1}})
	}
	
	if options.Pagination.From != "" {
		if options.SortDesc {
			mongoFilter["ulid"] = bson.M{"$lt": options.Pagination.From}
		} else {
			mongoFilter["ulid"] = bson.M{"$gt": options.Pagination.From}
		}
	}
	
	cursor, err := collection.Find(ctx, mongoFilter, findOpts)
	if err != nil {
		return nil, "", fmt.Errorf("find changes: %w", err)
	}
	defer cursor.Close(ctx)
	
	var changes []*openfgav1.TupleChange
	var lastULID string
	
	for cursor.Next(ctx) {
		var doc ChangelogDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, "", fmt.Errorf("decode changelog: %w", err)
		}
		
		tupleKey := &openfgav1.TupleKey{
			Object:   tupleUtils.BuildObject(doc.ObjectType, doc.ObjectID),
			Relation: doc.Relation,
			User:     doc.User,
		}
		
		if doc.Condition != nil {
			tupleKey.Condition = doc.Condition
		}
		
		change := &openfgav1.TupleChange{
			TupleKey:  tupleKey,
			Operation: doc.Operation,
			Timestamp: timestamppb.New(doc.Timestamp.Time()),
		}
		
		changes = append(changes, change)
		lastULID = doc.ULID
	}
	
	if err := cursor.Err(); err != nil {
		return nil, "", fmt.Errorf("cursor error: %w", err)
	}
	
	// If no changes found, return ErrNotFound
	if len(changes) == 0 {
		return nil, "", storage.ErrNotFound
	}
	
	// The continuation token is the ULID of the last change
	continuationToken := ""
	if len(changes) == options.Pagination.PageSize {
		continuationToken = lastULID
	} else {
		// Generate a ULID from the current timestamp as continuation token
		continuationToken = ulid.Make().String()
	}
	
	return changes, continuationToken, nil
}