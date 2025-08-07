package mongo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/openfga/openfga/pkg/storage"
	"go.mongodb.org/mongo-driver/bson"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

const (
	TuplesCollection = "tuples"
)

func startTrace(ctx context.Context, name string) (context.Context, trace.Span) {
	return tracer.Start(ctx, "mongo."+name)
}

// mongoTupleIterator implements storage.TupleIterator for MongoDB
type mongoTupleIterator struct {
	ctx       context.Context
	cursor    *mongo.Cursor
	once      *sync.Once
	lastTuple *openfgav1.Tuple
}

var tracer = otel.Tracer("openfga/pkg/storage/mysql")

// Datastore provides a MongoDB-based implementation of [storage.OpenFGADatastore].
type Datastore struct {
	database               *mongo.Database
	client                 *mongo.Client
	tuplesCollection       *mongo.Collection
	stbl                   sq.StatementBuilderType
	db                     *sql.DB
	dbInfo                 *sqlcommon.DBInfo
	logger                 logger.Logger
	dbStatsCollector       prometheus.Collector
	maxTuplesPerWriteField int
	maxTypesPerModelField  int
}

func (d *Datastore) Write(ctx context.Context, store string, deletes storage.Deletes, wr storage.Writes) error {
	ctx, span := startTrace(ctx, "Write")
	defer span.End()

	coll := d.tuplesCollection

	// Process deletions first
	if len(deletes) > 0 {
		var deleteFilters []bson.M
		for _, tk := range deletes {
			filter := bson.M{
				"store": store,
			}

			if tk.GetObject() != "" {
				filter["object"] = tk.GetObject()
			}

			if tk.GetRelation() != "" {
				filter["relation"] = tk.GetRelation()
			}

			if tk.GetUser() != "" {
				filter["user"] = tk.GetUser()
			}

			deleteFilters = append(deleteFilters, filter)
		}

		if len(deleteFilters) > 0 {
			// Use OR to combine filters
			deleteOp := bson.M{"$or": deleteFilters}
			_, err := coll.DeleteMany(ctx, deleteOp)
			if err != nil {
				return fmt.Errorf("failed to delete tuples: %w", err)
			}
		}
	}

	// Process insertions
	if len(wr) > 0 {
		var documents []interface{}
		for _, tupleKey := range wr {
			if tupleKey == nil {
				continue
			}

			doc := bson.M{
				"store":    store,
				"object":   tupleKey.GetObject(),
				"relation": tupleKey.GetRelation(),
				"user":     tupleKey.GetUser(),
			}

			// Handle conditions
			if tupleKey.GetCondition() != nil {
				conditionName, conditionContext, err := sqlcommon.MarshalRelationshipCondition(tupleKey.GetCondition())
				if err != nil {
					return fmt.Errorf("failed to marshal condition: %w", err)
				}
				doc["condition_name"] = conditionName
				if conditionContext != nil {
					doc["condition_context"] = conditionContext
				}
			}

			documents = append(documents, doc)
		}

		if len(documents) > 0 {
			_, err := coll.InsertMany(ctx, documents)
			if err != nil {
				return fmt.Errorf("failed to insert tuples: %w", err)
			}
		}
	}

	return nil
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
	coll := d.tuplesCollection
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
			Object           string `bson:"object"`
			Relation         string `bson:"relation"`
			User             string `bson:"user"`
			ConditionName    string `bson:"condition_name,omitempty"`
			ConditionContext []byte `bson:"condition_context,omitempty"`
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

		// Handle conditions
		if doc.ConditionName != "" {
			condition := &openfgav1.RelationshipCondition{
				Name: doc.ConditionName,
			}

			if doc.ConditionContext != nil {
				var conditionContextStruct structpb.Struct
				if err := proto.Unmarshal(doc.ConditionContext, &conditionContextStruct); err != nil {
					return nil, fmt.Errorf("error unmarshalling condition context: %w", err)
				}
				condition.Context = &conditionContextStruct
			} else {
				// Set empty context if none provided
				condition.Context = &structpb.Struct{}
			}

			tuple.Key.Condition = condition
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

	findOptions := options.FindOptions{}

	if opts.Pagination.PageSize > 0 {
		findOptions.SetLimit(int64(opts.Pagination.PageSize + 1)) // +1 to check if there are more results
	} else {
		// If no page size specified, return all results
		// Don't set a limit
	}

	if opts.Pagination.From != "" {
		// Assuming continuation token is the MongoDB ObjectID of the last document
		findOptions.SetSkip(1) // Skip the last document we've seen
		filter["_id"] = bson.M{"$gt": opts.Pagination.From}
	}

	// Set sorting (assuming we sort by _id for continuation)
	findOptions.SetSort(bson.M{"_id": 1})

	coll := d.tuplesCollection
	cursor, err := coll.Find(ctx, filter, &findOptions)
	if err != nil {
		return nil, "", fmt.Errorf("error querying tuples: %w", err)
	}
	defer cursor.Close(ctx)

	var tuples []*openfgav1.Tuple
	var lastID string

	for cursor.Next(ctx) {
		var doc struct {
			ID               string `bson:"_id"`
			Object           string `bson:"object"`
			Relation         string `bson:"relation"`
			User             string `bson:"user"`
			ConditionName    string `bson:"condition_name,omitempty"`
			ConditionContext []byte `bson:"condition_context,omitempty"`
		}

		if err := cursor.Decode(&doc); err != nil {
			return nil, "", fmt.Errorf("error decoding tuple: %w", err)
		}

		lastID = doc.ID

		tuple := &openfgav1.Tuple{
			Key: &openfgav1.TupleKey{
				Object:   doc.Object,
				Relation: doc.Relation,
				User:     doc.User,
			},
		}

		// Handle conditions
		if doc.ConditionName != "" {
			condition := &openfgav1.RelationshipCondition{
				Name: doc.ConditionName,
			}

			if doc.ConditionContext != nil {
				var conditionContextStruct structpb.Struct
				if err := proto.Unmarshal(doc.ConditionContext, &conditionContextStruct); err != nil {
					return nil, "", fmt.Errorf("error unmarshalling condition context: %w", err)
				}
				condition.Context = &conditionContextStruct
			} else {
				// Set empty context if none provided
				condition.Context = &structpb.Struct{}
			}

			tuple.Key.Condition = condition
		}

		tuples = append(tuples, tuple)
	}

	if err := cursor.Err(); err != nil {
		return nil, "", fmt.Errorf("cursor error: %w", err)
	}

	continuationToken := ""
	if len(tuples) > opts.Pagination.PageSize {
		// Remove the extra item we retrieved
		continuationToken = lastID
		tuples = tuples[:opts.Pagination.PageSize]
	}

	return tuples, continuationToken, nil
}

func (d *Datastore) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	ctx, span := startTrace(ctx, "ReadUserTuple")
	defer span.End()

	if tupleKey == nil {
		return nil, fmt.Errorf("tuple key cannot be nil")
	}

	// Build the filter for an exact match on the tuple key
	filter := bson.M{
		"store":    store,
		"object":   tupleKey.GetObject(),
		"relation": tupleKey.GetRelation(),
		"user":     tupleKey.GetUser(),
	}

	// Execute the query
	coll := d.tuplesCollection
	var result struct {
		Object           string `bson:"object"`
		Relation         string `bson:"relation"`
		User             string `bson:"user"`
		ConditionName    string `bson:"condition_name,omitempty"`
		ConditionContext []byte `bson:"condition_context,omitempty"`
	}

	err := coll.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("error querying tuple: %w", err)
	}

	// Convert the result to a tuple
	tuple := &openfgav1.Tuple{
		Key: &openfgav1.TupleKey{
			Object:   result.Object,
			Relation: result.Relation,
			User:     result.User,
		},
	}

	// Handle conditions
	if result.ConditionName != "" {
		condition := &openfgav1.RelationshipCondition{
			Name: result.ConditionName,
		}

		if result.ConditionContext != nil {
			var conditionContextStruct structpb.Struct
			if err := proto.Unmarshal(result.ConditionContext, &conditionContextStruct); err != nil {
				return nil, fmt.Errorf("error unmarshalling condition context: %w", err)
			}
			condition.Context = &conditionContextStruct
		} else {
			// Set empty context if none provided
			condition.Context = &structpb.Struct{}
		}

		tuple.Key.Condition = condition
	}

	return tuple, nil
}

func (d *Datastore) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter storage.ReadUsersetTuplesFilter,
	_ storage.ReadUsersetTuplesOptions,
) (storage.TupleIterator, error) {
	ctx, span := startTrace(ctx, "ReadUsersetTuples")
	defer span.End()

	// Build the filter for querying usersets
	mongoFilter := bson.M{"store": store}

	// Filter by object and relation if provided
	if filter.Object != "" {
		mongoFilter["object"] = filter.Object
	}
	if filter.Relation != "" {
		mongoFilter["relation"] = filter.Relation
	}

	// Filter by allowed user type restrictions
	if len(filter.AllowedUserTypeRestrictions) > 0 {
		userPatterns := []bson.M{}

		for _, userset := range filter.AllowedUserTypeRestrictions {
			if _, ok := userset.GetRelationOrWildcard().(*openfgav1.RelationReference_Relation); ok {
				// Pattern for type:*#relation
				userPatterns = append(userPatterns, bson.M{
					"user": bson.M{
						"$regex": userset.GetType() + ":%#" + userset.GetRelation(),
					},
				})
			}
			if _, ok := userset.GetRelationOrWildcard().(*openfgav1.RelationReference_Wildcard); ok {
				// Pattern for type:*
				userPatterns = append(userPatterns, bson.M{
					"user": userset.GetType() + ":*",
				})
			}
		}

		if len(userPatterns) > 0 {
			mongoFilter["$or"] = userPatterns
		}
	}

	// Execute the query
	coll := d.tuplesCollection
	cursor, err := coll.Find(ctx, mongoFilter)
	if err != nil {
		return nil, fmt.Errorf("error querying userset tuples: %w", err)
	}

	return newMongoTupleIterator(ctx, cursor), nil
}

func (d *Datastore) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
	opts storage.ReadStartingWithUserOptions,
) (storage.TupleIterator, error) {
	ctx, span := startTrace(ctx, "ReadStartingWithUser")
	defer span.End()

	// Build the filter for querying tuples starting with user
	mongoFilter := bson.M{
		"store": store,
	}

	// Process user filters similar to the MySQL implementation
	if len(filter.UserFilter) > 0 {
		var userFilters []bson.M

		for _, u := range filter.UserFilter {
			targetUser := u.GetObject()
			if u.GetRelation() != "" {
				targetUser = u.GetObject() + "#" + u.GetRelation()
			}
			userFilters = append(userFilters, bson.M{"user": targetUser})
		}

		if len(userFilters) > 0 {
			mongoFilter["$or"] = userFilters
		}
	}
	// TODO: @SoulPancake check this once
	//else if filter.User != "" { // Handle the original User field for backward compatibility
	//	mongoFilter["user"] = filter.User
	//}

	// Add object type and relation filters
	if filter.ObjectType != "" {
		// Match on object_type field or use regex on object field depending on schema
		mongoFilter["object_type"] = filter.ObjectType
	}

	if filter.Relation != "" {
		mongoFilter["relation"] = filter.Relation
	}

	// Handle ObjectIDs filter if provided
	if filter.ObjectIDs != nil && filter.ObjectIDs.Size() > 0 {
		mongoFilter["object_id"] = bson.M{"$in": filter.ObjectIDs.Values()}
	}

	// Execute the query with sorting similar to MySQL implementation
	findOptions := options.FindOptions{}
	findOptions.SetSort(bson.M{"object_id": 1})

	// Execute the query
	coll := d.tuplesCollection
	cursor, err := coll.Find(ctx, mongoFilter, &findOptions)
	if err != nil {
		return nil, fmt.Errorf("error querying tuples starting with user: %w", err)
	}

	return newMongoTupleIterator(ctx, cursor), nil
}

func (d *Datastore) MaxTuplesPerWrite() int {
	return d.maxTuplesPerWriteField
}

func (d *Datastore) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgav1.AuthorizationModel, error) {
	ctx, span := startTrace(ctx, "ReadAuthorizationModel")
	defer span.End()

	coll := d.database.Collection("authorization_models")
	filter := bson.M{
		"store": store,
		"id":    id,
	}

	var result struct {
		ID        string                      `bson:"id"`
		Store     string                      `bson:"store"`
		TypeDefs  []*openfgav1.TypeDefinition `bson:"type_definitions"`
		SchemaVer string                      `bson:"schema_version"`
		CreatedAt time.Time                   `bson:"created_at"`
	}

	err := coll.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("error querying authorization model: %w", err)
	}

	// If model has zero types, return ErrNotFound
	if len(result.TypeDefs) == 0 {
		return nil, storage.ErrNotFound
	}

	return &openfgav1.AuthorizationModel{
		Id:              result.ID,
		TypeDefinitions: result.TypeDefs,
		SchemaVersion:   result.SchemaVer,
	}, nil
}

func (d *Datastore) ReadAuthorizationModels(ctx context.Context, store string, options storage.ReadAuthorizationModelsOptions) ([]*openfgav1.AuthorizationModel, string, error) {
	ctx, span := startTrace(ctx, "ReadAuthorizationModels")
	defer span.End()

	coll := d.database.Collection("authorization_models")
	filter := bson.M{"store": store}
	findOptions := optionsMongoPagination(options.Pagination)

	cursor, err := coll.Find(ctx, filter, findOptions)
	if err != nil {
		return nil, "", fmt.Errorf("error querying authorization models: %w", err)
	}
	defer cursor.Close(ctx)

	var models []*openfgav1.AuthorizationModel
	var lastID string

	for cursor.Next(ctx) {
		var doc struct {
			ID        string                      `bson:"id"`
			TypeDefs  []*openfgav1.TypeDefinition `bson:"type_definitions"`
			SchemaVer string                      `bson:"schema_version"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, "", fmt.Errorf("error decoding authorization model: %w", err)
		}
		models = append(models, &openfgav1.AuthorizationModel{
			Id:              doc.ID,
			TypeDefinitions: doc.TypeDefs,
			SchemaVersion:   doc.SchemaVer,
		})
		lastID = doc.ID
	}

	continuationToken := ""
	if options.Pagination.PageSize > 0 && len(models) > options.Pagination.PageSize {
		continuationToken = lastID
		models = models[:options.Pagination.PageSize]
	}

	return models, continuationToken, nil
}

// Helper for pagination options
func optionsMongoPagination(p storage.PaginationOptions) *options.FindOptions {
	findOptions := options.Find()
	if p.PageSize > 0 {
		findOptions.SetLimit(int64(p.PageSize + 1))
	}
	if p.From != "" {
		findOptions.SetSort(bson.M{"id": 1})
		findOptions.SetHint(bson.M{"id": 1})
	}
	return findOptions
}

func (d *Datastore) FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	ctx, span := startTrace(ctx, "FindLatestAuthorizationModel")
	defer span.End()

	coll := d.database.Collection("authorization_models")
	filter := bson.M{"store": store}

	// Sort by creation time (or id) descending to get latest model
	opts := options.FindOne().SetSort(bson.M{"created_at": -1})

	var result struct {
		ID        string                      `bson:"id"`
		TypeDefs  []*openfgav1.TypeDefinition `bson:"type_definitions"`
		SchemaVer string                      `bson:"schema_version"`
	}

	err := coll.FindOne(ctx, filter, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("error finding latest authorization model: %w", err)
	}

	return &openfgav1.AuthorizationModel{
		Id:              result.ID,
		TypeDefinitions: result.TypeDefs,
		SchemaVersion:   result.SchemaVer,
	}, nil
}

func (d *Datastore) MaxTypesPerAuthorizationModel() int {
	return d.maxTypesPerModelField
}

func (d *Datastore) WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error {
	ctx, span := startTrace(ctx, "WriteAuthorizationModel")
	defer span.End()

	typeDefinitions := model.GetTypeDefinitions()

	if len(typeDefinitions) < 1 {
		return nil
	}

	coll := d.database.Collection("authorization_models")

	doc := bson.M{
		"id":               model.GetId(),
		"store":            store,
		"type_definitions": model.GetTypeDefinitions(),
		"schema_version":   model.GetSchemaVersion(),
		"created_at":       time.Now(),
	}

	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to write authorization model: %w", err)
	}

	return nil
}

func (d *Datastore) CreateStore(ctx context.Context, store *openfgav1.Store) (*openfgav1.Store, error) {
	ctx, span := startTrace(ctx, "CreateStore")
	defer span.End()

	coll := d.database.Collection("stores")

	// If name is empty, return error
	if store.GetName() == "" {
		return nil, fmt.Errorf("store name cannot be empty")
	}

	// Check if store with same name already exists
	existingFilter := bson.M{"name": store.GetName()}
	var existingStore bson.M
	err := coll.FindOne(ctx, existingFilter).Decode(&existingStore)
	if err == nil {
		return nil, storage.ErrCollision
	} else if !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("error checking store existence: %w", err)
	}

	// Prepare store document
	now := time.Now().UTC()
	storeDoc := bson.M{
		"id":         store.GetId(),
		"name":       store.GetName(),
		"created_at": now,
		"updated_at": now,
	}

	// Insert the store document
	_, err = coll.InsertOne(ctx, storeDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	// Return the created store
	return &openfgav1.Store{
		Id:        store.GetId(),
		Name:      store.GetName(),
		CreatedAt: timestamppb.New(now),
		UpdatedAt: timestamppb.New(now),
	}, nil
}

func (d *Datastore) DeleteStore(ctx context.Context, id string) error {
	ctx, span := startTrace(ctx, "DeleteStore")
	defer span.End()

	// Start a session and transaction
	session, err := d.client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start mongo session: %w", err)
	}
	defer session.EndSession(ctx)

	err = mongo.WithSession(ctx, session, func(sessionContext mongo.SessionContext) error {
		if err := session.StartTransaction(); err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}

		// Delete the store
		storeCollection := d.database.Collection("stores")
		result, err := storeCollection.DeleteOne(sessionContext, bson.M{"id": id})
		if err != nil {
			return fmt.Errorf("failed to delete store: %w", err)
		}
		if result.DeletedCount == 0 {
			return storage.ErrNotFound
		}

		// Delete related data - tuples
		_, err = d.tuplesCollection.DeleteMany(sessionContext, bson.M{"store": id})
		if err != nil {
			return fmt.Errorf("failed to delete store tuples: %w", err)
		}

		// Delete related data - authorization models
		_, err = d.database.Collection("authorization_models").DeleteMany(sessionContext, bson.M{"store": id})
		if err != nil {
			return fmt.Errorf("failed to delete store authorization models: %w", err)
		}

		// Delete related data - assertions
		_, err = d.database.Collection("assertions").DeleteMany(sessionContext, bson.M{"store": id})
		if err != nil {
			return fmt.Errorf("failed to delete store assertions: %w", err)
		}

		return session.CommitTransaction(sessionContext)
	})

	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	return nil
}

func (d *Datastore) GetStore(ctx context.Context, id string) (*openfgav1.Store, error) {
	ctx, span := startTrace(ctx, "GetStore")
	defer span.End()

	coll := d.database.Collection("stores")
	filter := bson.M{"id": id}

	var result struct {
		ID        string    `bson:"id"`
		Name      string    `bson:"name"`
		CreatedAt time.Time `bson:"created_at"`
		UpdatedAt time.Time `bson:"updated_at"`
	}

	err := coll.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("error querying store: %w", err)
	}

	return &openfgav1.Store{
		Id:        result.ID,
		Name:      result.Name,
		CreatedAt: timestamppb.New(result.CreatedAt),
		UpdatedAt: timestamppb.New(result.UpdatedAt),
	}, nil
}

func (d *Datastore) ListStores(ctx context.Context, opts storage.ListStoresOptions) ([]*openfgav1.Store, string, error) {
	ctx, span := startTrace(ctx, "ListStores")
	defer span.End()

	coll := d.database.Collection("stores")
	filter := bson.M{}

	// Configure find opts for pagination
	findOptions := options.FindOptions{}
	if opts.Pagination.PageSize > 0 {
		findOptions.SetLimit(int64(opts.Pagination.PageSize + 1)) // +1 to check if there are more results
	}

	// Apply continuation token if provided
	if opts.Pagination.From != "" {
		findOptions.SetSkip(1) // Skip the last document we've seen
		filter["id"] = bson.M{"$gt": opts.Pagination.From}
	}

	// Set sorting for consistent pagination
	findOptions.SetSort(bson.M{"id": 1})

	// Execute query
	cursor, err := coll.Find(ctx, filter, &findOptions)
	if err != nil {
		return nil, "", fmt.Errorf("error querying stores: %w", err)
	}
	defer cursor.Close(ctx)

	var stores []*openfgav1.Store
	var lastID string

	// Fetch results
	for cursor.Next(ctx) {
		var doc struct {
			ID        string    `bson:"id"`
			Name      string    `bson:"name"`
			CreatedAt time.Time `bson:"created_at"`
			UpdatedAt time.Time `bson:"updated_at"`
		}

		if err := cursor.Decode(&doc); err != nil {
			return nil, "", fmt.Errorf("error decoding store: %w", err)
		}

		lastID = doc.ID
		stores = append(stores, &openfgav1.Store{
			Id:        doc.ID,
			Name:      doc.Name,
			CreatedAt: timestamppb.New(doc.CreatedAt),
			UpdatedAt: timestamppb.New(doc.UpdatedAt),
		})
	}

	if err := cursor.Err(); err != nil {
		return nil, "", fmt.Errorf("cursor error: %w", err)
	}

	// Handle pagination
	continuationToken := ""
	if len(stores) > opts.Pagination.PageSize {
		// Remove the extra item we retrieved
		continuationToken = lastID
		stores = stores[:opts.Pagination.PageSize]
	}

	return stores, continuationToken, nil
}

func (d *Datastore) ReadChanges(ctx context.Context, store string, filter storage.ReadChangesFilter, opts storage.ReadChangesOptions) ([]*openfgav1.TupleChange, string, error) {
	ctx, span := startTrace(ctx, "ReadChanges")
	defer span.End()

	objectTypeFilter := filter.ObjectType
	horizonOffset := filter.HorizonOffset

	coll := d.database.Collection("tuple_changes")
	mongoFilter := bson.M{"store": store}

	// Add horizon offset filtering
	if horizonOffset > 0 {
		cutoffTime := time.Now().Add(-1 * horizonOffset)
		mongoFilter["timestamp"] = bson.M{"$lte": cutoffTime}
	}

	// Apply object type filter
	if objectTypeFilter != "" {
		mongoFilter["object_type"] = objectTypeFilter
	}

	// Configure pagination and sorting
	findOptions := options.FindOptions{}
	sortDirection := 1
	if opts.SortDesc {
		sortDirection = -1
	}
	findOptions.SetSort(bson.D{{"timestamp", sortDirection}, {"_id", 1}})

	if opts.Pagination.PageSize > 0 {
		findOptions.SetLimit(int64(opts.Pagination.PageSize + 1))
	}

	if opts.Pagination.From != "" {
		// Use the continuation token as a starting point
		mongoFilter["_id"] = bson.M{"$gt": opts.Pagination.From}
	}

	// Execute query
	cursor, err := coll.Find(ctx, mongoFilter, &findOptions)
	if err != nil {
		return nil, "", fmt.Errorf("error querying tuple changes: %w", err)
	}
	defer cursor.Close(ctx)

	var changes []*openfgav1.TupleChange
	var lastID string

	for cursor.Next(ctx) {
		var doc struct {
			ID               string    `bson:"_id"`
			ObjectType       string    `bson:"object_type"`
			ObjectID         string    `bson:"object_id"`
			Relation         string    `bson:"relation"`
			User             string    `bson:"user"`
			Operation        int32     `bson:"operation"`
			Timestamp        time.Time `bson:"timestamp"`
			ConditionName    string    `bson:"condition_name,omitempty"`
			ConditionContext []byte    `bson:"condition_context,omitempty"`
		}

		if err := cursor.Decode(&doc); err != nil {
			return nil, "", fmt.Errorf("error decoding tuple change: %w", err)
		}

		lastID = doc.ID

		// Build the object reference string (type:id format)
		objectRef := doc.ObjectType + ":" + doc.ObjectID

		// Create the tuple key
		tupleKey := &openfgav1.TupleKey{
			Object:   objectRef,
			Relation: doc.Relation,
			User:     doc.User,
		}

		// Handle condition if present
		if doc.ConditionName != "" && len(doc.ConditionContext) > 0 {
			var conditionContextStruct structpb.Struct
			if err := proto.Unmarshal(doc.ConditionContext, &conditionContextStruct); err != nil {
				return nil, "", err
			}

			tupleKey.Condition = &openfgav1.RelationshipCondition{
				Name:    doc.ConditionName,
				Context: &conditionContextStruct,
			}
		}

		changes = append(changes, &openfgav1.TupleChange{
			TupleKey:  tupleKey,
			Operation: openfgav1.TupleOperation(doc.Operation),
			Timestamp: timestamppb.New(doc.Timestamp),
		})
	}

	if err := cursor.Err(); err != nil {
		return nil, "", fmt.Errorf("cursor error: %w", err)
	}

	if len(changes) == 0 {
		return nil, "", storage.ErrNotFound
	}

	// Handle pagination
	continuationToken := ""
	if len(changes) > opts.Pagination.PageSize {
		// Remove the extra item we retrieved
		continuationToken = lastID
		changes = changes[:opts.Pagination.PageSize]
	}

	return changes, continuationToken, nil
}

func (d *Datastore) IsReady(ctx context.Context) (storage.ReadinessStatus, error) {
	ctx, span := startTrace(ctx, "IsReady")
	defer span.End()

	// Try to ping the database with a timeout
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := d.client.Ping(pingCtx, readpref.Primary())
	if err != nil {
		return storage.ReadinessStatus{IsReady: false}, fmt.Errorf("mongodb ping failed: %w", err)
	}

	// Database connection is ready
	return storage.ReadinessStatus{IsReady: true}, nil
}

func (d *Datastore) Close() {
	if d.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := d.client.Disconnect(ctx); err != nil {
			d.logger.Error("error closing MongoDB connection")
		}

		if d.dbStatsCollector != nil {
			prometheus.Unregister(d.dbStatsCollector)
		}
	}
}

func (d *Datastore) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgav1.Assertion) error {
	ctx, span := startTrace(ctx, "WriteAssertions")
	defer span.End()

	// Start a session and transaction
	session, err := d.client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start mongo session: %w", err)
	}
	defer session.EndSession(ctx)

	err = mongo.WithSession(ctx, session, func(sessionContext mongo.SessionContext) error {
		if err := session.StartTransaction(); err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}

		// Delete existing assertions for this store and modelID
		assertionsCollection := d.database.Collection("assertions")
		_, err := assertionsCollection.DeleteOne(
			sessionContext,
			bson.M{
				"store":                  store,
				"authorization_model_id": modelID,
			},
		)
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
			return fmt.Errorf("failed to delete existing assertions: %w", err)
		}

		// Marshal all assertions into a single binary field
		marshalledAssertions, err := proto.Marshal(&openfgav1.Assertions{Assertions: assertions})
		if err != nil {
			return err
		}

		// Insert new assertions document
		_, err = assertionsCollection.InsertOne(
			sessionContext,
			bson.M{
				"store":                  store,
				"authorization_model_id": modelID,
				"assertions":             marshalledAssertions,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to insert assertions: %w", err)
		}

		return session.CommitTransaction(sessionContext)
	})

	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	return nil
}

// ReadAssertions see [storage.AssertionsBackend].ReadAssertions.
func (d *Datastore) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgav1.Assertion, error) {
	ctx, span := startTrace(ctx, "ReadAssertions")
	defer span.End()

	assertionsCollection := d.database.Collection("assertions")
	filter := bson.M{
		"store":                  store,
		"authorization_model_id": modelID,
	}

	var result struct {
		Assertions []byte `bson:"assertions"`
	}

	err := assertionsCollection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return []*openfgav1.Assertion{}, nil
		}
		return nil, fmt.Errorf("error querying assertions: %w", err)
	}

	var assertions openfgav1.Assertions
	err = proto.Unmarshal(result.Assertions, &assertions)
	if err != nil {
		return nil, err
	}

	return assertions.GetAssertions(), nil
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

	tuplesCollection := database.Collection(TuplesCollection)
	return &Datastore{
		client:                 client,
		database:               database,
		tuplesCollection:       tuplesCollection,
		logger:                 cfg.Logger,
		dbStatsCollector:       collector,
		maxTuplesPerWriteField: cfg.MaxTuplesPerWriteField,
		maxTypesPerModelField:  cfg.MaxTypesPerModelField,
	}, nil
}
