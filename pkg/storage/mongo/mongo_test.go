package mongo

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/test"
	testfixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestMongoDatastore(t *testing.T) {
	testDatastore := testfixtures.RunDatastoreTestContainer(t, "mongo")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()

	test.RunAllTests(t, ds)
}

func TestWrite(t *testing.T) {
	testDatastore := testfixtures.RunDatastoreTestContainer(t, "mongo")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()

	ctx := context.Background()
	store := "test-store-" + testutils.CreateRandomString(8)

	t.Run("insert_tuples", func(t *testing.T) {
		writes := storage.Writes{
			&openfgav1.TupleKey{
				Object:   "folder:2021-budget",
				Relation: "owner",
				User:     "user:anne",
			},
		}

		err := ds.Write(ctx, store, nil, writes)
		require.NoError(t, err)

		// Verify the tuple was written
		filter := bson.M{
			"store":    store,
			"object":   "folder:2021-budget",
			"relation": "owner",
			"user":     "user:anne",
		}

		var result bson.M
		err = ds.tuplesCollection.FindOne(ctx, filter).Decode(&result)
		require.NoError(t, err)
		require.Equal(t, "folder:2021-budget", result["object"])
		require.Equal(t, "owner", result["relation"])
		require.Equal(t, "user:anne", result["user"])
	})

	t.Run("delete_tuples", func(t *testing.T) {
		// First write a tuple
		writes := storage.Writes{
			&openfgav1.TupleKey{
				Object:   "folder:2022-budget",
				Relation: "viewer",
				User:     "user:bob",
			},
		}

		err := ds.Write(ctx, store, nil, writes)
		require.NoError(t, err)

		// Now delete the tuple
		deletes := storage.Deletes{
			&openfgav1.TupleKeyWithoutCondition{
				Object:   "folder:2022-budget",
				Relation: "viewer",
				User:     "user:bob",
			},
		}

		err = ds.Write(ctx, store, deletes, nil)
		require.NoError(t, err)

		// Verify the tuple was deleted
		filter := bson.M{
			"store":    store,
			"object":   "folder:2022-budget",
			"relation": "viewer",
			"user":     "user:bob",
		}

		count, err := ds.tuplesCollection.CountDocuments(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(0), count)
	})

	t.Run("delete_with_partial_key", func(t *testing.T) {
		// First write multiple tuples with same object
		writes := storage.Writes{
			&openfgav1.TupleKey{
				Object:   "folder:reports",
				Relation: "viewer",
				User:     "user:carol",
			},
			&openfgav1.TupleKey{
				Object:   "folder:reports",
				Relation: "editor",
				User:     "user:dave",
			},
		}

		err := ds.Write(ctx, store, nil, writes)
		require.NoError(t, err)

		// Delete all tuples for the object regardless of relation/user
		deletes := storage.Deletes{
			&openfgav1.TupleKeyWithoutCondition{
				Object: "folder:reports",
			},
		}

		err = ds.Write(ctx, store, deletes, nil)
		require.NoError(t, err)

		// Verify all tuples for the object were deleted
		filter := bson.M{
			"store":  store,
			"object": "folder:reports",
		}

		count, err := ds.tuplesCollection.CountDocuments(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(0), count)
	})

	t.Run("write_and_delete_in_same_transaction", func(t *testing.T) {
		// First write a tuple
		initialWrites := storage.Writes{
			&openfgav1.TupleKey{
				Object:   "folder:presentations",
				Relation: "viewer",
				User:     "user:eve",
			},
		}

		err := ds.Write(ctx, store, nil, initialWrites)
		require.NoError(t, err)

		// Now in the same transaction delete the old tuple and add a new one
		deletes := storage.Deletes{
			&openfgav1.TupleKeyWithoutCondition{
				Object:   "folder:presentations",
				Relation: "viewer",
				User:     "user:eve",
			},
		}

		writes := storage.Writes{
			&openfgav1.TupleKey{
				Object:   "folder:presentations",
				Relation: "editor",
				User:     "user:eve",
			},
		}

		err = ds.Write(ctx, store, deletes, writes)
		require.NoError(t, err)

		// Verify old tuple was deleted and new tuple was written
		filter := bson.M{
			"store":  store,
			"object": "folder:presentations",
		}

		cursor, err := ds.tuplesCollection.Find(ctx, filter)
		require.NoError(t, err)
		defer cursor.Close(ctx)

		var results []bson.M
		err = cursor.All(ctx, &results)
		require.NoError(t, err)

		require.Len(t, results, 1)
		require.Equal(t, "folder:presentations", results[0]["object"])
		require.Equal(t, "editor", results[0]["relation"])
		require.Equal(t, "user:eve", results[0]["user"])
	})

	t.Run("write_with_nil_tuple", func(t *testing.T) {
		writes := storage.Writes{
			nil,
			&openfgav1.TupleKey{
				Object:   "document:spec",
				Relation: "viewer",
				User:     "user:frank",
			},
		}

		err := ds.Write(ctx, store, nil, writes)
		require.NoError(t, err)

		// Verify only the non-nil tuple was written
		filter := bson.M{
			"store":    store,
			"object":   "document:spec",
			"relation": "viewer",
			"user":     "user:frank",
		}

		var result bson.M
		err = ds.tuplesCollection.FindOne(ctx, filter).Decode(&result)
		require.NoError(t, err)
		require.Equal(t, "document:spec", result["object"])
	})

	t.Run("write_multiple_tuples", func(t *testing.T) {
		writes := storage.Writes{
			&openfgav1.TupleKey{
				Object:   "folder:project-x",
				Relation: "viewer",
				User:     "user:grace",
			},
			&openfgav1.TupleKey{
				Object:   "folder:project-x",
				Relation: "viewer",
				User:     "user:helen",
			},
			&openfgav1.TupleKey{
				Object:   "folder:project-x",
				Relation: "editor",
				User:     "user:ivan",
			},
		}

		err := ds.Write(ctx, store, nil, writes)
		require.NoError(t, err)

		// Verify all tuples were written
		filter := bson.M{
			"store":  store,
			"object": "folder:project-x",
		}

		cursor, err := ds.tuplesCollection.Find(ctx, filter)
		require.NoError(t, err)
		defer cursor.Close(ctx)

		var results []bson.M
		err = cursor.All(ctx, &results)
		require.NoError(t, err)

		require.Len(t, results, 3)

		// Create a map to check all expected tuples
		found := make(map[string]bool)
		for _, doc := range results {
			key := doc["relation"].(string) + ":" + doc["user"].(string)
			found[key] = true
		}

		require.True(t, found["viewer:user:grace"], "First tuple not found")
		require.True(t, found["viewer:user:helen"], "Second tuple not found")
		require.True(t, found["editor:user:ivan"], "Third tuple not found")
	})

	t.Run("delete_with_relation_filter", func(t *testing.T) {
		// First write multiple tuples with different relations
		writes := storage.Writes{
			&openfgav1.TupleKey{
				Object:   "document:report",
				Relation: "viewer",
				User:     "user:john",
			},
			&openfgav1.TupleKey{
				Object:   "document:report",
				Relation: "editor",
				User:     "user:john",
			},
		}

		err := ds.Write(ctx, store, nil, writes)
		require.NoError(t, err)

		// Delete only viewer relation
		deletes := storage.Deletes{
			&openfgav1.TupleKeyWithoutCondition{
				Object:   "document:report",
				Relation: "viewer",
			},
		}

		err = ds.Write(ctx, store, deletes, nil)
		require.NoError(t, err)

		// Verify only the viewer relation was deleted
		filter := bson.M{
			"store":    store,
			"object":   "document:report",
			"relation": "editor",
			"user":     "user:john",
		}

		count, err := ds.tuplesCollection.CountDocuments(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(1), count)

		filter = bson.M{
			"store":    store,
			"object":   "document:report",
			"relation": "viewer",
		}

		count, err = ds.tuplesCollection.CountDocuments(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(0), count)
	})

	t.Run("stores_are_isolated", func(t *testing.T) {
		// Create a tuple in store1
		store1 := "store1-" + testutils.CreateRandomString(8)
		writes1 := storage.Writes{
			&openfgav1.TupleKey{
				Object:   "document:shared",
				Relation: "viewer",
				User:     "user:shared",
			},
		}

		err := ds.Write(ctx, store1, nil, writes1)
		require.NoError(t, err)

		// Create a tuple with the same values in store2
		store2 := "store2-" + testutils.CreateRandomString(8)
		writes2 := storage.Writes{
			&openfgav1.TupleKey{
				Object:   "document:shared",
				Relation: "viewer",
				User:     "user:shared",
			},
		}

		err = ds.Write(ctx, store2, nil, writes2)
		require.NoError(t, err)

		// Delete the tuple from store1 only
		deletes := storage.Deletes{
			&openfgav1.TupleKeyWithoutCondition{
				Object:   "document:shared",
				Relation: "viewer",
				User:     "user:shared",
			},
		}

		err = ds.Write(ctx, store1, deletes, nil)
		require.NoError(t, err)

		// Verify the tuple is gone from store1
		filter := bson.M{
			"store":    store1,
			"object":   "document:shared",
			"relation": "viewer",
			"user":     "user:shared",
		}

		count, err := ds.tuplesCollection.CountDocuments(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(0), count)

		// Verify the tuple still exists in store2
		filter = bson.M{
			"store":    store2,
			"object":   "document:shared",
			"relation": "viewer",
			"user":     "user:shared",
		}

		count, err = ds.tuplesCollection.CountDocuments(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
	})
}
