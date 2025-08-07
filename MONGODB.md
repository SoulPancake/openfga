# MongoDB Storage Backend for OpenFGA

This document describes the MongoDB storage backend implementation for OpenFGA.

## Overview

The MongoDB storage backend provides a complete implementation of the OpenFGA storage interface using MongoDB as the underlying database. It supports all OpenFGA operations including:

- Tuple storage and retrieval
- Authorization model management  
- Store management
- Assertions handling
- Change log tracking

## Configuration

To use MongoDB as your storage backend, configure OpenFGA with the following settings:

### Command Line

```bash
openfga run --datastore-engine mongo --datastore-uri "mongodb://localhost:27017/openfga"
```

### Environment Variables

```bash
export OPENFGA_DATASTORE_ENGINE=mongo
export OPENFGA_DATASTORE_URI="mongodb://localhost:27017/openfga"
```

### Configuration File (config.yaml)

```yaml
datastore:
  engine: mongo
  uri: mongodb://localhost:27017/openfga
  username: optional_username
  password: optional_password
  max-open-conns: 30
  max-idle-conns: 10
  conn-max-idle-time: 30m
  conn-max-lifetime: 1h
  metrics:
    enabled: true
```

## Connection URI Format

The MongoDB connection URI follows the standard MongoDB connection string format:

```
mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultauthdb][?options]]
```

Examples:
- `mongodb://localhost:27017/openfga` - Local MongoDB with database name
- `mongodb://user:pass@localhost:27017/openfga` - With authentication
- `mongodb://localhost:27017,localhost:27018/openfga?replicaSet=rs0` - Replica set
- `mongodb+srv://cluster.example.com/openfga` - MongoDB Atlas

## Database Design

The MongoDB storage backend uses the following collections:

### Collections

1. **tuples** - Stores relationship tuples
   - Indexes: compound index on (store, object_type, object_id, relation, user)
   - Indexes: reverse lookup index on (store, user, object_type, relation)

2. **authorization_models** - Stores authorization models
   - Indexes: compound index on (store, id)

3. **stores** - Stores OpenFGA stores
   - Indexes: unique index on (id)

4. **assertions** - Stores test assertions
   - Indexed by (store, model_id)

5. **changelog** - Stores tuple change history
   - Indexes: compound index on (store, ulid)

## Features

### Transactions
- Uses MongoDB transactions for atomic writes
- Ensures consistency between tuple operations and changelog entries

### Indexing
- Optimized indexes for common query patterns
- Supports efficient reverse lookups for ReadStartingWithUser
- Compound indexes for multi-field queries

### Pagination
- Uses ULID-based pagination for consistent ordering
- Supports continuation tokens for large result sets

### Error Handling
- Proper MongoDB error mapping to OpenFGA storage errors
- Connection retry with exponential backoff
- Graceful handling of duplicate key errors

## Testing

The MongoDB storage backend includes comprehensive tests:

```bash
# Unit tests (no MongoDB required)
go test ./pkg/storage/mongo/... -short

# Integration tests (requires MongoDB)
go test ./pkg/storage/mongo/... 
```

For integration tests, ensure MongoDB is running locally:

```bash
# Using Docker
docker run -d -p 27017:27017 mongo:latest

# Run integration tests
go test ./pkg/storage/mongo/... -v
```