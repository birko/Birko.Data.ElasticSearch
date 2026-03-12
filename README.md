# Birko.Data.ElasticSearch

Elasticsearch implementation for the Birko Framework providing full-text search and document storage.

## Features

- Full-text search with tokenization, stemming, and relevance scoring
- Document-based CRUD operations (sync/async)
- Bulk operations optimized for Elasticsearch
- Aggregations and pagination
- Index mapping and management

## Installation

```bash
dotnet add package Birko.Data.ElasticSearch
```

## Dependencies

- Birko.Data
- NEST (Elasticsearch .NET client)

## Usage

```csharp
using Birko.Data.ElasticSearch.Stores;

var settings = new ElasticSearchSettings
{
    Uri = "http://localhost:9200",
    IndexName = "products",
    Username = "elastic",
    Password = "password"
};

var store = new ElasticSearchStore<Product>(settings);
var id = store.Create(product);
```

### Search

```csharp
var response = Client.Search<Product>(s => s
    .Index(Settings.IndexName)
    .Query(q => q.MultiMatch(m => m
        .Query("search term")
        .Fields(f => f.Field(p => p.Name).Field(p => p.Description))
    ))
    .From(0).Size(20)
    .Sort(sort => sort.Descending(f => f.CreatedAt))
);
```

## API Reference

### Stores

- **ElasticSearchStore\<T\>** - Sync store
- **ElasticSearchBulkStore\<T\>** - Bulk operations
- **AsyncElasticSearchStore\<T\>** - Async store
- **AsyncElasticSearchBulkStore\<T\>** - Async bulk store

### Repositories

- **ElasticSearchRepository\<T\>** / **ElasticSearchBulkRepository\<T\>**
- **AsyncElasticSearchRepository\<T\>** / **AsyncElasticSearchBulkRepository\<T\>**

## Related Projects

- [Birko.Data](../Birko.Data/) - Core interfaces
- [Birko.Data.ElasticSearch.ViewModel](../Birko.Data.ElasticSearch.ViewModel/) - ViewModel repositories

## License

Part of the Birko Framework.
