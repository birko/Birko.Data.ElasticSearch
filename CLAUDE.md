# Birko.Data.ElasticSearch

## Overview
Elasticsearch implementation for the Birko data layer providing full-text search and document storage.

## Project Location
`C:\Source\Birko.Data.ElasticSearch\`

## Purpose
- Full-text search capabilities
- Document-based storage
- Real-time indexing and querying
- Distributed search architecture

## Components

### Stores
- `ElasticSearchStore<T>` - Synchronous Elasticsearch store
- `ElasticSearchBulkStore<T>` - Bulk operations store
- `AsyncElasticSearchStore<T>` - Asynchronous Elasticsearch store
- `AsyncElasticSearchBulkStore<T>` - Async bulk operations store

### Repositories
- `ElasticSearchRepository<T>` - Elasticsearch repository
- `ElasticSearchBulkRepository<T>` - Bulk repository
- `AsyncElasticSearchRepository<T>` - Async repository
- `AsyncElasticSearchBulkRepository<T>` - Async bulk repository

## Connection

Connection settings:
```csharp
var settings = new ElasticSearchSettings
{
    Uri = "http://localhost:9200",
    IndexName = "entities",
    Username = "elastic",
    Password = "password"
};
```

## Implementation

```csharp
using Birko.Data.ElasticSearch.Stores;
using Nest;

public class ProductStore : ElasticSearchStore<Product>
{
    public ProductStore(ElasticSearchSettings settings) : base(settings)
    {
    }

    public override Guid Create(Product item)
    {
        var response = Client.Index(item, i => i
            .Index(Settings.IndexName)
            .Id(item.Id.ToString())
            .Refresh(Refresh.True)
        );
        return item.Id;
    }

    public override void Read(Product item)
    {
        var response = Client.Get<Product>(item.Id.ToString(), g => g
            .Index(Settings.IndexName)
        );

        if (response.Found)
        {
            CopyProperties(response.Source, item);
        }
        else
        {
            throw new NotFoundException($"Product {item.Id} not found");
        }
    }
}
```

## Bulk Operations

```csharp
public override IEnumerable<KeyValuePair<Product, Guid>> CreateAllAsync(IEnumerable<Product> items)
{
    var bulkResponse = Client.Bulk(b => b
        .Index(Settings.IndexName)
        .Refresh(Refresh.True)
        .UpdateMany(items, (descriptor, item) => descriptor
            .Index(Settings.IndexName)
            .Id(item.Id.ToString())
            .Doc(item)
            .DocAsUpsert(true)
        )
    );

    return items.Select(item => new KeyValuePair<Product, Guid>(item, item.Id));
}
```

## Search

Elasticsearch excels at search:

```csharp
public IEnumerable<Product> Search(string query)
{
    var response = Client.Search<Product>(s => s
        .Index(Settings.IndexName)
        .Query(q => q
            .MultiMatch(m => m
                .Query(query)
                .Fields(f => f
                    .Field(p => p.Name)
                    .Field(p => p.Description)
                )
            )
        )
    );

    return response.Documents;
}
```

## Index Mapping

Define mapping for your types:

```csharp
Client.Indices.Create(Settings.IndexName, c => c
    .Map<Product>(m => m
        .Properties(p => p
            .Keyword(t => t.Name(n => n.Id))
            .Text(t => t.Name(n => n.Name).Analyzer("standard"))
            .Text(t => t.Name(n => n.Description))
            .Number(t => t.Name(n => n.Price).Type(NumberType.Double))
            .Date(t => t.Name(n => n.CreatedAt))
        )
    )
);
```

## Dependencies
- Birko.Data.Core
- Birko.Data.Stores
- Nest (Elasticsearch .NET client)
- Elasticsearch (7.x or 8.x)

## Features

### Full-Text Search
- Tokenization and stemming
- Relevance scoring
- Highlighting
- Auto-suggest

### Aggregations
```csharp
var response = Client.Search<Product>(s => s
    .Size(0)
    .Aggregations(a => a
        .Terms("categories", t => t
            .Field(f => f.Category)
            .Size(10)
        )
    )
);
```

### Pagination
```csharp
var response = Client.Search<Product>(s => s
    .From(0)
    .Size(20)
    .Query(q => q.MatchAll())
);
```

### Sorting
```csharp
var response = Client.Search<Product>(s => s
    .Sort(sort => sort
        .Descending(f => f.CreatedAt)
        .Ascending(f => f.Name)
    )
);
```

## Data Types

Common .NET to Elasticsearch mappings:
- `Guid` → `keyword`
- `string` → `text` or `keyword`
- `int` → `integer`
- `long` → `long`
- `double` → `double`
- `decimal` → `scaled_float`
- `bool` → `boolean`
- `DateTime` → `date`
- `List<T>` → `nested` or `object`

## Best Practices

### Index Naming
Use lowercase with dashes:
```
products-v1
customer-orders
app-logs
```

### Refresh Strategy
- `Refresh.True` - Immediate (not for bulk)
- `Refresh.False` - Default
- `Refresh.WaitFor` - Wait for refresh

### Bulk Size
Optimal bulk size is typically 1-5 MB per bulk request.

### Mapping
Define mappings before indexing to avoid dynamic mapping issues.

## Reference Implementation
This is an excellent reference implementation for:
- Async operations
- Bulk operations
- Settings pattern
- Connector management

## Limitations
- Not a primary database (should be used alongside a primary store)
- Eventual consistency
- No complex transactions
- Memory-intensive for large results

## Maintenance

### README Updates
When making changes that affect the public API, features, or usage patterns of this project, update the README.md accordingly. This includes:
- New classes, interfaces, or methods
- Changed dependencies
- New or modified usage examples
- Breaking changes

### CLAUDE.md Updates
When making major changes to this project, update this CLAUDE.md to reflect:
- New or renamed files and components
- Changed architecture or patterns
- New dependencies or removed dependencies
- Updated interfaces or abstract class signatures
- New conventions or important notes

### Test Requirements
Every new public functionality must have corresponding unit tests. When adding new features:
- Create test classes in the corresponding test project
- Follow existing test patterns (xUnit + FluentAssertions)
- Test both success and failure cases
- Include edge cases and boundary conditions
