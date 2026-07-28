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

### Search Result Highlighting
- `HighlightOptions` - Configuration for search result highlighting (pre/post tags, fragment size)
- `SearchResult<T>` - Wrapper for search results including highlight data
- `HighlightedSearchResults<T>` - Collection of search results with per-field highlight fragments
- Supports customizable pre/post tags, field-specific highlighting, and fragment count

### Index Management
- `IndexInfo` - DTO containing index name, health, status, document count, size, alias list
- `ReindexResult` - DTO containing success flag, documents indexed, duration, failures list
- `IndexManager` - Index CRUD operations, settings updates, mappings, alias management, index templates, cache/refresh/flush operations
- `ReindexHelper` - Basic reindex between indices, reindex with Painless script transformation, zero-downtime reindex via alias swap

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

### Filter translation (`ElasticSearch.ParseExpression`)

Store CRUD filters (`Expression<Func<T, bool>>`) are translated to NEST queries by the hand-rolled
`ElasticSearch.ParseExpression`. Supported forms — kept in parity with the SQL parser and the native-LINQ
backends (verified in `Birko.Data.ElasticSearch.Tests.ExpressionDivergenceTests`):

- Comparisons `== != < <= > >=` (→ term / numeric range), and `== null` / `!= null` (→ must-not-exists / exists)
- Logical combinators: `&&`/`||` **and** the bitwise `&`/`|` on booleans (→ bool must / should), plus `!` (→ must-not)
- Bare boolean member `x => x.IsActive` (→ `IsActive == true`) and constant predicates `x => true` / `x => false`
  (→ `MatchAllQuery` / `MatchNoneQuery`; `x => true` is the idiomatic "read all" filter)
- String `StartsWith` (→ prefix), `EndsWith` (→ leading-wildcard, the `LIKE '%x'` equivalent), `Contains` (→ query-string),
  `IsNullOrEmpty`
- The IN pattern `collection.Contains(x.Member)` (→ terms query) and array membership `x.ArrayMember.Contains(value)`
  (→ term)
- Nullable `x.NullableProp.HasValue` (→ exists), `Any(...)` on nested collections (→ nested query), and `MultiMatch`
- **Ternary `c ? t : f` and null-coalescing `a ?? b`** (boolean position) — `ParseLambda` runs the shared
  `Birko.Data.Expressions.ExpressionNormalizer` (Birko.Data.Core) at the lambda boundary first, which
  funcletizes parameter-free subtrees and desugars a boolean ternary to `(c && t) || (!c && f)` and a
  boolean `a ?? b` to `(a == true) || (a == null && b)`. ES then translates the resulting AND/OR/NOT tree
  with no ternary-specific code. Same normalizer the SQL parser adopted (STORY-047).
- **Value-expression operands → Painless script query** — column arithmetic (`x.A + x.B > 5`),
  value null-coalescing (`(x.Score ?? 0) > 5`) and a value-position ternary compared to something
  (`(x.Vip ? x.A : x.B) > 5`) cannot be a term/range query, so `ParseComparison` emits a
  `ScriptQuery` with a Painless script (`ScriptValue` / `ScriptBool` / `ScriptConstant`). Field access
  is `doc['field'].value`; a `??` renders its missing-fallback as `(doc['f'].size() == 0 ? fallback :
  doc['f'].value)`; every field accessed **outside** a coalesce is collected into an existence guard
  `(doc['a'].size() > 0 && …) ? (body) : false`, so a missing/null field yields `false` (C#
  null-propagation → doc excluded) instead of a Painless runtime error. Constants inline as portable
  literals (numeric / bool / enum→int / escaped string); non-scriptable shapes throw
  `NotSupportedException` rather than silently dropping the filter.

Caveat: `ToLower()`/`ToUpper()` are treated as transparent (the wrapped column resolves, but the case
transformation is not applied) — case-insensitivity is delegated to the field's analyzer, mirroring how the
SQL parser delegates to the column collation.

Note: the script-query path needs the referenced fields to be `doc_values`-enabled (the default for
numeric/keyword/boolean; `text` fields are not — use their `.keyword` sub-field, which the renderer already
appends for strings). Runtime cost is per-document script evaluation, so scope such filters with other
term/range clauses where possible.

#### An empty/null collection in `Contains` matches nothing

`ParseContains` returned `null` for an empty terms array, and `CombineBool` **drops** null sub-queries. So
`ids.Contains(x.Field) && x.Status == active` with an empty `ids` collapsed to `x.Status == active` — the
membership filter silently vanished and the query returned everything matching the remaining clauses; as the
only clause it became an unfiltered read. This is the worst variant of the family (SQL either returned no rows
or raised a syntax error; here the query silently returns **wrong rows**) and it is reachable from ordinary
code — an empty collection is the normal outcome of the canonical batch pattern (fetch parents, then filter
children by their ids).

Both branches now emit `MatchNoneQuery` — the store's own vocabulary for a constant-false predicate, matching
the native-LINQ backends (InMemory/JSON/XML/Raven/Cosmos), where an empty collection contains nothing:

| Input | Query |
|-------|-------|
| empty collection | `MatchNoneQuery` |
| null collection | `MatchNoneQuery` |
| negated empty | `MustNot(MatchNone)` → every document, mirroring SQL's `1 = 1` for an empty `NOT IN` |
| single element | still a real `TermsQuery` (boundary) |

#### Untranslatable filters fail loudly at every boundary (CR-H047)

A NEST request with `Query = null` carries no query, which ES reads as **match-all** — so a supplied-but-
untranslatable filter turned reads into "return everything", and reached `_delete_by_query` /
`_update_by_query` targeting the whole index. The invariant used to be enforced in
`ElasticSearchViewStore.BuildFilterQuery` only; the main entity stores assigned the parser's output straight to
their requests across 14 sites with no null checks. Two shared helpers now own it and every filter→query
conversion routes through one of them (the view store delegates too, so the paths cannot drift):

| Helper | Null filter | Untranslatable filter |
|--------|-------------|----------------------|
| `ParseFilterQuery<T>` (optional filter — reads/counts) | `null` query = read everything **on purpose** | `NotSupportedException` |
| `ParseRequiredFilterQuery<T>` (destructive by-query paths — Delete/Update) | `ArgumentNullException` | `NotSupportedException` |

The optional/required split matters: `ReadCore`/`ReadCoreAsync` take a **nullable** filter, so a filterless read
is legitimate and must not throw; only the four genuinely destructive paths use the required variant. Three
outcomes stay distinct and only one is an error — *no filter supplied* (null query), *matches nothing*
(`MatchNoneQuery`, a legitimate translation — hence the guard is a null check, not a falsy check), *cannot be
expressed* (throw).

Deliberately unchanged: the parser still returns `null` for its ~20 other "cannot translate this shape" paths
rather than throwing at source. Guarding at the boundary is the smaller change and now covers every consumer
path; moving the throw into the parser is a behaviour change for consumers and remains an open option.

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
