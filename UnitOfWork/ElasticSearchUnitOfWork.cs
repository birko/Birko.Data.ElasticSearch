using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Birko.Data.Patterns.UnitOfWork;
using Nest;

namespace Birko.Data.ElasticSearch.UnitOfWork;

/// <summary>
/// Collects bulk operations for batched execution.
/// Elasticsearch has no ACID transactions — this provides best-effort batching via the Bulk API.
/// </summary>
public sealed class BulkOperationContext
{
    internal readonly List<Func<BulkDescriptor, IBulkRequest>> Operations = new();

    /// <summary>
    /// The ElasticClient for executing the bulk request.
    /// </summary>
    public ElasticClient Client { get; }

    internal BulkOperationContext(ElasticClient client)
    {
        Client = client;
    }

    /// <summary>
    /// Enqueues an index (create/update) operation.
    /// </summary>
    public void Index<T>(T document, string? index = null) where T : class
    {
        Operations.Add(b => b.Index<T>(i =>
        {
            i.Document(document);
            if (index is not null) i.Index(index);
            return i;
        }));
    }

    /// <summary>
    /// Enqueues a delete operation.
    /// </summary>
    public void Delete<T>(string id, string? index = null) where T : class
    {
        Operations.Add(b => b.Delete<T>(d =>
        {
            d.Id(id);
            if (index is not null) d.Index(index);
            return d;
        }));
    }

    /// <summary>
    /// Enqueues an update operation.
    /// </summary>
    public void Update<T>(string id, T partialDocument, string? index = null) where T : class
    {
        Operations.Add(b => b.Update<T>(u =>
        {
            u.Id(id).Doc(partialDocument);
            if (index is not null) u.Index(index);
            return u;
        }));
    }
}

/// <summary>
/// Elasticsearch "Unit of Work" — collects operations and executes them as a single Bulk API call.
/// NOTE: This is NOT a true ACID transaction. Individual operations within the bulk may succeed or fail independently.
/// </summary>
public sealed class ElasticSearchUnitOfWork : IUnitOfWork<BulkOperationContext>
{
    private readonly ElasticClient _client;
    private BulkOperationContext? _context;
    private bool _disposed;

    public bool IsActive => _context is not null;
    public BulkOperationContext? Context => _context;

    public ElasticSearchUnitOfWork(ElasticClient client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
    }

    /// <summary>
    /// Creates from a configured store.
    /// </summary>
    public static ElasticSearchUnitOfWork FromStore<T>(Stores.AsyncElasticSearchStore<T> store)
        where T : Data.Models.AbstractModel
    {
        var client = store.Connector
            ?? throw new InvalidOperationException("Store connector is not initialized. Call SetSettings() first.");
        return new ElasticSearchUnitOfWork(client);
    }

    public Task BeginAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (IsActive)
            throw new TransactionAlreadyActiveException();

        _context = new BulkOperationContext(_client);
        return Task.CompletedTask;
    }

    public async Task CommitAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!IsActive)
            throw new NoActiveTransactionException();

        if (_context!.Operations.Count > 0)
        {
            var response = await _client.BulkAsync(b =>
            {
                foreach (var op in _context.Operations)
                {
                    op(b);
                }
                return b;
            }, ct);

            if (!response.IsValid)
            {
                throw new UnitOfWorkException(
                    $"Elasticsearch bulk operation failed: {response.ServerError?.Error?.Reason ?? response.OriginalException?.Message ?? "Unknown error"}",
                    response.OriginalException);
            }

            if (response.Errors)
            {
                var failedCount = response.ItemsWithErrors?.Count() ?? 0;
                throw new UnitOfWorkException(
                    $"Elasticsearch bulk operation partially failed: {failedCount} items had errors.");
            }
        }

        _context = null;
    }

    public Task RollbackAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!IsActive)
            throw new NoActiveTransactionException();

        // Just discard collected operations — nothing was sent to ES yet.
        _context = null;
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _context = null;
        }
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _context = null;
        }
    }
}
