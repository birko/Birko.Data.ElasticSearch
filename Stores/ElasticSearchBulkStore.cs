using Birko.Data.ElasticSearch;
using Birko.Data.ViewModels;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Birko.Data.Stores
{
    public class ElasticSearchBulkStore<T> : ElasticSearchStore<T>, IBulkStore<T>
         where T : Models.AbstractModel
    {
        public ElasticSearchBulkStore() : base ()
        {
        }

        public void Create(IEnumerable<T> data, StoreDataDelegate<T>? storeDelegate = null)
        {
            Bulk(data?.Where(x=>x!= null).Select(x=> {
                x.Guid = Guid.NewGuid();
                storeDelegate?.Invoke(x);
                return x;
            }), null);
        }

        public void Delete(IEnumerable<T> data)
        {
            Bulk(null, null, data.Where(x=> x!= null && x.Guid != null && x.Guid != Guid.Empty));
        }

        public void Update(IEnumerable<T> data, StoreDataDelegate<T>? storeDelegate = null)
        {
            Bulk(null, data?.Where(x => x != null && x.Guid != null && x.Guid!= Guid.Empty).Select(x =>
            {
                storeDelegate?.Invoke(x);
                return x;
            }), null);
        }


        protected void Bulk(IEnumerable<T>? create = null, IEnumerable<T>? update = null, IEnumerable<T>? delete = null)
        {
            if(!((create?.Any() ?? false) && (update?.Any() ?? false) && (delete?.Any() ?? false)))
            {
                return;
            }

            var indexName = GetIndexName();
            Connector?.Bulk((bulk) => {
                if (create?.Any() ?? false)
                {
                    bulk = bulk.CreateMany<T>(create, (i, o) => i.Id(o.Guid).Index(indexName));
                }
                if (update?.Any() ?? false)
                {
                    bulk = bulk.UpdateMany<T>(update, (i, o) => i.Id(o.Guid).Index(indexName).Doc(o));
                }
                if (delete?.Any() ?? false)
                {
                    bulk = bulk.DeleteMany<T>(delete, (i, o) => i.Id(o.Guid).Index(indexName));
                }
                return bulk;
            }
            );
        }

        public IEnumerable<T> Read(SearchRequest request)
        {
            if (request == null)
            {
                yield break;
            }
            int count = request.From ?? 0;
            Time scrollTime = null;
            string scrollId;
            int? size = request.Size;
            int skip = 0;
            var maxResultWindow = _settings.IndexSettings
                ?.FirstOrDefault(x => x.TypeName == typeof(T).FullName)?.MaxResultWindow ?? MaxResultWindow;
            if ((request.From == null && request.Size == null)
               || ((request.Size ?? 0) + count) >= maxResultWindow)
            {
                scrollTime = new Time(new TimeSpan(0, 1, 0));
                request.Scroll = scrollTime;
                request.Size = 1000;
                request.From = null;
                if (count != 0)
                {
                    skip = count;
                }
            }

            var searchResponse = Connector.Search<T>(request);
            scrollId = searchResponse.ScrollId;
            long end = (size != null) ? (count + size ?? 12) : searchResponse.Total;
            if (end > searchResponse.Total)
            {
                end = searchResponse.Total;
            }
            while (count < end)
            {
                if (searchResponse.Documents.Count >= skip)
                {
                    foreach (var document in searchResponse.Documents)
                    {
                        if (skip > 0)
                        {
                            skip--;
                            continue;
                        }
                        if (count >= end)
                        {
                            yield break;
                        }
                        yield return document;
                        count++;
                    }
                }
                else
                {
                    skip -= searchResponse.Documents.Count;
                }
                if (count > end)
                {
                    yield break;
                }
                if (!string.IsNullOrEmpty(scrollId) && scrollTime != null)
                {
                    searchResponse = Connector.Scroll<T>(new Nest.ScrollRequest(scrollId, scrollTime));
                    scrollId = searchResponse.ScrollId;
                }
                else
                {
                    request.From = count;
                    searchResponse = Connector.Search<T>(request);
                }
                if (searchResponse.Total <= 0)
                {
                    throw new Exception("Connection exception");
                }
            }

            if (!string.IsNullOrEmpty(scrollId) && scrollTime != null)
            {
                Connector.ClearScroll(new Nest.ClearScrollRequest(scrollId));
            }
        }

        public IEnumerable<T> Read(Expression<Func<T, bool>>? filter = null, int? limit = null, int? offset = null)
        {
            foreach (T item in Read(Data.ElasticSearch.ElasticSearch.ParseExpression(filter), limit, offset))
            {
                yield return item;
            }
        }


        public IEnumerable<T> Read(QueryContainer query, int? limit = null, int? offset = null)
        {
            string indexName = GetIndexName();
            SearchRequest request = new SearchRequest(indexName)
            {
                Size = limit,
                From = offset
            };
            if (query != null)
            {
                request.Query = query;
            }
            foreach (T item in Read(request)) 
            {
                yield return item;
            }
        }
    }
}
