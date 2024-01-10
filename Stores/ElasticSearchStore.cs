using Birko.Data.ElasticSearch;
using Nest;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;

namespace Birko.Data.Stores
{
    public class ElasticSearchStore<T> 
        : AbstractStore<T>
        , ISettingsStore<ElasticSearch.Settings>
         where T : Models.AbstractModel
    {
        public ElasticClient Connector { get; private set; }
        protected ElasticSearch.Settings _settings = null;

        public static int MaxResultWindow { get; set; } = 10000;

        public ElasticSearchStore() : base ()
        {
        }

        public virtual void SetSettings(ElasticSearch.Settings settings)
        {
            if (settings is ElasticSearch.Settings sets)
            {
                _settings = sets;
                Connector = Data.ElasticSearch.ElasticSearch.GetClient(_settings);
            }
        }

        public override long Count(Expression<Func<T, bool>>? filter = null)
        {
            return Count(filter != null ? Data.ElasticSearch.ElasticSearch.ParseExpression(filter) : null);
        }

        public long Count(QueryContainer query)
        {
            string indexName = GetIndexName();
            var request = new CountRequest(indexName);
            if (query != null)
            {
                request.Query = query;
            }
            return Connector.Count(request).Count;
        }

        public override T? ReadOne(Expression<Func<T, bool>>? filter = null)
        {
            var searchResponse = Connector.Search<T>(new SearchRequest()
            {
                Size = 1,
                From = 0,
                Query = Data.ElasticSearch.ElasticSearch.ParseExpression(filter)
            });

            return (searchResponse.Total > 0) ? searchResponse.Documents.FirstOrDefault() : null;
        }
        public override void Create(T data, StoreDataDelegate<T>? storeDelegate = null)
        {
            if (data != null)
            {
                data.Guid = Guid.NewGuid();
                storeDelegate?.Invoke(data);
                Connector.Create(data, i => i.Id(data.Guid).Index(GetIndexName()));
            }
        }

        public override void Update(T data, StoreDataDelegate<T>? storeDelegate = null)
        {
            if (data != null && data.Guid != null && data.Guid != Guid.Empty)
            {
                storeDelegate?.Invoke(data);
                Connector.Update<T, T>(data.Guid, (i) => i.Index(GetIndexName()).Doc(data));
            }
        }

        public override void Delete(T data)
        {
            if (data != null && data.Guid != null && data.Guid != Guid.Empty)
            {
                Connector.Delete<T>(data.Guid, (i) => i.Index(GetIndexName()));
            }
        }

        public void Init(Func<CreateIndexDescriptor, ICreateIndexRequest> indexDescriptor)
        {
            var indexName = GetIndexName();
            if (!Connector.Indices.Exists(indexName).Exists)
            {
                _ = Connector.Indices.Create(indexName, indexDescriptor);
            }
        }

        public override void Init()
        {
            Init(cid =>
                cid.Map<T>(m => m.AutoMap())
            );
        }

        public void DeleteIndex()
        {
            DeleteIndex(GetIndexName());
        }

        public void DeleteIndex(string name)
        {
            if (!string.IsNullOrEmpty(name))
            {
                var indexName = string.Format("{0}_{1}", _settings.Name, name).ToLower();
                _ = Connector.Indices.Delete(indexName);
            }
        }

        public string GetIndexName()
        {
            var type = typeof(T);
            string indexName = _settings.IndexSettings?.FirstOrDefault(x => x.TypeName == type.FullName)?.Name ?? type.Name;
            return $"{_settings.Name}_{indexName}".ToLower();
        }

        public override void Destroy()
        {
            DeleteIndex();
        }

        public void ClearCache()
        {
            Connector.Indices.ClearCache(GetIndexName());
        }
    }
}
