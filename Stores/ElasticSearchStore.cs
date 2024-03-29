﻿using Birko.Data.ElasticSearch;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Birko.Data.Stores
{
    public class ElasticSearchStore<T> : AbstractStore<T>
         where T : Models.AbstractModel
    {
        public ElasticClient Connector { get; private set; }
        private ElasticSearch.Settings _settings = null;

        private Dictionary<Guid, T> _insertList = null;
        private Dictionary<Guid, T> _updateList = null;
        private Dictionary<Guid, T> _deleteList = null;

        public static int MaxResultWindow { get; set; } = 10000;

        public ElasticSearchStore() : base ()
        {
        }

        public override void SetSettings(ISettings settings)
        {
            if (settings is ElasticSearch.Settings sets)
            {
                _settings = sets;
                Connector = Data.ElasticSearch.ElasticSearch.GetClient(_settings);
                _insertList = new Dictionary<Guid, T>();
                _updateList = new Dictionary<Guid, T>();
                _deleteList = new Dictionary<Guid, T>();
            }
        }

        public override long Count(Expression<Func<T, bool>> filter)
        {
            return Count(Data.ElasticSearch.ElasticSearch.ParseExpression(filter));
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

        public override void Delete(T data)
        {
            if (data != null && data.Guid != null && !_deleteList.ContainsKey(data.Guid.Value))
            {
                _deleteList.Add(data.Guid.Value, data);
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

        public override void List(Expression<Func<T, bool>> filter, Action<T> listAction, int? limit = null, int? offset = null)
        {
            List(Data.ElasticSearch.ElasticSearch.ParseExpression(filter), listAction, limit, offset);
        }

        public void List(SearchRequest request, Action<T> listAction)
        {
            if (request != null)
            {
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
                long end = (size != null) ? (count + size ?? 12 ) : searchResponse.Total;
                if(end > searchResponse.Total)
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
                                break;
                            }
                            listAction?.Invoke(document);
                            count++;
                        }
                    }
                    else
                    {
                        skip -= searchResponse.Documents.Count;
                    }
                    if (count < end)
                    {
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
                        if(searchResponse.Total <= 0)
                        {
                            throw new Exception("Connection exception");
                        }
                    }
                }
                if (!string.IsNullOrEmpty(scrollId) && scrollTime != null)
                {
                    Connector.ClearScroll(new Nest.ClearScrollRequest(scrollId));
                }
            }
        }

        public void List(QueryContainer query, Action<T> listAction, int? limit = null, int? offset = null)
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
            List(request, listAction);
        }

        public override void Save(T data, StoreDataDelegate<T> storeDelegate = null)
        {
            if (data != null)
            {
                bool newItem = data.Guid == null;
                if (newItem) // new
                {
                    data.Guid = Guid.NewGuid();
                }
                data = storeDelegate?.Invoke(data) ?? data;
                if (data != null)
                {
                    if (newItem)
                    {
                        _insertList[data.Guid.Value] = data;
                    }
                    else
                    {
                        if (data is Models.AbstractLogModel)
                        {
                            (data as Models.AbstractLogModel).PrevUpdatedAt = (data as Models.AbstractLogModel).UpdatedAt;
                            (data as Models.AbstractLogModel).UpdatedAt = DateTime.UtcNow;
                        }
                        _updateList[data.Guid.Value] = data;
                    }
                }
            }
        }

        public override void StoreChanges()
        {
            if (Connector != null)
            {
                string indexName = GetIndexName();

                //delete
                while (_deleteList.Count > 0)
                {
                    var kvp = _deleteList.First();
                    Connector.Delete<T>(kvp.Key, (i) => i.Index(indexName));
                    _deleteList.Remove(kvp.Key);
                }
                //update
                while (_updateList.Count > 0)
                {
                    var kvp = _updateList.First();
                    Connector.Index<T>(kvp.Value, i => i.Id(kvp.Key).Index(indexName));
                    _updateList.Remove(kvp.Key);
                }
                //insert
                while (_insertList.Count > 0)
                {
                    var kvp = _insertList.First();
                    Connector.Index(kvp.Value, i => i.Id(kvp.Key).Index(indexName));
                    _insertList.Remove(kvp.Key);
                }
            }
            else
            {
                throw new Exceptions.StoreException("No database connector provided");
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
