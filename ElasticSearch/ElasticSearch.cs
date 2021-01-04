using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Birko.Data.ElasticSearch
{
    public static class ElasticSearch
    {
        private static Dictionary<string, ElasticClient> _clients;

        public static ElasticClient GetClient(Stores.Settings settings)
        {
            if (_clients == null)
            {
                _clients = new Dictionary<string, ElasticClient>();
            }
            if (!_clients.ContainsKey(settings.GetId()))
            {
                var local = new Uri(settings.Location);
                ConnectionSettings clientSettings = new ConnectionSettings(local)
                        .DisableDirectStreaming();
                _clients.Add(settings.GetId(), new ElasticClient(clientSettings));
            }
            return _clients[settings.GetId()];
        }
    }
}
