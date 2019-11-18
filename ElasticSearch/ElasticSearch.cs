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

        public static ElasticClient GetClient(Store.Settings settings)
        {
            if (_clients == null)
            {
                _clients = new Dictionary<string, ElasticClient>();
            }
            if (!_clients.ContainsKey(settings.GetId()))
            {
                #if DEBUG
                var isFiddlerRunning = System.Diagnostics.Process.GetProcessesByName("fiddler").Any();
                var host = isFiddlerRunning ? settings.Location.Replace("localhost", "ipv4.fiddler") : settings.Location;
                #else
                var host = settings.Location;
                #endif

                var local = new Uri(host);
                ConnectionSettings clientSettings = new ConnectionSettings(local)
                        .DisableDirectStreaming();
                _clients.Add(settings.GetId(), new ElasticClient(clientSettings));
            }
            return _clients[settings.GetId()];
        }
    }
}
