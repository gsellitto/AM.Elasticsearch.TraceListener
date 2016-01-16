﻿using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using Elasticsearch.Net;

//using Nest;

using Newtonsoft.Json.Linq;
using System.Threading;
using System.Xml.XPath;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Xml.Linq;
using System.Xml;
using System.Dynamic;
using System.Collections.Concurrent;
using System.Reactive.Linq;
using System.Reactive.Concurrency;
using Elasticsearch.Net.Connection;

namespace ElasticSearch.Diagnostics
{
    /// <summary>
    /// A TraceListener class used to submit trace data to elasticsearch
    /// </summary>
    public class ElasticSearchTraceListener : TraceListenerBase
    {
        private readonly BlockingCollection<JObject> _queueToBePosted = new BlockingCollection<JObject>();

        private ElasticsearchClient _client;
        //private ElasticClient _client;

        /// <summary>
        /// Uri for the ElasticSearch server
        /// </summary>
        public Uri Uri { get; private set; }

        /// <summary>
        /// prefix for the Index for traces
        /// </summary>
        public string Index
        {
            get
            {
                return this.ElasticSearchTraceIndex.ToLower() + "-" + DateTime.UtcNow.ToString("yyyy-MM-dd");
            }
            //private set; }
        }


        private static readonly string[] _supportedAttributes = new string[]
            {
                "ElasticSearchUri", "elasticSearchUri", "elasticsearchuri",
                "ElasticSearchIndex", "elasticSearchIndex", "elasticsearchindex",
                "ElasticSearchTraceIndex", "elasticSearchTraceIndex", "elasticsearchtraceindex",
            };

        /// <summary>
        /// Allowed attributes for this trace listener.
        /// </summary>
        protected override string[] GetSupportedAttributes()
        {
            return _supportedAttributes;
        }


        /// <summary>
        /// Uri for the ElasticSearch server
        /// </summary>
        public string ElasticSearchUri
        {
            get
            {
                if (Attributes.ContainsKey("elasticsearchuri"))
                {
                    return Attributes["elasticsearchuri"];
                }
                else
                {
                    //return _defaultTemplate;
                    throw new ArgumentException("elasticsearchuri attribute is not defined");
                }
            }
            set
            {
                Attributes["elasticsearchuri"] = value;
            }
        }

        /// <summary>
        /// prefix for the Index for traces
        /// </summary>
        public string ElasticSearchIndex
        {
            get
            {
                if (Attributes.ContainsKey("elasticsearchindex"))
                {
                    return Attributes["elasticsearchindex"];
                }
                else
                {
                    //return _defaultTemplate;
                    throw new ArgumentException("elasticsearchindex attribute is not defined");
                }
            }
            set
            {
                Attributes["elasticsearchindex"] = value;
            }
        }


        /// <summary>
        /// prefix for the Index for traces
        /// </summary>
        public string ElasticSearchTraceIndex
        {
            get
            {
                if (Attributes.ContainsKey("elasticsearchtraceindex"))
                {
                    return Attributes["elasticsearchtraceindex"];
                }
                else
                {
                    //return _defaultTemplate;
                    throw new ArgumentException("elasticsearchtraceindex attribute is not defined");
                }
            }
            set
            {
                Attributes["elasticsearchtraceindex"] = value;
            }
        }



        /// <summary>
        /// Gets a value indicating the trace listener is thread safe.
        /// </summary>
        /// <value>true</value>
        public override bool IsThreadSafe
        {
            get
            {
                return true;
            }
        }

        public ElasticsearchClient Client
        //public ElasticClient Client
        {
            get
            {
                if (_client != null)
                {
                    return _client;
                }
                else
                {
                    Uri = new Uri(this.ElasticSearchUri);

                    //Index = this.ElasticSearchTraceIndex.ToLower() + "-" + DateTime.UtcNow.ToString("yyyy-MM-dd");
                    //var cs = new ConnectionSettings(Uri);
                    //cs.ExposeRawResponse();
                    //cs.ThrowOnElasticsearchServerExceptions();

                    var cc = new ConnectionConfiguration(Uri);
                    cc.ThrowOnElasticsearchServerExceptions();

                    this._client = new ElasticsearchClient(cc, null, null, new Elasticsearch.Net.JsonNet.ElasticsearchJsonNetSerializer() );
                    //this._client = new ElasticClient(cs);
                    return this._client;
                }
            }
        }

        /// <summary>
        /// We cant grab any of the attributes until the class and more importantly its base class has finsihed initializing
        /// so keep the constructor at a minimum
        /// </summary>
        public ElasticSearchTraceListener() : base()
        {
            Initialize();
        }

        /// <summary>
        /// We cant grab any of the attributes until the class and more importantly its base class has finsihed initializing
        /// so keep the constructor at a minimum
        /// </summary>
        public ElasticSearchTraceListener(string name) : base(name)
        {
            Initialize();
        }

        private void Initialize()
        {
            //SetupObserver();
            SetupObserverBatchy();
        }

        private Action<JObject> _scribeProcessor;

        private void SetupObserver()
        {
            _scribeProcessor = a => WriteDirectlyToES(a);

            //this._queueToBePosted.GetConsumingEnumerable()
            //.ToObservable(Scheduler.Default)
            //.Subscribe(x => WriteDirectlyToES(x));
        }


        private void SetupObserverBatchy()
        {
            _scribeProcessor = a => WriteToQueueForprocessing(a);

            this._queueToBePosted.GetConsumingEnumerable()
                .ToObservable(Scheduler.Default).Buffer(5)
                .Subscribe(x => this.WriteDirectlyToESAsBatch(x));
        }



        /// <summary>
        /// Write trace event with data.
        /// </summary>
        protected override void WriteTrace(
            TraceEventCache eventCache,
            string source,
            TraceEventType eventType,
            int id,
            string message,
            Guid? relatedActivityId,
            object data)
        {

            if (eventCache != null && eventCache.Callstack.Contains("Elasticsearch.Net.ElasticsearchClient"))
            {
                return;
            }

            string updatedMessage = message;
            JObject payload = null;
            if (data != null)
            {
                if (data is Exception)
                {
                    updatedMessage = ((Exception)data).Message;
                    payload = JObject.FromObject(data);
                }
                else if (data is XPathNavigator)
                {
                    var xdata = data as XPathNavigator;
                    //xdata.MoveToRoot();

                    XDocument xmlDoc;
                    try
                    {
                        xmlDoc = XDocument.Parse(xdata.OuterXml);

                    }
                    catch (Exception)
                    {
                        xmlDoc = XDocument.Parse(xdata.ToString());
                        //eat
                        //throw;
                    }

                    // Convert the XML document in to a dynamic C# object.
                    dynamic xmlContent = new ExpandoObject();
                    ExpandoObjectHelper.Parse(xmlContent, xmlDoc.Root);

                    string json = JsonConvert.SerializeObject(xmlContent);
                    payload = JObject.Parse(json);
                }
                else if (data is DateTime)
                {
                    payload = new JObject();
                    payload.Add("System.DateTime", (DateTime)data);
                }
                else if (data.GetType().IsValueType)
                {
                    payload = new JObject { { "data", data.ToString() } };
                }
                else
                {
                    payload = JObject.FromObject(data);
                }
            }

            //Debug.Assert(!string.IsNullOrEmpty(updatedMessage));
            //Debug.Assert(payload != null);

            InternalWrite(eventCache, source, eventType, id, updatedMessage, relatedActivityId, payload);
        }

        private async void InternalWrite(
            TraceEventCache eventCache,
            string source,
            TraceEventType eventType,
            int? traceId,
            string message,
            Guid?
            relatedActivityId,
            JObject dataObject)
        {

            var timeStamp = DateTime.UtcNow.ToString("o");
            //var source = Process.GetCurrentProcess().ProcessName;
            var stacktrace = Environment.StackTrace;
            var methodName = (new StackTrace()).GetFrame(StackTrace.METHODS_TO_SKIP + 4).GetMethod().Name;


            DateTime logTime;
            string logicalOperationStack = null;
            if (eventCache != null)
            {
                logTime = eventCache.DateTime.ToUniversalTime();
                if (eventCache.LogicalOperationStack != null && eventCache.LogicalOperationStack.Count > 0)
                {
                    StringBuilder stackBuilder = new StringBuilder();
                    foreach (object o in eventCache.LogicalOperationStack)
                    {
                        if (stackBuilder.Length > 0) stackBuilder.Append(", ");
                        stackBuilder.Append(o);
                    }
                    logicalOperationStack = stackBuilder.ToString();
                }
            }
            else
            {
                logTime = DateTimeOffset.UtcNow.UtcDateTime;
            }

            string threadId = eventCache != null ? eventCache.ThreadId : string.Empty;
            string thread = Thread.CurrentThread.Name ?? threadId;

            try
            {
                //await Client.Raw.IndexAsync(Index, "Trace",
                var jo = new JObject
                    {
                        {"Source", source },
                        {"TraceId", traceId ?? 0},
                        {"EventType", eventType.ToString()},
                        {"UtcDateTime", logTime},
                        {"timestamp", eventCache != null ? eventCache.Timestamp : 0},
                        {"MachineName", Environment.MachineName},
                        {"AppDomainFriendlyName", AppDomain.CurrentDomain.FriendlyName},
                        {"ProcessId", eventCache != null ? eventCache.ProcessId : 0},
                        {"ThreadName", thread},
                        {"ThreadId", threadId},
                        {"Message", message},
                        {"ActivityId", Trace.CorrelationManager.ActivityId != Guid.Empty ? Trace.CorrelationManager.ActivityId.ToString() : string.Empty},
                        {"RelatedActivityId", relatedActivityId.HasValue ? relatedActivityId.Value.ToString() : string.Empty},
                        {"LogicalOperationStack", logicalOperationStack},
                        {"Data", dataObject},
                    };
                //    .ToString());

                //var te = new TraceEntry
                //{
                //    Source = source,
                //    TraceId = traceId ?? 0,
                //    EventType = eventType.ToString(),
                //    UtcDateTime = logTime,
                //    timestamp = eventCache != null ? eventCache.Timestamp : 0,
                //    MachineName = Environment.MachineName,
                //    AppDomainFriendlyName = AppDomain.CurrentDomain.FriendlyName,
                //    ProcessId = eventCache != null ? eventCache.ProcessId : 0,
                //    ThreadName = thread,
                //    ThreadId = threadId,
                //    Message = message,
                //    ActivityId = Trace.CorrelationManager.ActivityId != Guid.Empty ? Trace.CorrelationManager.ActivityId.ToString() : string.Empty,
                //    RelatedActivityId = relatedActivityId.HasValue ? relatedActivityId.Value.ToString() : string.Empty,
                //    LogicalOperationStack = logicalOperationStack,
                //    Data = dataObject != null ? dataObject.ToString() : string.Empty
                //};

                ////WriteToQueueForprocessing(te);

                //WriteDirectlyToES(jo);
                _scribeProcessor(jo);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
            }
        }

        private async void WriteDirectlyToES(JObject jo)
        {
            //var payload = JObject.FromObject(te);


            var res = await Client.IndexAsync(Index, "Trace", jo.ToString());
            //var res = Client.Index(Index, "Trace", jo.ToString());
            //var res = await Client.Raw.IndexAsync<TraceEntry>(Index, "Trace", te);

            //resa.Wait();
            //var res = resa.Result;

            Debug.WriteLine("--------------------");
            Debug.WriteLine(res.ToString());
            //var res = await Client.IndexAsync(jo, i => i
            //.Index(Index)
            //.Type("trace"));

            //Debug.WriteLine(res);
        }

        private async void WriteDirectlyToESAsBatch(IEnumerable<JObject> jos)
        {

            var x = from o in jos
                    select
                    new
                    {
                        i = new { index = new { _index = Index, _type = "Trace" } },
                        trace = o
                    };

            var indx = new { index = new { _index = Index, _type = "Trace" } };
            var indxC = Enumerable.Repeat(indx, jos.Count());

            var bb = jos.Zip(indxC, (f,s)=> new object[] { s, f });
            var bbo = bb.SelectMany(a => a);

            //var bb = new object[jos.Count()*2] { };
            //foreach (var item in jos)
            //{
            //    bb.add
            //}

            //jos.First().

            //dynamic xmlContent = new ExpandoObject();
            //ExpandoObjectHelper.Parse(xmlContent, xmlDoc.Root);

            //string json = JsonConvert.SerializeObject(xmlContent);
            //payload = JObject.Parse(json);


            var bulk = new object[]
            {
    new { index = new { _index = Index, _type="Trace" }},
    new
    {
        name = "my object's name"
    },
    new { index = new { _index = Index, _type="Trace" }},
    new
    {
        name = "my object's name"
    },
    new { index = new { _index = Index, _type="Trace" }},
    new
    {
        name = "my object's name"
    },
    new { index = new { _index = Index, _type="Trace" }},
    new
    {
        name = "my object's name"
    },
            };



            try
            {
                var bulkXXXXX = new object[] { x.ToArray() };
                var res = Client.Bulk(Index, "Trace", bbo.ToArray());

                Debug.WriteLine("+++++++++++++++++++++++++++");
                Debug.WriteLine(res.ToString());
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                //throw;
            }


            //            var bulkX = new object[]
            //{
            //    new { index = new { _index = "test", _type="type", _id = "1"  }},
            //    new
            //    {
            //        name = "my object's name"
            //    }
            //};
            //            var bdesc = new BulkDescriptor();
            //            foreach (var jo in jos)
            //            {
            //                bdesc.Index<object>(f =>
            //                {
            //                    f.Document(new
            //                    {
            //                        name = "my object's name"
            //                    })
            //                });
            //            }


            //var x = Client.Bulk(
            //        var request = new BulkRequest()
            //        {
            //            Refresh = true,
            //            Consistency = Consistency.One,
            //            Operations = new List<IBulkOperation>
            //        {
            //            { new BulkIndexOperation< TraceEntry >(jos.First()) },
            //    //{ new BulkDeleteOperation< ElasticsearchProject> (6) },
            //    //{ new BulkCreateOperation< ElasticsearchProject > (project) { Id = "6" } },
            //    //{ new BulkUpdateOperation< ElasticsearchProject, object>(project, new { Name = "new- project2" }) { Id = "3" }
            //        },
            //        };


            //var response = Client.Bulk(request);

            //            var result = Client.Bulk(b => b
            //    .Index<TraceEntry>(i => i
            //        .Document(jos.First())
            //    )
            ////.Create<ElasticSearchProject>(c => c
            ////    .Document(new ElasticSearchProject { Id = 3 })
            ////)
            ////.Delete<ElasticSearchProject>(d => d
            ////    .Document(new ElasticSearchProject { Id = 4 })
            ////)
            //);


        }

        private async void WriteDirectlyToESAsBatchX(IEnumerable<JObject> jos)
        {
            //await Client.IndexManyAsync(jos, Index, "Trace");

            //var bdesc = new BulkDescriptor();
            //foreach (var jo in jos)
            //{
            //    bdesc.Index<TraceEntry>(i => i
            //    .Index(Index)
            //    .Type("Trace")
            //    //.Id(Guid.NewGuid().ToString())
            //    .Document(jo));

            //    //Client.Index(Index, "Trace", jo.ToString());
            //var ir = new IndexRequest<TraceEntry>(jo);
            //    var bb = Client.Serializer.Serialize(ir);
            //    //Client.Serializer.Deserialize<dynamic>()

            //}

            ////bdesc.IndexMany(jos, (f,g) => f.Index(Index).Type("Trace"));
            ////bdesc.
            //var x = Client.Bulk(bdesc);
            //Debug.Write(x.ToString());

        }

        private void WriteToQueueForprocessing(JObject jo)
        {
            this._queueToBePosted.Add(jo);
        }


        public override void Flush()
        {
            //check to make sure the "queue" has been emptied
            while (this._queueToBePosted.Count() > 0)
            { }
            base.Flush();
        }
    }
}
