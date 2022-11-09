using Elasticsearch.Net;
using Nest;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Security.Principal;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml.XPath;

namespace AM.Elasticsearch.TraceListener
{
    /// <summary>
    /// A TraceListener class used to submit trace data to elasticsearch
    /// </summary>
    public class ElasticSearchTraceListener : TraceListenerBase
    {
        private const string DocumentType = "doc";
        private const string _defaultIndexName = "trace";
        private const string _defaultUri = "http://localhost:9200/";

        private readonly BlockingCollection<TraceEntry> _queueToBePosted = new BlockingCollection<TraceEntry>();

        private ElasticClient _client;

        private readonly string _userDomainName;
        private readonly string _userName;

        /// <summary>
        /// Uri for the ElasticSearch server
        /// </summary>
        private Uri Uri { get; set; }

        /// <summary>
        /// prefix for the Index for traces
        /// </summary>
        private string Index => this.ElasticSearchTraceIndex.ToLower() + "-" + DateTime.UtcNow.ToString("yyyy-MM");


        private static readonly string[] _supportedAttributes = new string[]
        {
            "ElasticSearchUri", "elasticSearchUri", "elasticsearchuri",
            "ElasticSearchTraceIndex", "elasticSearchTraceIndex", "elasticsearchtraceindex","ElasticSearchTimeout","ElasticSearchBufferSize",

            //this attribute is to be removed next minor release
            "ElasticSearchIndex", "elasticSearchIndex", "elasticsearchindex",
            "elasticsearchusername","ElasticSearchUserName","elasticsearchpassword","ElasticSearchPassword"


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
                    return _defaultUri;
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
                    return "";
                }
            }
            set
            {
                Attributes["elasticsearchtraceindex"] = value;
            }
        }

        /// <summary>
        /// Username for basic authentication to elasticserver
        /// </summary>
        public string ElasticSearchUserName
        {
            get
            {
                if (Attributes.ContainsKey("elasticsearchusername"))
                {
                    return Attributes["elasticsearchusername"];
                }
                else
                {
                    return "";
                }
            }
            set
            {
                Attributes["elasticsearchusername"] = value;
            }
        }

        /// <summary>
        /// Password for basic authentication to elastiserver
        /// </summary>
        public string ElasticSearchPassword
        {
            get
            {
                if (Attributes.ContainsKey("elasticsearchpassword"))
                {
                    return Attributes["elasticsearchpassword"];
                }
                else
                {
                    return "";
                }
            }
            set
            {
                Attributes["elasticsearchpassword"] = value;
            }
        }

        public int ElasticSearchTimeout
        {
            get
            {
                if (Attributes.ContainsKey("elasticsearchtimeout"))
                {
                    return int.Parse(Attributes["elasticsearchtimeout"]);
                }
                else
                {
                    return 1;
                }
            }
            set
            {
                Attributes["elasticsearchtimeout"] = value.ToString();
            }
        }

        public int ElasticSearchBufferSize
        {
            get
            {
                if (Attributes.ContainsKey("elasticsearchbuffersize"))
                {
                    return int.Parse(Attributes["elasticsearchbuffersize"]);
                }
                else
                {
                    return 10;
                }
            }
            set
            {
                Attributes["elasticsearchbuffersize"] = value.ToString();
            }
        }
        
        public ElasticClient Client
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

					var pool = new SingleNodeConnectionPool(Uri);

                    var settings = new ConnectionSettings(pool)
                     .ServerCertificateValidationCallback((o, certificate, chain, errors) => true)
                    .EnableApiVersioningHeader()
                    .DefaultIndex(Index);
                    
                    if (this.ElasticSearchPassword.Length > 0)
                    {
                        settings.BasicAuthentication(this.ElasticSearchUserName, this.ElasticSearchPassword);
                    }
                    					
	                this._client = new ElasticClient(settings);
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
	        _userDomainName = Environment.UserDomainName;
	        _userName = Environment.UserName;
	        _machineName = Environment.MachineName;
            Initialize();
        }

        /// <summary>
        /// We cant grab any of the attributes until the class and more importantly its base class has finsihed initializing
        /// so keep the constructor at a minimum
        /// </summary>
        public ElasticSearchTraceListener(string name) : base(name)
        {
            _userDomainName = Environment.UserDomainName;
            _userName = Environment.UserName;
            _machineName = Environment.MachineName;
            Initialize();
        }

        public ElasticSearchTraceListener(string name,string uri,string user,string password,
                string indexname,int timouttoflush,int buffersize) 
            : base(name)
        {
            _userDomainName = Environment.UserDomainName;
            _userName = Environment.UserName;
            _machineName = Environment.MachineName;
            ElasticSearchPassword = password;
            ElasticSearchUserName = user;
            ElasticSearchUri = uri;
            ElasticSearchTraceIndex = indexname;
            ElasticSearchBufferSize = buffersize;
            ElasticSearchTimeout = timouttoflush;
            Initialize();
        }

        private void Initialize()
        {
            SetupObserverBatchy();
        }

        private Action<TraceEntry> _scribeProcessor;
        private string _machineName;

        
        private void SetupObserverBatchy()
        {
            _scribeProcessor = a => WriteToQueueForProcessing(a);

            this._queueToBePosted.GetConsumingEnumerable()
                .ToObservable(Scheduler.Default)
                .Buffer(TimeSpan.FromSeconds(ElasticSearchTimeout ), ElasticSearchBufferSize)
                .Subscribe(async x => await this.WriteDirectlyToESAsBatch(x));
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

            //if (eventCache != null && eventCache.Callstack.Contains(nameof(Elasticsearch.Net.ElasticLowLevelClient)))
            //{
            //    return;
            //}

            string updatedMessage = message;
            JObject payload = null;
            if (data != null)
            {
                try
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
                    else if (data is string)
                    {
                        payload = new JObject();
                        payload.Add("string", (string)data);
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
                catch (JsonSerializationException jEx)
                {
                    payload = new JObject();
                    payload.Add("FAILURE", jEx.ToString()); //using .ToString() instead of .Message will give you the stack dump too
                    payload.Add("datatype", data.GetType().ToString());
                    payload.Add("data", data.ToString());   //rather than just log the name of the Type, this will give us the stack dump if data was originally an exception, maybe some other useful info
                }
            }
            
            InternalWrite(eventCache, source, eventType, id, updatedMessage, relatedActivityId, payload);
        }

        private void InternalWrite(
            TraceEventCache eventCache,
            string source,
            TraceEventType eventType,
            int? traceId,
            string message,
            Guid?
            relatedActivityId,
            JObject dataObject)
        {

            //var timeStamp = DateTime.UtcNow.ToString("o");
            //var source = Process.GetCurrentProcess().ProcessName;
            //var stacktrace = Environment.StackTrace;
            //var methodName = (new StackTrace()).GetFrame(StackTrace.METHODS_TO_SKIP + 4).GetMethod().Name;


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

            IPrincipal principal = Thread.CurrentPrincipal;
            IIdentity identity = principal?.Identity;
            string identityname = identity == null ? string.Empty : identity.Name;            
            string username = $"{_userDomainName}\\{_userName}";

            try
            {
                var jo = new TraceEntry
                    {
                        Source= source,
                        TraceId= traceId ?? 0,
                        EventType= eventType.ToString(),
                        UtcDateTime=logTime,
                        timestamp= eventCache?.Timestamp ?? 0,
                        MachineName=_machineName,
                        AppDomainFriendlyName= AppDomain.CurrentDomain.FriendlyName,
                        ProcessId= eventCache?.ProcessId ?? 0,
                        ThreadName= thread,
                        ThreadId= threadId,
                        Message= message,
                        ActivityId= Trace.CorrelationManager.ActivityId != Guid.Empty ? Trace.CorrelationManager.ActivityId.ToString() : string.Empty,
                        RelatedActivityId= relatedActivityId.HasValue ? relatedActivityId.Value.ToString() : string.Empty,
                        LogicalOperationStack= logicalOperationStack,
                        Data= dataObject!=null ? dataObject.ToString():"",
                        Username=username,
                        Identityname= identityname
                    };

                _scribeProcessor(jo);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
            }
        }

        private async Task WriteDirectlyToES(TraceEntry jo)
        {
	        try
	        {
                await Client.IndexDocumentAsync( jo);
	        }
	        catch (Exception ex)
	        {
		        Debug.WriteLine(ex);
	        }
		}

        private async Task WriteDirectlyToESAsBatch(IEnumerable<TraceEntry> jos)
        {
            if (!jos.Any())
                return;

            
            try
            {
                var res=await Client.IndexManyAsync(jos);

            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
            }
        }

        private void WriteToQueueForProcessing(TraceEntry jo)
        {
            this._queueToBePosted.Add(jo);
        }


        /// <summary>
        /// removing the spin flush
        /// </summary>
        public override void Flush()
        {
            //check to make sure the "queue" has been emptied
            //while (this._queueToBePosted.Count() > 0)            { }
            base.Flush();
        }

        protected override void Dispose(bool disposing)
        {
            this._queueToBePosted.Dispose();
            base.Flush();
            base.Dispose(disposing);
        }

        new public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
