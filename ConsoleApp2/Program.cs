using Elasticsearch.Net;
using Nest;
using Newtonsoft.Json.Linq;
using System.Diagnostics;

System.Diagnostics.Trace.Listeners.Add(
                new AM.Elasticsearch.TraceListener.ElasticSearchTraceListener("EPCAPI",
                "https://localhost:9200", "elastic", "hJ3neUHqIeYwaQ6y1ndN", "new2"));


var c = new ciccio();

JObject o = (JObject)JToken.FromObject(c);

Trace.Write(c);

try
{
    int cc = 0;
    int i = 10 / cc;
}
catch (Exception ex)
{
    Trace.TraceError("Errore in ciccio");
    Trace.Write(ex);
}

Task.Delay(200000).Wait();
void test()
{
    var pool = new SingleNodeConnectionPool(new Uri("https://localhost:9200"));

    //.CertificateFingerprint("94:75:CE:4F:EB:05:32:83:40:B8:18:BB:79:01:7B:E0:F0:B6:C3:01:57:DB:4D:F5:D8:B8:A6:BA:BD:6D:C5:C4")

    var settings = new ConnectionSettings(pool)
        .BasicAuthentication("elastic", "hJ3neUHqIeYwaQ6y1ndN")
        .ServerCertificateValidationCallback((o, certificate, chain, errors) => true)
        .EnableApiVersioningHeader()
        .DefaultIndex("my-index");

    var client = new ElasticClient(settings);

    var document = new MyDocument
    {
        Id = 1,
        Name = "My first document",
        OwnerId = 2,
        SubDocuments = new[]
        {
        new MySubDocument { Name = "my first sub document" },
        new MySubDocument { Name = "my second sub document" },
    }
    };

    try
    {
        client.IndexDocument(document);
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex.Message);
    }
}


public class ciccio
{

   public string uno = "unoval";
    public string due = "tre";
}

public class MyDocument
{
    public int Id { get; set; }

    public string Name { get; set; }

    public string FilePath { get; set; }

    public int OwnerId { get; set; }

    public IEnumerable<MySubDocument> SubDocuments { get; set; }
}

public class MySubDocument
{
    public string Name { get; set; }
}


