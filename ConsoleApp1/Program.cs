using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    internal class Program
    {
        static  void Main(string[] args)
        {

            System.Diagnostics.Trace.Listeners.Add(
                new AM.Elasticsearch.TraceListener.ElasticSearchTraceListener("EPCAPI",
                "https://localhost:9200", "elastic", "hJ3neUHqIeYwaQ6y1ndN","ciccio2"));

            System.Diagnostics.Trace.TraceError("ook");
            System.Diagnostics.Trace.TraceError("ook");
            System.Diagnostics.Trace.Flush();
            System.Diagnostics.Trace.TraceError("ook");
            System.Diagnostics.Trace.TraceError("ook");
            System.Diagnostics.Trace.TraceError("ook");

            try
            {
                Trace.Write("Entering");

                throw new Exception( "Not Found!");


            }
            catch (Exception ex)
            {
                Trace.TraceError ( ex.ToString());
                //throw;
            }

            Task.Delay(200000).Wait();

        }
    }
}
