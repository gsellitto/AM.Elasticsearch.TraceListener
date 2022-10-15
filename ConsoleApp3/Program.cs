using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp3
{
    internal class Program
    {
        static void Main(string[] args)
        {
            System.Diagnostics.Trace.Listeners.Add(
                new AM.Elasticsearch.TraceListener.ElasticSearchTraceListener("EPCAPI",
                "https://localhost:9200", "elastic", "hJ3neUHqIeYwaQ6y1ndN", "ciccio2"));

           
            var c = new ciccio();

            Trace.Write(c);
            try
            {
                Trace.Write("Entering");

                throw new Exception("Not Found!");


            }
            catch (Exception ex)
            {
                Trace.TraceError(ex.ToString());
                //throw;
            }

            Task.Delay(200000).Wait();
        }
    }
}

public class ciccio {

    string uno = "unoval";
    string due = "tre";
}
