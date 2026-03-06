using System;
using System.Data.SqlClient;
using System.IO;

class QueryPatients
{
    static void Main(string[] args)
    {
        string configPath = args.Length > 0 ? args[0] : Path.Combine(Environment.CurrentDirectory, "..", "_config.json");
        string configText = File.ReadAllText(configPath);

        string server = ExtractJsonValue(configText, "db_server");
        string uid = ExtractJsonValue(configText, "db_user");
        string pw = ExtractJsonValue(configText, "db_pw");

        string connStr = string.Format(
            "Data Source={0};Initial Catalog=VARIAN;Integrated Security=False;User Id={1};Password={2};Connection Timeout=10",
            server, uid, pw);

        Console.WriteLine("Connecting to {0} as {1}...", server, uid);

        using (var con = new SqlConnection(connStr))
        {
            con.Open();
            Console.WriteLine("Connected.\n");

            string sql = "SELECT TOP 10 PatientSer, PatientId, LastName, FirstName FROM Patient ORDER BY PatientSer";
            using (var cmd = new SqlCommand(sql, con))
            using (var dr = cmd.ExecuteReader())
            {
                Console.WriteLine("{0,-12} {1,-15} {2,-20} {3,-20}", "PatientSer", "PatientId", "LastName", "FirstName");
                Console.WriteLine(new string('-', 70));
                while (dr.Read())
                {
                    Console.WriteLine("{0,-12} {1,-15} {2,-20} {3,-20}",
                        dr["PatientSer"],
                        dr.IsDBNull(1) ? "" : dr["PatientId"],
                        dr.IsDBNull(2) ? "" : dr["LastName"],
                        dr.IsDBNull(3) ? "" : dr["FirstName"]);
                }
            }
        }
    }

    static string ExtractJsonValue(string json, string key)
    {
        int idx = json.IndexOf("\"" + key + "\"");
        if (idx < 0) return "";
        idx = json.IndexOf(":", idx);
        if (idx < 0) return "";
        idx = json.IndexOf("\"", idx);
        if (idx < 0) return "";
        int end = json.IndexOf("\"", idx + 1);
        return json.Substring(idx + 1, end - idx - 1);
    }
}
