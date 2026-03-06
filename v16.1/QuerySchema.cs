using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Text;

class QuerySchema
{
    static string dbserver;

    static void Main(string[] args)
    {
        // Read config
        string configPath = args.Length > 0 ? args[0] : "_config.json";
        if (!File.Exists(configPath))
        {
            configPath = Path.Combine(Environment.CurrentDirectory, "..", "_config.json");
        }

        string configText = File.ReadAllText(configPath);
        dbserver = ExtractJsonValue(configText, "db_server");

        Console.Error.WriteLine("Server: " + dbserver);

        // First, list all databases
        string masterConn = string.Format(
            "Data Source={0};Initial Catalog=master;Integrated Security=True;MultipleActiveResultSets=True;Connection Timeout=10",
            dbserver);

        Console.Error.WriteLine("--- Listing databases ---");
        using (var con = new SqlConnection(masterConn))
        {
            con.Open();
            using (var cmd = new SqlCommand("SELECT name FROM sys.databases ORDER BY name", con))
            using (var dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                    Console.Error.WriteLine("  DB: " + dr.GetString(0));
            }
        }

        // Query VARIAN database using Windows Authentication
        string connStr = string.Format(
            "Data Source={0};Initial Catalog=VARIAN;Integrated Security=True;MultipleActiveResultSets=True;Connection Timeout=10",
            dbserver);

        var sb = new StringBuilder();
        sb.AppendLine("{");
        sb.AppendLine("  \"metadata\": {");
        sb.AppendLine("    \"database\": \"VARIAN\",");
        sb.AppendLine("    \"version\": \"16.1\",");
        sb.AppendLine("    \"generated\": \"" + DateTime.Now.ToString("yyyy-MM-dd") + "\"");
        sb.AppendLine("  },");

        using (var con = new SqlConnection(connStr))
        {
            con.Open();
            Console.Error.WriteLine("Connected to VARIAN database.");

            // Debug: check current user and permissions
            using (var cmd = new SqlCommand("SELECT SYSTEM_USER, USER_NAME(), DB_NAME()", con))
            using (var dr = cmd.ExecuteReader())
            {
                if (dr.Read())
                    Console.Error.WriteLine("Login: " + dr[0] + ", User: " + dr[1] + ", DB: " + dr[2]);
            }

            // 1. Get all user tables using sys.tables
            var tables = new List<string>();
            using (var cmd = new SqlCommand(
                "SELECT t.name FROM sys.tables t WHERE t.type = 'U' ORDER BY t.name", con))
            using (var dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                    tables.Add(dr.GetString(0));
            }
            Console.Error.WriteLine("Found " + tables.Count + " tables (sys.tables).");

            // If still 0, try sys.objects
            if (tables.Count == 0)
            {
                using (var cmd = new SqlCommand(
                    "SELECT name FROM sys.objects WHERE type='U' ORDER BY name", con))
                using (var dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                        tables.Add(dr.GetString(0));
                }
                Console.Error.WriteLine("Found " + tables.Count + " tables (sys.objects).");
            }

            // If still 0, try querying sysobjects (legacy)
            if (tables.Count == 0)
            {
                using (var cmd = new SqlCommand(
                    "SELECT name FROM sysobjects WHERE xtype='U' ORDER BY name", con))
                using (var dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                        tables.Add(dr.GetString(0));
                }
                Console.Error.WriteLine("Found " + tables.Count + " tables (sysobjects).");
            }

            // 2. Get all columns using sys.columns + sys.types
            var columns = new Dictionary<string, List<string[]>>();
            using (var cmd = new SqlCommand(
                @"SELECT t.name AS table_name,
                         c.name AS column_name,
                         tp.name AS type_name,
                         c.max_length,
                         c.is_nullable,
                         c.precision,
                         c.scale,
                         c.is_identity,
                         c.column_id
                  FROM sys.columns c
                  JOIN sys.tables t ON c.object_id = t.object_id
                  JOIN sys.types tp ON c.user_type_id = tp.user_type_id
                  WHERE t.type = 'U'
                  ORDER BY t.name, c.column_id", con))
            using (var dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    string tbl = dr.GetString(0);
                    string colName = dr.GetString(1);
                    string dataType = dr.GetString(2);
                    int maxLen = dr.GetInt16(3);
                    bool isNullable = dr.GetBoolean(4);
                    byte prec = dr.GetByte(5);
                    byte scale = dr.GetByte(6);
                    bool isIdentity = dr.GetBoolean(7);

                    // Build type string
                    string fullType = dataType;
                    if (dataType == "nvarchar" || dataType == "nchar")
                    {
                        if (maxLen == -1) fullType = dataType + "(max)";
                        else fullType = dataType + "(" + (maxLen / 2) + ")";
                    }
                    else if (dataType == "varchar" || dataType == "char" || dataType == "varbinary" || dataType == "binary")
                    {
                        if (maxLen == -1) fullType = dataType + "(max)";
                        else fullType = dataType + "(" + maxLen + ")";
                    }
                    else if (dataType == "decimal" || dataType == "numeric")
                    {
                        fullType = dataType + "(" + prec + "," + scale + ")";
                    }

                    if (!columns.ContainsKey(tbl))
                        columns[tbl] = new List<string[]>();
                    columns[tbl].Add(new string[] {
                        colName, fullType,
                        isNullable ? "YES" : "NO",
                        isIdentity ? "1" : "0"
                    });
                }
            }
            Console.Error.WriteLine("Loaded columns for " + columns.Count + " tables.");

            // 3. Get primary keys using sys views
            var primaryKeys = new Dictionary<string, List<string>>();
            using (var cmd = new SqlCommand(
                @"SELECT t.name AS table_name, c.name AS column_name
                  FROM sys.indexes i
                  JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                  JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                  JOIN sys.tables t ON i.object_id = t.object_id
                  WHERE i.is_primary_key = 1
                  ORDER BY t.name, ic.key_ordinal", con))
            using (var dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    string tbl = dr.GetString(0);
                    string col = dr.GetString(1);
                    if (!primaryKeys.ContainsKey(tbl))
                        primaryKeys[tbl] = new List<string>();
                    primaryKeys[tbl].Add(col);
                }
            }
            Console.Error.WriteLine("Found primary keys for " + primaryKeys.Count + " tables.");

            // 4. Get foreign keys
            var foreignKeys = new List<string[]>();
            using (var cmd = new SqlCommand(
                @"SELECT
                    fk.name AS FK_NAME,
                    tp.name AS PARENT_TABLE,
                    cp.name AS PARENT_COLUMN,
                    tr.name AS REFERENCED_TABLE,
                    cr.name AS REFERENCED_COLUMN
                  FROM sys.foreign_keys fk
                  JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                  JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
                  JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
                  JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
                  JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
                  ORDER BY tp.name, fk.name, fkc.constraint_column_id", con))
            using (var dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    foreignKeys.Add(new string[] {
                        dr.GetString(0),
                        dr.GetString(1),
                        dr.GetString(2),
                        dr.GetString(3),
                        dr.GetString(4)
                    });
                }
            }
            Console.Error.WriteLine("Found " + foreignKeys.Count + " foreign key columns.");

            // 5. Get unique constraints
            var uniqueKeys = new Dictionary<string, Dictionary<string, List<string>>>();
            using (var cmd = new SqlCommand(
                @"SELECT t.name AS table_name, i.name AS index_name, c.name AS column_name
                  FROM sys.indexes i
                  JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                  JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                  JOIN sys.tables t ON i.object_id = t.object_id
                  WHERE i.is_unique_constraint = 1
                  ORDER BY t.name, i.name, ic.key_ordinal", con))
            using (var dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    string tbl = dr.GetString(0);
                    string idxName = dr.GetString(1);
                    string col = dr.GetString(2);
                    if (!uniqueKeys.ContainsKey(tbl))
                        uniqueKeys[tbl] = new Dictionary<string, List<string>>();
                    if (!uniqueKeys[tbl].ContainsKey(idxName))
                        uniqueKeys[tbl][idxName] = new List<string>();
                    uniqueKeys[tbl][idxName].Add(col);
                }
            }

            // Build JSON: entities
            sb.AppendLine("  \"entities\": {");
            for (int t = 0; t < tables.Count; t++)
            {
                string tbl = tables[t];
                sb.AppendLine("    " + JsonStr(tbl) + ": {");

                // columns
                sb.AppendLine("      \"columns\": [");
                if (columns.ContainsKey(tbl))
                {
                    var cols = columns[tbl];
                    for (int c = 0; c < cols.Count; c++)
                    {
                        var col = cols[c];
                        sb.Append("        { \"name\": " + JsonStr(col[0])
                            + ", \"type\": " + JsonStr(col[1])
                            + ", \"nullable\": " + (col[2] == "YES" ? "true" : "false"));
                        if (col[3] == "1")
                            sb.Append(", \"identity\": true");
                        sb.Append(" }");
                        if (c < cols.Count - 1) sb.Append(",");
                        sb.AppendLine();
                    }
                }
                sb.AppendLine("      ],");

                // primary key
                sb.Append("      \"primaryKey\": [");
                if (primaryKeys.ContainsKey(tbl))
                {
                    var pks = primaryKeys[tbl];
                    for (int p = 0; p < pks.Count; p++)
                    {
                        sb.Append(JsonStr(pks[p]));
                        if (p < pks.Count - 1) sb.Append(", ");
                    }
                }
                sb.AppendLine("],");

                // unique constraints
                sb.Append("      \"uniqueConstraints\": [");
                if (uniqueKeys.ContainsKey(tbl))
                {
                    var uks = uniqueKeys[tbl];
                    int ui = 0;
                    foreach (var uk in uks)
                    {
                        sb.Append("[");
                        for (int u = 0; u < uk.Value.Count; u++)
                        {
                            sb.Append(JsonStr(uk.Value[u]));
                            if (u < uk.Value.Count - 1) sb.Append(", ");
                        }
                        sb.Append("]");
                        ui++;
                        if (ui < uks.Count) sb.Append(", ");
                    }
                }
                sb.AppendLine("]");

                sb.Append("    }");
                if (t < tables.Count - 1) sb.Append(",");
                sb.AppendLine();
            }
            sb.AppendLine("  },");

            // Build JSON: relationships
            sb.AppendLine("  \"relationships\": [");
            var fkGroups = new Dictionary<string, List<string[]>>();
            foreach (var fk in foreignKeys)
            {
                if (!fkGroups.ContainsKey(fk[0]))
                    fkGroups[fk[0]] = new List<string[]>();
                fkGroups[fk[0]].Add(fk);
            }
            int fi = 0;
            foreach (var fkg in fkGroups)
            {
                var first = fkg.Value[0];
                sb.AppendLine("    {");
                sb.AppendLine("      \"name\": " + JsonStr(fkg.Key) + ",");
                sb.AppendLine("      \"fromTable\": " + JsonStr(first[1]) + ",");
                sb.Append("      \"fromColumns\": [");
                for (int c = 0; c < fkg.Value.Count; c++)
                {
                    sb.Append(JsonStr(fkg.Value[c][2]));
                    if (c < fkg.Value.Count - 1) sb.Append(", ");
                }
                sb.AppendLine("],");
                sb.AppendLine("      \"toTable\": " + JsonStr(first[3]) + ",");
                sb.Append("      \"toColumns\": [");
                for (int c = 0; c < fkg.Value.Count; c++)
                {
                    sb.Append(JsonStr(fkg.Value[c][4]));
                    if (c < fkg.Value.Count - 1) sb.Append(", ");
                }
                sb.AppendLine("]");
                sb.Append("    }");
                fi++;
                if (fi < fkGroups.Count) sb.Append(",");
                sb.AppendLine();
            }
            sb.AppendLine("  ]");
        }

        sb.AppendLine("}");

        // Write output
        string outputPath = args.Length > 1 ? args[1] : "entity-relation.json";
        File.WriteAllText(outputPath, sb.ToString(), Encoding.UTF8);
        Console.Error.WriteLine("Written to: " + outputPath);
        Console.WriteLine(outputPath);
    }

    static string JsonStr(string s)
    {
        return "\"" + s.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n").Replace("\r", "\\r") + "\"";
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
