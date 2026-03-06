using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Text;

class QueryTables
{
    static void Main(string[] args)
    {
        string configPath = args.Length > 0 ? args[0] : Path.Combine(Environment.CurrentDirectory, "..", "_config.json");
        string configText = File.ReadAllText(configPath);

        string server = ExtractJsonValue(configText, "db_server");
        string uid = ExtractJsonValue(configText, "db_user");
        string pw = ExtractJsonValue(configText, "db_pw");

        string connStr = string.Format(
            "Data Source={0};Initial Catalog=VARIAN;Integrated Security=False;User Id={1};Password={2};MultipleActiveResultSets=True;Connection Timeout=10",
            server, uid, pw);

        Console.Error.WriteLine("Connecting to {0} as {1}...", server, uid);

        using (var con = new SqlConnection(connStr))
        {
            con.Open();
            Console.Error.WriteLine("Connected.");

            // 1. Get all user tables
            var tables = new List<string>();
            using (var cmd = new SqlCommand(
                "SELECT t.name FROM sys.tables t WHERE t.type = 'U' ORDER BY t.name", con))
            using (var dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                    tables.Add(dr.GetString(0));
            }
            Console.Error.WriteLine("Found {0} tables.", tables.Count);

            // 2. Get all columns
            var columns = new Dictionary<string, List<string[]>>();
            using (var cmd = new SqlCommand(
                @"SELECT t.name, c.name, tp.name, c.max_length, c.is_nullable, c.precision, c.scale, c.is_identity
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
                    bool nullable = dr.GetBoolean(4);
                    byte prec = dr.GetByte(5);
                    byte scale = dr.GetByte(6);
                    bool identity = dr.GetBoolean(7);

                    string fullType = FormatType(dataType, maxLen, prec, scale);

                    if (!columns.ContainsKey(tbl))
                        columns[tbl] = new List<string[]>();
                    columns[tbl].Add(new string[] {
                        colName, fullType,
                        nullable ? "true" : "false",
                        identity ? "true" : "false"
                    });
                }
            }

            // 3. Get primary keys
            var primaryKeys = new Dictionary<string, List<string>>();
            using (var cmd = new SqlCommand(
                @"SELECT t.name, c.name
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

            // 4. Get foreign keys
            var foreignKeys = new Dictionary<string, List<string[]>>();
            using (var cmd = new SqlCommand(
                @"SELECT tp.name, cp.name, tr.name, cr.name
                  FROM sys.foreign_keys fk
                  JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                  JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
                  JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
                  JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
                  JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
                  ORDER BY tp.name, fkc.constraint_column_id", con))
            using (var dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    string tbl = dr.GetString(0);
                    string col = dr.GetString(1);
                    string refTbl = dr.GetString(2);
                    string refCol = dr.GetString(3);
                    if (!foreignKeys.ContainsKey(tbl))
                        foreignKeys[tbl] = new List<string[]>();
                    foreignKeys[tbl].Add(new string[] { col, refTbl, refCol });
                }
            }

            // Build JSON
            var sb = new StringBuilder();
            sb.AppendLine("{");
            sb.AppendLine("  \"database\": \"VARIAN\",");
            sb.AppendLine("  \"server\": " + J(server) + ",");
            sb.AppendLine("  \"generated\": " + J(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")) + ",");
            sb.AppendLine("  \"tableCount\": " + tables.Count + ",");
            sb.AppendLine("  \"tables\": {");

            for (int t = 0; t < tables.Count; t++)
            {
                string tbl = tables[t];
                sb.AppendLine("    " + J(tbl) + ": {");

                // primary key
                sb.Append("      \"primaryKey\": [");
                if (primaryKeys.ContainsKey(tbl))
                {
                    var pks = primaryKeys[tbl];
                    for (int i = 0; i < pks.Count; i++)
                    {
                        if (i > 0) sb.Append(", ");
                        sb.Append(J(pks[i]));
                    }
                }
                sb.AppendLine("],");

                // foreign keys
                sb.Append("      \"foreignKeys\": [");
                if (foreignKeys.ContainsKey(tbl))
                {
                    var fks = foreignKeys[tbl];
                    for (int i = 0; i < fks.Count; i++)
                    {
                        if (i > 0) sb.Append(", ");
                        sb.Append("{ \"column\": " + J(fks[i][0])
                            + ", \"references\": " + J(fks[i][1] + "." + fks[i][2]) + " }");
                    }
                }
                sb.AppendLine("],");

                // columns
                sb.AppendLine("      \"columns\": [");
                if (columns.ContainsKey(tbl))
                {
                    var cols = columns[tbl];
                    for (int c = 0; c < cols.Count; c++)
                    {
                        sb.Append("        { \"name\": " + J(cols[c][0])
                            + ", \"type\": " + J(cols[c][1])
                            + ", \"nullable\": " + cols[c][2]);
                        if (cols[c][3] == "true")
                            sb.Append(", \"identity\": true");
                        sb.Append(" }");
                        if (c < cols.Count - 1) sb.Append(",");
                        sb.AppendLine();
                    }
                }
                sb.AppendLine("      ]");

                sb.Append("    }");
                if (t < tables.Count - 1) sb.Append(",");
                sb.AppendLine();
            }

            sb.AppendLine("  }");
            sb.AppendLine("}");

            string outputPath = args.Length > 1 ? args[1] : "tables.json";
            File.WriteAllText(outputPath, sb.ToString(), Encoding.UTF8);
            Console.Error.WriteLine("Written to {0}", outputPath);
        }
    }

    static string FormatType(string dataType, int maxLen, byte prec, byte scale)
    {
        if (dataType == "nvarchar" || dataType == "nchar")
            return maxLen == -1 ? dataType + "(max)" : dataType + "(" + (maxLen / 2) + ")";
        if (dataType == "varchar" || dataType == "char" || dataType == "varbinary" || dataType == "binary")
            return maxLen == -1 ? dataType + "(max)" : dataType + "(" + maxLen + ")";
        if (dataType == "decimal" || dataType == "numeric")
            return dataType + "(" + prec + "," + scale + ")";
        return dataType;
    }

    static string J(string s)
    {
        return "\"" + s.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
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
