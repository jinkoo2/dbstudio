using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Text;

class QueryColumns
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

            var sb = new StringBuilder();
            int count = 0;

            sb.AppendLine("{");
            sb.AppendLine("  \"database\": \"VARIAN\",");
            sb.AppendLine("  \"server\": " + J(server) + ",");
            sb.AppendLine("  \"generated\": " + J(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")) + ",");

            // Query all columns with table name, PK/FK info
            string sql = @"
                SELECT
                    t.name AS table_name,
                    c.name AS column_name,
                    tp.name AS type_name,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.is_nullable,
                    c.is_identity,
                    c.column_id,
                    CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key,
                    fk_ref.referenced_table,
                    fk_ref.referenced_column
                FROM sys.columns c
                JOIN sys.tables t ON c.object_id = t.object_id AND t.type = 'U'
                JOIN sys.types tp ON c.user_type_id = tp.user_type_id
                LEFT JOIN (
                    SELECT ic.object_id, ic.column_id
                    FROM sys.index_columns ic
                    JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                    WHERE i.is_primary_key = 1
                ) pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id
                LEFT JOIN (
                    SELECT
                        fkc.parent_object_id,
                        fkc.parent_column_id,
                        tr.name AS referenced_table,
                        cr.name AS referenced_column
                    FROM sys.foreign_key_columns fkc
                    JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
                    JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
                ) fk_ref ON fk_ref.parent_object_id = c.object_id AND fk_ref.parent_column_id = c.column_id
                ORDER BY t.name, c.column_id";

            using (var cmd = new SqlCommand(sql, con))
            using (var dr = cmd.ExecuteReader())
            {
                sb.Append("  \"columns\": [");
                sb.AppendLine();
                bool first = true;

                while (dr.Read())
                {
                    string tbl = dr.GetString(0);
                    string col = dr.GetString(1);
                    string typeName = dr.GetString(2);
                    int maxLen = dr.GetInt16(3);
                    byte prec = dr.GetByte(4);
                    byte scale = dr.GetByte(5);
                    bool nullable = dr.GetBoolean(6);
                    bool identity = dr.GetBoolean(7);
                    int colId = dr.GetInt32(8);
                    bool isPK = dr.GetInt32(9) == 1;
                    string refTable = dr.IsDBNull(10) ? null : dr.GetString(10);
                    string refCol = dr.IsDBNull(11) ? null : dr.GetString(11);

                    string fullType = FormatType(typeName, maxLen, prec, scale);

                    if (!first) sb.AppendLine(",");
                    first = false;

                    sb.Append("    { ");
                    sb.Append("\"table\": " + J(tbl));
                    sb.Append(", \"column\": " + J(col));
                    sb.Append(", \"ordinal\": " + colId);
                    sb.Append(", \"type\": " + J(fullType));
                    sb.Append(", \"nullable\": " + (nullable ? "true" : "false"));
                    if (identity) sb.Append(", \"identity\": true");
                    if (isPK) sb.Append(", \"primaryKey\": true");
                    if (refTable != null)
                        sb.Append(", \"foreignKey\": { \"table\": " + J(refTable) + ", \"column\": " + J(refCol) + " }");
                    sb.Append(" }");
                    count++;
                }
                sb.AppendLine();
                sb.AppendLine("  ],");
            }

            sb.AppendLine("  \"columnCount\": " + count);
            sb.AppendLine("}");

            Console.Error.WriteLine("Found {0} columns.", count);

            string outputPath = args.Length > 1 ? args[1] : "columns.json";
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
