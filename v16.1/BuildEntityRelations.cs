using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

class BuildEntityRelations
{
    static void Main(string[] args)
    {
        string dataDir = args.Length > 0 ? args[0] : "data";
        string tablesPath = Path.Combine(dataDir, "tables.json");
        string outputPath = args.Length > 1 ? args[1] : Path.Combine(dataDir, "entity-relations.json");

        Console.Error.WriteLine("Reading {0}...", tablesPath);
        var root = JObject.Parse(File.ReadAllText(tablesPath));
        var tables = (JObject)root["tables"];

        // Build entity list and relationship list
        var entities = new JObject();
        var relationships = new JArray();

        // Track relationship names for uniqueness
        var relSet = new HashSet<string>();

        foreach (var prop in tables.Properties())
        {
            string tableName = prop.Name;
            var tbl = (JObject)prop.Value;

            // Build entity
            var entity = new JObject();

            // Primary key
            entity["primaryKey"] = tbl["primaryKey"];

            // Columns: just name and type
            var cols = new JArray();
            foreach (var c in (JArray)tbl["columns"])
            {
                var col = new JObject();
                col["name"] = c["name"];
                col["type"] = c["type"];
                col["nullable"] = c["nullable"];
                if (c["identity"] != null && (bool)c["identity"])
                    col["identity"] = true;
                cols.Add(col);
            }
            entity["columns"] = cols;

            entities[tableName] = entity;

            // Build relationships from foreign keys
            var fks = (JArray)tbl["foreignKeys"];
            if (fks != null)
            {
                foreach (var fk in fks)
                {
                    string fromCol = (string)fk["column"];
                    string refStr = (string)fk["references"];
                    string[] parts = refStr.Split('.');
                    string toTable = parts[0];
                    string toCol = parts[1];

                    // Deduplicate
                    string key = tableName + "." + fromCol + "->" + toTable + "." + toCol;
                    if (relSet.Contains(key))
                        continue;
                    relSet.Add(key);

                    var rel = new JObject();
                    rel["fromTable"] = tableName;
                    rel["fromColumn"] = fromCol;
                    rel["toTable"] = toTable;
                    rel["toColumn"] = toCol;

                    // Determine cardinality:
                    // FK column in the source table pointing to PK in target = many-to-one
                    rel["type"] = "many-to-one";

                    relationships.Add(rel);
                }
            }
        }

        Console.Error.WriteLine("Entities: {0}, Relationships: {1}", entities.Count, relationships.Count);

        // Build output
        var output = new JObject();
        var metadata = new JObject();
        metadata["database"] = (string)root["database"];
        metadata["server"] = (string)root["server"];
        metadata["version"] = "16.1";
        metadata["generated"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        metadata["entityCount"] = entities.Count;
        metadata["relationshipCount"] = relationships.Count;
        output["metadata"] = metadata;
        output["entities"] = entities;
        output["relationships"] = relationships;

        File.WriteAllText(outputPath, output.ToString(Formatting.Indented));
        Console.Error.WriteLine("Written to {0}", outputPath);
    }
}
