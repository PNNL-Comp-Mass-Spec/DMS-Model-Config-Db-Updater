using PRISM;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DMSModelConfigDbUpdater
{
    /// <summary>
    /// This class tracks the argument names in database stored procedures and functions
    /// </summary>
    internal class DatabaseFunctionAndProcedureInfo : EventNotifier
    {
        /// <summary>
        /// Database name
        /// </summary>
        public string DatabaseName { get; }

        /// <summary>
        /// Keys in this dictionary are schema names
        /// Values are a dictionary where keys are procedure or function name and values are instances of FunctionOrProcedureInfo
        /// </summary>
        public Dictionary<string, Dictionary<string, FunctionOrProcedureInfo>> FunctionsAndProceduresBySchema { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="databaseName"></param>
        public DatabaseFunctionAndProcedureInfo(string databaseName)
        {
            DatabaseName = databaseName;

            FunctionsAndProceduresBySchema = new Dictionary<string, Dictionary<string, FunctionOrProcedureInfo>>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Add database function or procedure
        /// </summary>
        /// <param name="schemaName"></param>
        /// <param name="objectName"></param>
        /// <param name="objectInfo"></param>
        public void AddFunctionOrProcedure(string schemaName, string objectName, FunctionOrProcedureInfo objectInfo)
        {
            if (FunctionsAndProceduresBySchema.TryGetValue(schemaName, out var functionsAndProcedures))
            {
                functionsAndProcedures.Add(objectName, objectInfo);
                return;
            }

            FunctionsAndProceduresBySchema.Add(schemaName, new Dictionary<string, FunctionOrProcedureInfo>(StringComparer.OrdinalIgnoreCase)
            {
                { objectName, objectInfo }
            });
        }

        /// <summary>
        /// Get argument names for the function or procedure
        /// </summary>
        /// <remarks>First looks for a match using the schema; if no match, looks for the first match in any schema</remarks>
        /// <param name="schemaName"></param>
        /// <param name="functionOrProcedureName"></param>
        /// <returns>Argument list</returns>
        public FunctionOrProcedureInfo GetArgumentListForFunctionOrProcedure(string schemaName, string functionOrProcedureName)
        {
            // First try to match by schema name and object name
            if (!string.IsNullOrWhiteSpace(schemaName) &&
                FunctionsAndProceduresBySchema.TryGetValue(schemaName, out var functionsAndProceduresForSchema) &&
                functionsAndProceduresForSchema.TryGetValue(functionOrProcedureName, out var objectInfoExactMatch))
            {
                return objectInfoExactMatch;
            }

            foreach (var schemaItem in FunctionsAndProceduresBySchema)
            {
                if (!schemaItem.Value.TryGetValue(functionOrProcedureName, out var objectInfo))
                    continue;

                OnWarningEvent("Function or procedure {0} was not found in schema {1}, but was found in schema {2}", functionOrProcedureName, schemaName, schemaItem.Key);

                return objectInfo;
            }

            // Function or procedure not found in any schema
            return new FunctionOrProcedureInfo(string.Empty, string.Empty, false);
        }
    }
}
