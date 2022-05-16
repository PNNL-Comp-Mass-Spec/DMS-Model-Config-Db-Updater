using System;
using System.Data;
using System.Data.SQLite;
using System.Globalization;

namespace DMSModelConfigDbUpdater
{
    internal class SQLiteUtilities
    {
        private static readonly CultureInfo mCultureInfoUS = new("en-US");

        /// <summary>
        /// Convert object read from database to boolean
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldName"></param>
        protected internal static bool GetBoolean(IDataRecord reader, string fieldName)
        {
            return Convert.ToBoolean(reader[fieldName], mCultureInfoUS);
        }

        /// <summary>
        /// Convert object read from database to double
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldName"></param>
        protected internal static double GetDouble(IDataRecord reader, string fieldName)
        {
            return Convert.ToDouble(reader[fieldName], mCultureInfoUS);
        }

        /// <summary>
        /// Convert object read from database to int16
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldName"></param>
        protected internal static short GetInt16(IDataRecord reader, string fieldName)
        {
            return Convert.ToInt16(reader[fieldName], mCultureInfoUS);
        }

        /// <summary>
        /// Convert object read from database to int32
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldName"></param>
        protected internal static int GetInt32(IDataRecord reader, string fieldName)
        {
            return Convert.ToInt32(reader[fieldName], mCultureInfoUS);
        }

        /// <summary>
        /// Convert object read from database to single/float
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldName"></param>
        protected internal static float GetSingle(IDataRecord reader, string fieldName)
        {
            return Convert.ToSingle(reader[fieldName], mCultureInfoUS);
        }

        /// <summary>
        /// Convert object read from database to string
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldName"></param>
        protected internal static string GetString(IDataRecord reader, string fieldName)
        {
            return Convert.ToString(reader[fieldName], mCultureInfoUS);
        }

        /// <summary>
        /// Check whether a table exists
        /// </summary>
        /// <param name="dbConnection"></param>
        /// <param name="tableName"></param>
        /// <returns>True if the table or view exists</returns>
        public static bool TableExists(SQLiteConnection dbConnection, string tableName)
        {
            using var cmd = new SQLiteCommand(dbConnection)
            {
                CommandText = "SELECT name " +
                              "FROM sqlite_master " +
                              "WHERE type IN ('table','view') And tbl_name = '" + tableName + "'"
            };

            using var reader = cmd.ExecuteReader();
            return reader.HasRows;
        }
    }
}
