namespace DMSModelConfigDbUpdater
{
    internal class ChooserDefinition
    {
        // Ignore Spelling: sql

        /// <summary>
        /// Row ID
        /// </summary>
        public int ID { get; }

        /// <summary>
        /// Chooser name
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Target database
        /// </summary>
        /// <remarks>Used when a SQL chooser</remarks>
        public string Database { get; }

        /// <summary>
        /// Chooser type
        /// </summary>
        /// <remarks>select or sql</remarks>
        public string Type { get; }

        /// <summary>
        /// Chooser definition
        /// </summary>
        /// <remarks>Either a SQL query or a comma-separated list of values</remarks>
        public string Value { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id"></param>
        /// <param name="chooserName"></param>
        /// <param name="database"></param>
        /// <param name="chooserType"></param>
        public ChooserDefinition(int id, string chooserName, string database, string chooserType)
        {
            ID = id;
            Name = chooserName;
            Database = database;
            Type = chooserType;
        }
    }
}
