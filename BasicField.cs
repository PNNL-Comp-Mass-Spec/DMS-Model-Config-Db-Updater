namespace DMSModelConfigDbUpdater
{
    internal class BasicField
    {
        public int ID { get; }

        public string FieldName { get; }

        public BasicField(int id, string fieldName)
        {
            ID = id;
            FieldName = fieldName;
        }

        public override string ToString()
        {
            return FieldName;
        }
    }
}
