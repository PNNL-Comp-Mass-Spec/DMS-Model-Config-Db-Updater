namespace DMSModelConfigDbUpdater
{
    internal class BasicFormField
    {
        public int ID { get; }

        public string FormFieldName { get; }

        public BasicFormField(int id, string formFieldName)
        {
            ID = id;
            FormFieldName = formFieldName;
        }

        public override string ToString()
        {
            return FormFieldName;
        }
    }
}
