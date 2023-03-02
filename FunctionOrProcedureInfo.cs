using System.Collections.Generic;

namespace DMSModelConfigDbUpdater
{
    internal class FunctionOrProcedureInfo
    {
        /// <summary>
        /// Argument list for the user defined function or procedure
        /// </summary>
        public List<FunctionOrProcedureArgumentInfo> ArgumentList { get; }

        /// <summary>
        /// Function or procedure name
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Function or procedure type
        /// </summary>
        public string ObjectType { get; }

        /// <summary>
        /// True if this is a stored procedure, false if a user defined function
        /// </summary>
        public bool IsProcedure { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="objectName">Function or procedure name</param>
        /// <param name="objectType">Object type description</param>
        /// <param name="isProcedure">True if a stored procedure, false if a function</param>
        public FunctionOrProcedureInfo(string objectName, string objectType, bool isProcedure)
        {
            Name = objectName;
            ObjectType = objectType;
            IsProcedure = isProcedure;

            ArgumentList = new List<FunctionOrProcedureArgumentInfo>();
        }
    }
}
