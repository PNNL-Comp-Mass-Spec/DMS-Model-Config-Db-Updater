using System;
using System.IO;
using System.Reflection;
using System.Threading;
using PRISM;

namespace DMSModelConfigDbUpdater
{
    internal class Program
    {
        // Ignore Spelling: Conf

        private static int Main(string[] args)
        {
            var asmName = typeof(Program).GetTypeInfo().Assembly.GetName();
            var exeName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);       // Alternatively: System.AppDomain.CurrentDomain.FriendlyName
            var version = ModelConfigDbUpdaterOptions.GetAppVersion();

            var parser = new CommandLineParser<ModelConfigDbUpdaterOptions>(asmName.Name, version)
            {
                ProgramInfo = "This program updates column names in the SQLite files used by the DMS website to " +
                              "retrieve data from the DMS database and display it for users. It reads information " +
                              "about renamed columns from a tab-delimited text file and looks through the SQLite files " +
                              "for references to the parent views, updating the names of any renamed columns.",

                ContactInfo = "Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)" +
                              Environment.NewLine + Environment.NewLine +
                              "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                              "Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://www.pnnl.gov/integrative-omics",

                UsageExamples =
                {
                    exeName + "  /I:. /O:UpdatedFiles /Map:DMS5_views_excerpt_RenamedColumns.txt",
                    exeName + "  /I:. /Validate /WriteResults",
                    exeName + "  /Conf:ModelConfigDbUpdaterOptions.conf",
                    exeName + "  /Conf:ModelConfigDbValidationOptions.conf"
                }
            };

            parser.AddParamFileKey("Conf");

            var result = parser.ParseArgs(args);
            var options = result.ParsedResults;

            try
            {
                if (!result.Success)
                {
                    if (parser.CreateParamFileProvided)
                    {
                        return 0;
                    }

                    // Delay for 1500 msec in case the user double-clicked this file from within Windows Explorer (or started the program via a shortcut)
                    Thread.Sleep(1500);
                    return -1;
                }

                if (!options.ValidateArgs(parser.ParameterFilePath, out var errorMessage))
                {
                    parser.PrintHelp();

                    Console.WriteLine();
                    ConsoleMsgUtils.ShowWarning("Validation error:");
                    ConsoleMsgUtils.ShowWarning(errorMessage);

                    Thread.Sleep(1500);
                    return -1;
                }

                options.OutputSetOptions();
                Console.WriteLine();
            }
            catch (Exception e)
            {
                Console.WriteLine();
                Console.Write($"Error running {exeName}");
                Console.WriteLine(e.Message);
                Console.WriteLine($"See help with {exeName} --help");
                return -1;
            }

            try
            {
                var processor = new ModelConfigDbUpdater(options);

                processor.ErrorEvent += Processor_ErrorEvent;
                processor.StatusEvent += Processor_StatusEvent;
                processor.WarningEvent += Processor_WarningEvent;

                var success = processor.ProcessInputDirectory();

                if (success)
                {
                    Console.WriteLine();
                    Console.WriteLine("Processing complete");
                    Thread.Sleep(500);
                    return 0;
                }

                ConsoleMsgUtils.ShowWarning("Processing error");
                Thread.Sleep(1500);
                return -1;
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error occurred in Program->Main", ex);
                Thread.Sleep(1500);
                return -1;
            }
        }

        private static void Processor_ErrorEvent(string message, Exception ex)
        {
            ConsoleMsgUtils.ShowErrorCustom(message, ex, false);
        }

        private static void Processor_StatusEvent(string message)
        {
            Console.WriteLine(message);
        }

        private static void Processor_WarningEvent(string message)
        {
            ConsoleMsgUtils.ShowWarning(message);
        }
    }
}
