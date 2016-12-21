using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.CommandLineUtils;

namespace ConsoleApplication
{
    public class Program
    {
        public static int Main(string[] args)
        {
            CancellationTokenSource consoleAppStopped = new CancellationTokenSource();

            Console.CancelKeyPress += (s, e) => {
                Console.WriteLine("stopping");

                e.Cancel = true;
                consoleAppStopped.Cancel();
            };

            var app = new CommandLineApplication(throwOnUnexpectedArg: false);
            
            var fromFileArg = app.Argument("src", "source file");
            var toFileArg = app.Argument("dest", "destination file");
            var offsetFileOption = app.Option("-offset", "optional offset file name, if not specified [dest].offset is used", CommandOptionType.SingleValue);
            app.HelpOption("--help|-h");

            app.OnExecute(async () =>
            {
                string fromFile = fromFileArg.Value;
                string toFile = toFileArg.Value;

                if (string.IsNullOrWhiteSpace(fromFile) ||
                    string.IsNullOrWhiteSpace(toFile))
                {
                    app.ShowHelp();
                    return 2;
                }

                string offsetFile = offsetFileOption.Value();
                
                if (string.IsNullOrWhiteSpace(offsetFile))
                {
                    offsetFile = $"{toFile}.offset";
                }

                Stopwatch sw = new Stopwatch();
                sw.Start();

                Console.WriteLine($"copying from \"{fromFile}\" to \"{toFile}\"");
                try
                {
                    using (var copier = new Copier(
                        fromFile,
                        toFile,
                        offsetFile,
                        10 * 1024 * 1024,
                        10))
                    {
                        await copier.Start(consoleAppStopped.Token);
                    }
                }
                catch (CopyException ex)
                {
                    Console.WriteLine();
                    Console.WriteLine(ex.Message);

                    sw.Stop();
                    Console.WriteLine($"stopped after {sw.Elapsed.Hours} hours, {sw.Elapsed.Minutes} minutes, {sw.Elapsed.Seconds} seconds");
                    return 1;
                }
                catch (Exception)
                {
                    sw.Stop();
                    Console.WriteLine();
                    Console.WriteLine($"stopped after {sw.Elapsed.Hours} hours, {sw.Elapsed.Minutes} minutes, {sw.Elapsed.Seconds} seconds");

                    if (!consoleAppStopped.IsCancellationRequested)
                    {
                        throw;
                    }
                    else
                    {
                        return 0;
                    }
                }

                sw.Stop();
                Console.WriteLine();
                Console.WriteLine($"copy finished successfully in {sw.Elapsed.Hours} hours, {sw.Elapsed.Minutes} minutes, {sw.Elapsed.Seconds} seconds");
                return 0;
            });

            return app.Execute(args);
        }
    }
}
