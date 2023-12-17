// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Samples.Bulk
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;

    // ----------------------------------------------------------------------------------------------------------
    // Prerequisites -
    //
    // 1. An Azure Cosmos account - 
    //    https://azure.microsoft.com/en-us/itemation/articles/itemdb-create-account/
    //
    // 2. Microsoft.Azure.Cosmos NuGet package - 
    //    http://www.nuget.org/packages/Microsoft.Azure.Cosmos/ 
    // ----------------------------------------------------------------------------------------------------------
    public class Program
    {
        private const string EndpointUrl = "https://<your-account>.documents.azure.com:443/";
        private const string AuthorizationKey = "<your-account-key like VxTxu70Mn4OksQLqzs>";
        private const string DatabaseName = "bulk-tutorial";
        private const string ContainerName = "items";
        private const string partitionKey = "/partitionKey";
        private const int AmountToInsert = 300000;


        static async Task Main(string[] args)
        {
            // <CreateClient>
            CosmosClient cosmosClient = new CosmosClient(EndpointUrl, AuthorizationKey, new CosmosClientOptions() { AllowBulkExecution = true });
            // </CreateClient>

            // Create with a throughput of 50000 RU/s
            // Indexing Policy to exclude all attributes to maximize RU/s usage
            Console.WriteLine("This tutorial will create a 50000 RU/s container, press any key to continue.");
            Console.ReadKey();

            // <Initialize>
            Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(Program.DatabaseName);

            if (database.GetContainer(Program.ContainerName) == null)
                await database.DefineContainer(Program.ContainerName, partitionKey)
                        .WithIndexingPolicy()
                            .WithIndexingMode(IndexingMode.Consistent)
                            .WithIncludedPaths()
                                .Attach()
                            .WithExcludedPaths()
                                .Path("/*")
                                .Attach()
                        .Attach()
                    .CreateAsync(50000);

            // </Initialize>

            try
            {
                // Prepare items for insertion
                Console.WriteLine($"Preparing {AmountToInsert} items to insert...");
                // <Operations>
                //IReadOnlyCollection<Item> itemsToInsert = Program.GetItemsToInsert();
                IReadOnlyCollection<Item> itemsToInsert = ReadExcelData<Item>(filePath: @"C:\Users\Madhusudhan\Downloads\Max\4 - Copy.xlsx");

                // </Operations>

                // Create the list of Tasks
                Console.WriteLine($"Starting...");
                Stopwatch stopwatch = Stopwatch.StartNew();
                // <ConcurrentTasks>
                Container container = database.GetContainer(ContainerName);
                List<Task> tasks = new List<Task>(AmountToInsert);
                int counter = 0;
                foreach (Item item in itemsToInsert)
                {
                    if (counter > 5) break;
                    counter++;
                    tasks.Add(container.CreateItemAsync(item, new PartitionKey(item.id))
                        .ContinueWith(itemResponse =>
                        {
                            if (!itemResponse.IsCompletedSuccessfully)
                            {
                                AggregateException innerExceptions = itemResponse.Exception.Flatten();
                                if (innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                                {
                                    Console.WriteLine($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                                }
                                else
                                {
                                    Console.WriteLine($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                                }
                            }
                        }));
                }

                // Wait until all are done
                await Task.WhenAll(tasks);
                // </ConcurrentTasks>
                stopwatch.Stop();

                Console.WriteLine($"Finished in writing {AmountToInsert} items in {stopwatch.Elapsed}.");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                Console.WriteLine("Cleaning up resources...");
                await database.DeleteAsync();
            }
        }

        // <Bogus>
        private static IReadOnlyCollection<Item> GetItemsToInsert()
        {
            return new Bogus.Faker<Item>()
            .StrictMode(true)
            //Generate item
            .RuleFor(o => o.id, f => Guid.NewGuid().ToString()) //id
            //.RuleFor(o => o.username, f => f.Internet.UserName())
            //.RuleFor(o => o.pk, (f, o) => o.id) //partitionkey
            .Generate(AmountToInsert);
        }

        static List<T> ReadExcelData<T>(string filePath, string sheetName = "Sheet1") where T : new()
        {
            var data = new List<T>();

            // Load the Excel file
            var workbook = new Aspose.Cells.Workbook(filePath);

            // Access the desired worksheet
            var worksheet = workbook.Worksheets[sheetName];

            // Create a mapping of column names to column indices
            var columnIndexMap = new Dictionary<string, int>();
            for (int columnIndex = 0; columnIndex <= worksheet.Cells.MaxDataColumn; columnIndex++)
            {
                var cellValue = worksheet.Cells[0, columnIndex].StringValue;
                columnIndexMap[cellValue] = columnIndex;
            }

            // Iterate over the rows (starting from the second row)
            for (int row = 1; row <= worksheet.Cells.MaxDataRow; row++)
            {
                // Create an instance of the class
                var item = new T();

                // Iterate over the properties of the class
                foreach (var property in typeof(T).GetProperties())
                {
                    // Check if the property name is present in the Excel file
                    if (columnIndexMap.TryGetValue(property.Name, out var columnIndex))
                    {
                        // Get the cell value from the current row and column
                        var cellValue = worksheet.Cells[row, columnIndex].StringValue;

                        // Convert the cell value to the property type and set the value
                        var convertedValue = Convert.ChangeType(cellValue, property.PropertyType);
                        property.SetValue(item, convertedValue);
                    }
                }

                // Add the object to the list
                data.Add(item);
            }

            return data;
        }
        // </Bogus>

        // <Model>
        public class Item
        {
            public string id { get; set; }
            public string name { get; set; }

            public string price { get; set; }
        }
        // </Model>
    }
}
