using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
//using Microsoft.Extensions.Configuration;

namespace CosmosDb
{
    //https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/tutorial-dotnet-web-app
    //public interface ICosmosService
    //{
    //    Task TestCreateDbContainerAddDataRetrieveFullFlow();
    //    Task<IEnumerable<Product>> RetrieveAllProductsAsync();
    //    Task<IEnumerable<Product>> RetrieveAllProductsWithConditionAsync(string? name = null, string? description = null);
    //}

    public class CosmosService //: ICosmosService
    {
        private readonly CosmosClient _client;
        public string _databaseName = "cosmicworks";
        public string _containerName = "products01";
        public string _partitionKeyPath = "/id";
        private Container Container
        {
            get => _client.GetDatabase(_databaseName).GetContainer(_containerName);
        }
        public CosmosService()
        {
            _client = new CosmosClient(accountEndpoint: "https://unwell-medihkjkhjkcines-cdb.documents.azure.com:443/",
    authKeyOrResourceToken: "NOj4XOY6jQ11jXWvQ32zCqACDbknZKfg==");
        }

        public async Task TestCreateDbContainerAddDataRetrieveFullFlow()
        {

            // <new_database> 
            // Database reference with creation if it does not already exist
            Database database = await _client.CreateDatabaseIfNotExistsAsync(
                id: _databaseName
            );

            Console.WriteLine($"New database:\t{database.Id}");
            // </new_database>

            // <new_container> 
            // Container reference with creation if it does not already exist
            Container container = await database.CreateContainerIfNotExistsAsync(
                id: _containerName,
                partitionKeyPath: _partitionKeyPath
            );

            Console.WriteLine($"New container:\t{container.Id}");
            // </new_container>

            // <new_item> 
            // Create new object and upsert (create or replace) to container

            Product newItem = new Product(id: "baaa4d2d-5ebe-45fb-9a5c-d06876f408e0",   name: "ML Road Frame - Red, 60", price: 594.83000000000004m);

            if (Debugger.IsAttached)
            {
                Product createdItem = await container.CreateItemAsync<Product>(
                    item: newItem,
                    partitionKey: new PartitionKey("baaa4d2d-5ebe-45fb-9a5c-d06876f408e0")
                );

                Console.WriteLine($"Created item:\t{createdItem.id}\t[{createdItem.name}]");
            }
            // </new_item>

            Product toUpdateItem = new Product(id: "baaa4d2d-5ebe-45fb-9a5c-d06876f408e0", name: "ML Road Frame - Red, 60 zxczxcxzc", price: 694.83000000000004m);

            Product updatedItem = await container.UpsertItemAsync<Product>(
                item: toUpdateItem,
                partitionKey: new PartitionKey("baaa4d2d-5ebe-45fb-9a5c-d06876f408e0")
            );

            Console.WriteLine($"Updated item:\t{updatedItem.id}\t[{updatedItem.name}]");

            // <read_item> 
            // Point read item from container using the id and partitionKey
            var readItem = await container.ReadItemAsync<Product>(
                id: "baaa4d2d-5ebe-45fb-9a5c-d06876f408e0",
                partitionKey: new PartitionKey("baaa4d2d-5ebe-45fb-9a5c-d06876f408e0")
            );
            if (readItem == null)
                Console.WriteLine("Item not foind");
            else
                Console.WriteLine("Item found...." + readItem);
            // </read_item>

            // <query_items> 
            // Create query using a SQL string and parameters
            var query = new QueryDefinition(
                query: $"SELECT * FROM {_containerName} p WHERE p.id = @id"
            )
                .WithParameter("@id", "baaa4d2d-5ebe-45fb-9a5c-d06876f408e0");

            using FeedIterator<Product> feed = container.GetItemQueryIterator<Product>(
                queryDefinition: query
            );

            while (feed.HasMoreResults)
            {
                FeedResponse<Product> response = await feed.ReadNextAsync();
                foreach (Product item in response)
                {
                    Console.WriteLine($"Found item:\t{item.name}");
                }
            }
        }
        public async Task<IEnumerable<Product>> RetrieveAllProductsAsync()
        {
            var queryable = Container.GetItemLinqQueryable<Product>();
            using FeedIterator<Product> feed = queryable
    //.Where(p => p.price < 2000m)
    //.OrderByDescending(p => p.price)
    .ToFeedIterator();

            List<Product> results = new List<Product>(); 
            while (feed.HasMoreResults)
            {
                var response = await feed.ReadNextAsync();
                foreach (Product item in response)
                {
                    results.Add(item);
                }
            }
            return results;
        }
        public async Task<IEnumerable<Product>> RetrieveAllProductsWithConditionAsync(string? name = null, string? description = null)
        {
            if (string.IsNullOrEmpty(name) && string.IsNullOrEmpty(description))
                return await RetrieveAllProductsAsync();

            string sql = $@"
SELECT
    p.id,
    p.categoryid,
    p.categoryName,
    p.sku,
    p.name,
    p.description,
    p.price
FROM {_containerName} p
WHERE contains( p.name ,@nameFilter) 
";
            var query = new QueryDefinition(query: sql).WithParameter("@nameFilter", name);

            using FeedIterator<Product> feed = Container.GetItemQueryIterator<Product>(queryDefinition: query);
            List<Product> results = new List<Product>();

            while (feed.HasMoreResults)
            {
                FeedResponse<Product> response = await feed.ReadNextAsync();
                foreach (Product item in response)
                {
                    results.Add(item);
                }
            }

            return results;
        }


    }
    public class Product
    {
        public Product()
        {
            
        }
        public Product(string id,string name,decimal price)
        {
            this.id = id;
            this.name = name;
            this.price = price;
        }
        public string id { get; set; }
        public string name { get; set; }

        public decimal price { get; set; }
    }
}