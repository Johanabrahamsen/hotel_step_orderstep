using System;
using System.Data.SqlClient;  // For SQL connection
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Starting API and database connection test...");

        try
        {
            // Step 1: Fetch the latest meal order data from SQL database
            Console.WriteLine("Fetching the latest meal order data from SQL Server...");
            var latestMealData = FetchLatestMealOrderData();

                        Console.ReadLine(); 


            if (latestMealData != null)
            {
                Console.WriteLine("Fetched latest meal data:");

                Console.WriteLine(latestMealData);  // Print fetched data for debugging
                Console.ReadLine();

                // Step 2: Get Token for API
                Console.WriteLine("Attempting to retrieve the token...");
                var token = await GetToken();

                if (token != null)
                {
                    Console.WriteLine("Token retrieval successful!");
                    Console.WriteLine($"Access Token: {token}");

                    // Step 3: Send the meal data to OrderStep API
                    Console.WriteLine("Attempting to send the meal order data to OrderStep API...");
                    await SendOrderData(token, latestMealData);
                }
                else
                {
                    Console.WriteLine("Failed to retrieve token. Exiting...");
                }
            }
            else
            {
                Console.WriteLine("No data found in the database.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }
    }

    // Step 1: Fetch the latest meal order data from SQL database
    public static string FetchLatestMealOrderData()
    {
        // SQL connection string with Windows Authentication
        var connectionString = "Server=aa-sql2;Database=PAX_DATA;Integrated Security=True;";

        string latestMealData = null;

        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            try
            {
                Console.WriteLine("Preparing to connect to SQL Server...");
                connection.Open();
                Console.WriteLine("Connected to SQL Server successfully.");

                // Query to get the latest meal order (adjust the table/column names as needed)
                string query = @"
                    SELECT TOP 1 * 
                    FROM [YourTableName]  -- Replace with actual table name
                    ORDER BY [DateColumn] DESC";  // Replace with actual date column to order by

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            // Replace with actual column names from your table
                            latestMealData = $"Order ID: {reader["OrderID"]}, Meal: {reader["MealName"]}, Quantity: {reader["Quantity"]}, Date: {reader["DateColumn"]}";
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred while fetching data: {ex.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
            }
        }

        return latestMealData;
    }

    // Step 2: Get Token from OrderStep API
    public static async Task<string> GetToken()
    {
        var tokenEndpoint = "https://test14.orderstep.dk/oauth2/token/";

        // Combine client_id and client_secret into a Base64-encoded string for Basic authentication
        var clientCredentials = Convert.ToBase64String(Encoding.UTF8.GetBytes("UEPk1ZljXzACz7P75hAhi03kzKHylgxpdnmDFFfK:08Y0kUgV6DBIYhgrEk2P2cppyWGylmLJW9lz3YC2A44JZFOHD0tMqFwZ6bkAemeChso7ZCphqOpeIqaMYeUaerkuBn82ohgHqtKCRUB7HzpUVhJ3aC7G0b7Suz8OdvXj"));

        var request = new HttpRequestMessage(HttpMethod.Post, tokenEndpoint);
        request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", clientCredentials);

        var content = new StringContent("grant_type=client_credentials", Encoding.UTF8, "application/x-www-form-urlencoded");
        request.Content = content;

        HttpResponseMessage response = await client.SendAsync(request);

        if (response.IsSuccessStatusCode)
        {
            Console.WriteLine("Token endpoint responded successfully.");
            var responseString = await response.Content.ReadAsStringAsync();
            dynamic jsonResponse = JsonConvert.DeserializeObject(responseString);
            Console.WriteLine("Token extracted from the response.");
            return jsonResponse.access_token;
        }
        else
        {
            Console.WriteLine($"Failed to get token. HTTP Status: {response.StatusCode}");
            return null;
        }
    }

    // Step 3: Send the fetched meal order data to OrderStep API
    public static async Task SendOrderData(string token, string mealData)
    {
        var orderEndpoint = "https://test14.orderstep.dk/public/api/v1/sale_orders/";  // Corrected endpoint
        var organizationId = "c878a8e9-dd94-45e2-82de-2497a8ca96a6";  // Organization ID

        try
        {
            // Clear headers from any previous requests
            client.DefaultRequestHeaders.Clear();

            // Set headers
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
            client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);
            client.DefaultRequestHeaders.Add("Accept-Language", "en");

            // Example of sending data (this will be developed further in the next step)
            var content = new StringContent(JsonConvert.SerializeObject(mealData), Encoding.UTF8, "application/json");
            HttpResponseMessage response = await client.PostAsync(orderEndpoint, content);

            if (response.IsSuccessStatusCode)
            {
                Console.WriteLine("Order data sent successfully!");
                var responseString = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"Response: {responseString}");
            }
            else
            {
                Console.WriteLine($"Failed to send order data. HTTP Status: {response.StatusCode}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exception occurred while sending order data: {ex.Message}");
        }
    }

    private static readonly HttpClient client = new HttpClient();
}
