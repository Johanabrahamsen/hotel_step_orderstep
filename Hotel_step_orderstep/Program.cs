using System;
using System.Data.SqlClient;
using System.Net;
using System.Net.Http;
using System.Security.Authentication;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System.Configuration;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;

class Program
{
    private static readonly HttpClient client = new HttpClient();

    // Fetching the credentials from App.config
    private static readonly string clientId = ConfigurationManager.AppSettings["ClientId"];
    private static readonly string clientSecret = ConfigurationManager.AppSettings["ClientSecret"];
    private static readonly string organizationId = ConfigurationManager.AppSettings["OrganizationId"];

    public static async Task Main(string[] args)
    {
        // Force TLS 1.2
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

        Console.WriteLine("Starting API and database connection test...");

        try
        {
            // Fetch the latest meal order data from SQL database
            Console.WriteLine("Fetching the latest meal order data from SQL Server...");
            var latestMealData = FetchLatestMealOrderData();

            if (latestMealData.Count > 0)
            {
                Console.WriteLine("Fetched latest meal data:");
                foreach (var meal in latestMealData)
                {
                    Console.WriteLine(JsonConvert.SerializeObject(meal));
                }

                // Get Token for API
                Console.WriteLine("Attempting to retrieve the token...");
                var token = await GetToken();

                if (token != null)
                {
                    Console.WriteLine("Token retrieval successful!");
                    Console.WriteLine($"Access Token: {token}");

                    // Retrieve products from OrderStep API
                    Console.WriteLine("Attempting to retrieve the product list...");
                    var orderstepProducts = await GetAtlanticAirwaysProducts(token);

                    // Compare and calculate orders
                    var ordersToPlace = CompareAndCalculateOrders(latestMealData, orderstepProducts);

                    // Send the orders to OrderStep API
                    Console.WriteLine("Attempting to send the order...");
                    await SendOrderData(token, ordersToPlace);  // Correct usage: two arguments

                    Console.WriteLine("Order data prepared, sent successfully.");

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

        Console.ReadLine();  // Keeps the console open
    }

    // Step 1: Fetch the latest meal order data from SQL database
    public static List<dynamic> FetchLatestMealOrderData()
    {
        var connectionString = "Server=aa-sql2;Database=PAX_DATA;Integrated Security=True;";
        var mealOrderDataList = new List<dynamic>();

        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            try
            {
                Console.WriteLine("Preparing to connect to SQL Server...");
                connection.Open();
                Console.WriteLine("Connected to SQL Server successfully.");

                // Query to fetch the meal order data
                string query = @"
                SELECT d.flight_no as FlúgviNr, CAST(d.std as date) as Dato,
                CASE WHEN mbmt.OrderType=1 THEN 'Søla'
                     WHEN mbmt.OrderType=3 THEN 'Prepaid'
                     WHEN mbmt.OrderType=5 THEN 'Crew'
                     WHEN mbmt.OrderType=6 THEN 'Ekstra' END as Slag,
                mt.Name as MatarSlag, mbmt.Quantity as Antal, mt.MealDeliveryCode
                FROM meal.Flight f
                INNER JOIN meal.FlightNr fn ON f.FlightNrId = fn.FlightNrId
                INNER JOIN meal.MealBooking mb ON mb.FlightId = f.FlightId
                INNER JOIN (
                    SELECT mb1.FlightId, MAX(mb1.MealBookingId) as MealBookingId
                    FROM meal.MealBooking mb1
                    GROUP BY mb1.FlightId) a ON a.FlightId = f.FlightId
                INNER JOIN meal.MealBookingMealType mbmt ON mbmt.MealBookingId = mb.MealBookingId
                INNER JOIN DEPARTURES d ON d.id = f.DepartureId
                INNER JOIN meal.MealType mt ON mbmt.MealTypeId = mt.MealTypeId
                WHERE CAST(f.FlightDate as date) = CAST(DATEADD(day, 1, GETDATE()) as date)
                AND mb.MealBookingId = a.MealBookingId
                ORDER BY 2, 1, 3 DESC, 4";

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        // Read each row and add to the list
                        while (reader.Read())
                        {
                            string formattedDate = DateTime.Parse(reader["Dato"].ToString()).ToString("yyyy-MM-dd");

                            var mealOrderData = new
                            {
                                flightNumber = reader["FlúgviNr"].ToString(),
                                date = formattedDate,
                                type = reader["Slag"].ToString(),
                                mealType = reader["MatarSlag"].ToString(),
                                quantity = reader["Antal"].ToString(),
                                MealDeliveryCode = reader["MealDeliveryCode"].ToString()
                            };

                            mealOrderDataList.Add(mealOrderData);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred while fetching data: {ex.Message}");
            }
        }

        return mealOrderDataList;
    }

    // Step 2: Get Token from OrderStep API
    public static async Task<string> GetToken()
    {
        var tokenEndpoint = "https://secure.orderstep.dk/oauth2/token/";

        try
        {
            var clientCredentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{clientId}:{clientSecret}"));

            var request = new HttpRequestMessage(HttpMethod.Post, tokenEndpoint);
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", clientCredentials);

            var content = new StringContent("grant_type=client_credentials", Encoding.UTF8, "application/x-www-form-urlencoded");
            request.Content = content;

            HttpResponseMessage response = await client.SendAsync(request);

            if (response.IsSuccessStatusCode)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                dynamic jsonResponse = JsonConvert.DeserializeObject(responseString);
                return jsonResponse.access_token;
            }
            else
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"Failed to get token. HTTP Status: {response.StatusCode}");
                Console.WriteLine($"Error Details: {errorContent}");
                return null;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Unexpected error: {ex.Message}");
            return null;
        }
    }

    // Step 3: Retrieve lead/customer list from the API
    public static async Task GetLeadsAndCustomers(string token)
    {
        var customerEndpoint = "https://secure.orderstep.dk/public/api/v1/leads_customers";

        try
        {
            client.DefaultRequestHeaders.Clear();
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
            client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);

            // Get the list of leads/customers
            HttpResponseMessage response = await client.GetAsync(customerEndpoint);

            if (response.IsSuccessStatusCode)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                dynamic jsonResponse = JsonConvert.DeserializeObject(responseString);

                Console.WriteLine("Retrieved lead/customer list:");

                // Loop through the results and print the lead/customer details
                foreach (var customer in jsonResponse.results)
                {
                    Console.WriteLine($"ID: {customer.id}, Name: {customer.name}, Number: {customer.number}, Email: {customer.mail}");
                }
            }
            else
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"Failed to retrieve customer list. HTTP Status: {response.StatusCode}");
                Console.WriteLine($"Error Details: {errorContent}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exception occurred while retrieving customer list: {ex.Message}");
        }
    }

    // Step 4: Retrieve products list from the API with pagination and rate limit handling
    public static async Task<List<dynamic>> GetAtlanticAirwaysProducts(string token)
    {
        var productsEndpoint = "https://secure.orderstep.dk/public/api/v1/products/";
        string nextPageUrl = productsEndpoint; // Start with the initial endpoint
        var products = new List<dynamic>();

        try
        {
            client.DefaultRequestHeaders.Clear();
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
            client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);  // This is the ID for Atlantic Airways

            while (!string.IsNullOrEmpty(nextPageUrl))
            {
                HttpResponseMessage response = await client.GetAsync(nextPageUrl);

                if (response.IsSuccessStatusCode)
                {
                    var responseString = await response.Content.ReadAsStringAsync();
                    dynamic jsonResponse = JsonConvert.DeserializeObject(responseString);

                    Console.WriteLine("Retrieved Atlantic Airways product list:");
                    products.AddRange(jsonResponse.results);


                    // Check for pagination (if there is a next page)
                    nextPageUrl = jsonResponse.next;

                    // Delay to avoid hitting rate limits
                    await Task.Delay(2000); // 2-second delay between requests
                }
                else if ((int)response.StatusCode == 429) // Check if status code is 429
                {
                    // Handle rate limit error (HTTP 429)
                    var retryAfter = response.Headers.RetryAfter?.Delta?.TotalSeconds ?? 1; // Use Retry-After header if provided
                    Console.WriteLine($"Rate limit hit. Retrying in {retryAfter} seconds...");
                    await Task.Delay((int)(retryAfter * 1000)); // Wait before retrying
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Failed to retrieve product list. HTTP Status: {response.StatusCode}");
                    Console.WriteLine($"Error Details: {errorContent}");
                    break; // Exit the loop on any other error
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exception occurred while retrieving product list: {ex.Message}");
        }

        return products;
    }

    // Step 5: Compare the meal order data with product list and calculate the orders
    public static List<dynamic> CompareAndCalculateOrders(List<dynamic> mealOrders, List<dynamic> orderstepProducts)
    {
        var ordersToPlace = new List<dynamic>();

        // Step 1: Group by MealDeliveryCode and sum the quantities
        var groupedMealOrders = mealOrders
            .GroupBy(m => m.MealDeliveryCode)
            .Select(g => new
            {
                MealDeliveryCode = g.Key,
                TotalQuantity = g.Sum(m => int.Parse(m.quantity)) // Summing up the quantity for each MealDeliveryCode
            })
            .ToList();

        // Step 2: Match with the products from OrderStep API
        foreach (var groupedMeal in groupedMealOrders)
        {
            // Find the matching product in the OrderStep product list using MealDeliveryCode as product_id
            var matchingProduct = orderstepProducts.Find(p => p.id.ToString() == groupedMeal.MealDeliveryCode.ToString());

            if (matchingProduct != null)
            {
                // Add the matched product and total quantity to the order
                var orderLine = new
                {
                    product_id = matchingProduct.id,
                    qty = groupedMeal.TotalQuantity,
                    description = matchingProduct.name // Adding the product name for clarity
                };

                ordersToPlace.Add(orderLine);
            }
            else
            {
                Console.WriteLine($"No matching product found for MealDeliveryCode: {groupedMeal.MealDeliveryCode}");
            }
        }

        return ordersToPlace;
    }

    // Step 6: Send the orders to OrderStep API
    public static async Task SendOrderData(string token, List<dynamic> ordersToPlace)
    {
        var orderEndpoint = "https://secure.orderstep.dk/public/api/v1/sale_orders/";

        try
        {
            client.DefaultRequestHeaders.Clear();
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
            client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);

            if (ordersToPlace.Count == 0)
            {
                Console.WriteLine("No order lines to send. Exiting...");
                return;
            }

            // Prepare the order payload
            // Prepare the order payload
            var jsonPayload = new
            {
                lead_customer_id = 307480, // Atlantic Airways ID
                title = $"TEST TEST {DateTime.UtcNow.AddDays(1):yyyy-MM-dd}", // Title with tomorrow's date
                language = "fo", // Faroese
                date = DateTime.UtcNow.AddDays(1).ToString("yyyy-MM-dd"), // Order date for tomorrow
                delivery_date = DateTime.UtcNow.AddDays(1).ToString("yyyy-MM-dd"), // Delivery date for tomorrow
                lines = ordersToPlace, // All the order lines with product_id and total quantity
                calendar_event_resources = new[]
                {
                    new
                    {
                        title = $"Flogmatur {DateTime.UtcNow.AddDays(1):yyyy-MM-dd}", // Event title for tomorrow
                        guests = 1,
                        start_datetime = $"{DateTime.UtcNow.AddDays(1):yyyy-MM-dd}T08:00:00Z", // Start datetime for tomorrow
                        end_datetime = $"{DateTime.UtcNow.AddDays(1):yyyy-MM-dd}T20:00:00Z", // End datetime for tomorrow
                        calendar_id = 5,
                        calendar_resource_id = 55,
                        calendar_category_id = 21
                    }
                }
            };


            // Convert to JSON
            var content = new StringContent(JsonConvert.SerializeObject(jsonPayload), Encoding.UTF8, "application/json");

            // POST the data to the Orderstep API
            HttpResponseMessage response = await client.PostAsync(orderEndpoint, content);

            if (response.IsSuccessStatusCode)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"Data successfully sent to OrderStep API: {responseString}");
            }
            else
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"Failed to send data to OrderStep API. HTTP Status: {response.StatusCode}");
                Console.WriteLine($"Error Details: {errorContent}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exception occurred while sending the request: {ex.Message}");
        }
    }



}
