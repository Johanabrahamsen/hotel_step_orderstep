using System;
using System.Data.SqlClient;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Configuration;

namespace HotelStepOrderStep
{
    // Extension method to support PATCH requests
    public static class HttpClientExtensions
    {
        public static Task<HttpResponseMessage> PatchAsync(this HttpClient client, string requestUri, HttpContent content)
        {
            var request = new HttpRequestMessage(new HttpMethod("PATCH"), requestUri)
            {
                Content = content
            };
            return client.SendAsync(request);
        }
    }

    // Represents a single line item in an order
    public class OrderLine
    {
        public int? Id { get; set; }
        public int ProductId { get; set; }
        public decimal Qty { get; set; }
        public string Description { get; set; }
        public decimal UnitSalesPrice { get; set; }
        public decimal UnitCostPrice { get; set; }
        public decimal? DiscountPer { get; set; }
    }

    class Program
    {
        private static readonly HttpClient client = new HttpClient();

        // Fetching the credentials from App.config or Environment Variables
        private static readonly string clientId = ConfigurationManager.AppSettings["ClientId"];
        private static readonly string clientSecret = ConfigurationManager.AppSettings["ClientSecret"];
        private static readonly string organizationId = ConfigurationManager.AppSettings["OrganizationId"];

        public static async Task Main(string[] args)
        {
            // Ensure TLS 1.2 is used for security
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            Console.WriteLine("Starting API and database connection test...");

            while (true)
            {
                try
                {
                    Console.WriteLine("Fetching the latest meal order data from SQL Server...");
                    var latestMealData = FetchLatestMealOrderData();

                    if (latestMealData.Count > 0)
                    {
                        Console.WriteLine("Fetched latest meal data:");
                        foreach (var meal in latestMealData)
                        {
                            Console.WriteLine(JsonConvert.SerializeObject(meal));
                        }

                        Console.WriteLine("Attempting to retrieve the token...");
                        var token = await GetToken();

                        if (token != null)
                        {
                            Console.WriteLine("Token retrieval successful!");

                            // Set the target date to one week forward
                            DateTime targetDate = DateTime.UtcNow.AddDays(7);

                            // Retrieve products from OrderStep API
                            var orderstepProducts = await GetAtlanticAirwaysProducts(token);

                            // Compare meal orders with the retrieved products to determine orders to place
                            var ordersToPlace = CompareAndCalculateOrders(latestMealData, orderstepProducts);

                            if (ordersToPlace.Count > 0)
                            {
                                // Fetch existing order for the target date for comparison
                                Console.WriteLine($"Fetching existing order for {targetDate:yyyy-MM-dd} from OrderStep API...");
                                var existingOrder = await FetchExistingOrderData(token, targetDate);

                                // Check and update orders based on the comparison
                                bool anyChanges = await CheckAndUpdateOrders(token, ordersToPlace, existingOrder, targetDate);

                                if (anyChanges)
                                {
                                    Console.WriteLine("Changes detected, processing order updates.");
                                    await SendOrderData(token, ordersToPlace, existingOrder, targetDate);
                                }
                                else
                                {
                                    Console.WriteLine("No changes detected. Skipping sending new orders.");
                                }

                                Console.WriteLine("Order data processed and updated successfully.");
                            }
                            else
                            {
                                Console.WriteLine("No valid orders to place after comparison.");
                            }
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

                // Wait for 15 minutes before the next iteration
                await Task.Delay(TimeSpan.FromMinutes(15));
            }
        }

        // Fetch the latest meal order data from SQL database
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

                    string query = @"
                        WITH MealData AS (
                            SELECT 
                                d.flight_no as FlúgviNr, 
                                CAST(d.std as date) as Dato,
                                CASE 
                                    WHEN mbmt.OrderType=1 THEN 'Sola'
                                    WHEN mbmt.OrderType=3 THEN 'Prepaid'
                                    WHEN mbmt.OrderType=5 THEN 'Crew'
                                    WHEN mbmt.OrderType=6 OR feo.FlightId IS NOT NULL THEN 'Ekstra'
                                END as Slag,
                                mt.Name as MatarSlag, 
                                mbmt.Quantity, 
                                ISNULL(feo.NumberOfExtra, 0) as NumberOfExtra,
                                mt.MealDeliveryCode,
                                ROW_NUMBER() OVER (PARTITION BY d.flight_no, mt.MealDeliveryCode ORDER BY mbmt.OrderType) as RowNum
                            FROM 
                                meal.Flight f
                            INNER JOIN 
                                meal.FlightNr fn ON f.FlightNrId = fn.FlightNrId
                            INNER JOIN 
                                meal.MealBooking mb ON mb.FlightId = f.FlightId
                            INNER JOIN 
                                (SELECT mb1.FlightId, MAX(mb1.MealBookingId) as MealBookingId
                                 FROM meal.MealBooking mb1
                                 GROUP BY mb1.FlightId) a ON a.FlightId = f.FlightId
                            INNER JOIN 
                                meal.MealBookingMealType mbmt ON mbmt.MealBookingId = mb.MealBookingId
                            INNER JOIN 
                                DEPARTURES d ON d.id = f.DepartureId
                            INNER JOIN 
                                meal.MealType mt ON mbmt.MealTypeId = mt.MealTypeId
                            LEFT JOIN 
                                [PAX_DATA].[Meal].[FlightExtraOrder] feo 
                                ON feo.FlightId = f.FlightId AND feo.MealTypeId = mbmt.MealTypeId
                            WHERE 
                                CAST(f.FlightDate as date) = CAST(DATEADD(day, 7, GETDATE()) as date)
                            AND 
                                mb.MealBookingId = a.MealBookingId
                        )
                        SELECT 
                            FlúgviNr, 
                            Dato,
                            Slag,
                            MatarSlag,
                            SUM(Quantity) + CASE WHEN RowNum = 1 THEN SUM(NumberOfExtra) ELSE 0 END as Antal, 
                            MealDeliveryCode
                        FROM 
                            MealData
                        GROUP BY 
                            FlúgviNr, Dato, Slag, MatarSlag, MealDeliveryCode, RowNum
                        ORDER BY 
                            2, 1, 3 DESC, 4;
                    ";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
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

        // Get Token from OrderStep API
        public static async Task<string> GetToken()
        {
            var tokenEndpoint = "https://secure.orderstep.dk/oauth2/token/";

            try
            {
                var clientCredentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{clientId}:{clientSecret}"));

                var request = new HttpRequestMessage(HttpMethod.Post, tokenEndpoint);
                request.Headers.Authorization = new AuthenticationHeaderValue("Basic", clientCredentials);

                var content = new StringContent("grant_type=client_credentials", Encoding.UTF8, "application/x-www-form-urlencoded");
                request.Content = content;

                HttpResponseMessage response = await SendWithRetryAsync(() => client.SendAsync(request));

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

        // Retrieve products list from the API with pagination and rate limit handling
        public static async Task<List<dynamic>> GetAtlanticAirwaysProducts(string token)
        {
            var productsEndpoint = "https://secure.orderstep.dk/public/api/v1/products/";
            string nextPageUrl = productsEndpoint; // Start with the initial endpoint
            var products = new List<dynamic>();

            try
            {
                client.DefaultRequestHeaders.Clear();
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);  // Set the organization ID for Atlantic Airways

                while (!string.IsNullOrEmpty(nextPageUrl))
                {
                    HttpResponseMessage response = await SendWithRetryAsync(() => client.GetAsync(nextPageUrl));

                    if (response.IsSuccessStatusCode)
                    {
                        var responseString = await response.Content.ReadAsStringAsync();
                        dynamic jsonResponse = JsonConvert.DeserializeObject(responseString);

                        Console.WriteLine("Retrieved Atlantic Airways product list:");
                        products.AddRange(jsonResponse.results);

                        nextPageUrl = jsonResponse.next != null ? jsonResponse.next.ToString() : null; // Update to the next page URL if it exists

                        // Delay to avoid hitting rate limits
                        await Task.Delay(2000); // Wait 2 seconds between requests
                    }
                    else
                    {
                        var errorContent = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"Failed to retrieve product list. HTTP Status: {response.StatusCode}");
                        Console.WriteLine($"Error Details: {errorContent}");
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occurred while retrieving product list: {ex.Message}");
            }

            return products;
        }

        // Compare the meal order data with product list and calculate the orders
        public static List<OrderLine> CompareAndCalculateOrders(List<dynamic> mealOrders, List<dynamic> orderstepProducts)
        {
            var ordersToPlace = new List<OrderLine>();

            var groupedMealOrders = mealOrders
                .GroupBy(m => m.MealDeliveryCode)
                .Select(g => new
                {
                    MealDeliveryCode = g.Key,
                    TotalQuantity = g.Sum(m => int.Parse(m.quantity))
                })
                .ToList();

            // Define default prices for products
            var productPrices = new Dictionary<int, decimal>
            {
                {207056, 44.00m},
                {207058, 43.00m},
                {207057, 48.00m},
                {207060, 40.00m},
                {207059, 50.00m},
                {207064, 65.00m},
                {207062, 75.00m},
                {207065, 75.00m},
                {207063, 75.00m},
                {207061, 60.00m} // Assuming price for Vegan (MealDeliveryCode: 207061)
            };

            foreach (var groupedMeal in groupedMealOrders)
            {
                var matchingProduct = orderstepProducts.FirstOrDefault(p => p.id.ToString() == groupedMeal.MealDeliveryCode.ToString());

                if (matchingProduct != null)
                {
                    // Explicitly convert matchingProduct.id to int
                    int productId = Convert.ToInt32(matchingProduct.id);

                    // Get the unit_sales_price from the productPrices dictionary
                    decimal unitSalesPrice = productPrices.ContainsKey(productId) ? productPrices[productId] : 0m;
                    decimal unitCostPrice = 0m; // Set to zero or appropriate value

                    var orderLine = new OrderLine
                    {
                        Id = null,
                        ProductId = productId,
                        Qty = groupedMeal.TotalQuantity,
                        Description = matchingProduct.name,
                        UnitSalesPrice = unitSalesPrice,
                        UnitCostPrice = unitCostPrice,
                        DiscountPer = null
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

        // Fetch existing order data from OrderStep API
        public static async Task<dynamic> FetchExistingOrderData(string token, DateTime targetDate)
        {
            var orderEndpoint = "https://secure.orderstep.dk/public/api/v1/sale_orders/";
            dynamic existingOrder = null;

            try
            {
                client.DefaultRequestHeaders.Clear();
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);

                var referenceValue = $"MealOrder_{targetDate:yyyyMMdd}";
                var requestUrl = $"{orderEndpoint}?ref={Uri.EscapeDataString(referenceValue)}";

                HttpResponseMessage response = await SendWithRetryAsync(() => client.GetAsync(requestUrl));

                if (response.IsSuccessStatusCode)
                {
                    var responseString = await response.Content.ReadAsStringAsync();
                    dynamic jsonResponse = JsonConvert.DeserializeObject(responseString);

                    if (jsonResponse.results.Count > 0)
                    {
                        existingOrder = jsonResponse.results[0];
                        Console.WriteLine("Existing order found:");
                        Console.WriteLine(JsonConvert.SerializeObject(existingOrder, Formatting.Indented));
                    }
                    else
                    {
                        Console.WriteLine("No existing order found.");
                    }
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Failed to fetch existing order. HTTP Status: {response.StatusCode}");
                    Console.WriteLine($"Error Details: {errorContent}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occurred while fetching existing order: {ex.Message}");
            }

            return existingOrder;
        }

        // Check and update the orders to OrderStep API if there are any changes
        public static async Task<bool> CheckAndUpdateOrders(string token, List<OrderLine> ordersToPlace, dynamic existingOrder, DateTime targetDate)
        {
            bool anyChanges = false;

            if (existingOrder != null && existingOrder.deleted != true)
            {
                var existingLines = existingOrder.lines;
                foreach (var orderToPlace in ordersToPlace)
                {
                    var matchingExistingLine = ((IEnumerable<dynamic>)existingLines).FirstOrDefault(existingLine =>
                        existingLine.product_id == orderToPlace.ProductId);

                    if (matchingExistingLine != null)
                    {
                        // Assign existing line ID
                        orderToPlace.Id = matchingExistingLine.id;

                        // Assign existing unit prices and discount
                        orderToPlace.UnitSalesPrice = Convert.ToDecimal(matchingExistingLine.unit_sales_price.ToString());
                        orderToPlace.UnitCostPrice = Convert.ToDecimal(matchingExistingLine.unit_cost_price.ToString());
                        orderToPlace.DiscountPer = matchingExistingLine.discount_per != null ? Convert.ToDecimal(matchingExistingLine.discount_per.ToString()) : (decimal?)null;

                        // If the line already exists, check for quantity differences
                        if (Convert.ToDecimal(matchingExistingLine.qty.ToString()) != orderToPlace.Qty)
                        {
                            Console.WriteLine($"Detected change in order for product {orderToPlace.ProductId}. Updating...");
                            anyChanges = true;
                        }
                        else
                        {
                            Console.WriteLine($"No changes detected for product {orderToPlace.ProductId}. Skipping update.");
                        }
                    }
                    else
                    {
                        // New product line to add
                        Console.WriteLine($"New product found: {orderToPlace.ProductId}. Marking for addition.");
                        anyChanges = true;
                    }
                }
            }
            else
            {
                // Existing order is deleted or does not exist
                Console.WriteLine("No existing active order found. A new order will be created.");
                anyChanges = true;
            }

            return anyChanges;
        }

        // Send the orders to OrderStep API (POST new orders or PATCH existing orders)
        public static async Task SendOrderData(string token, List<OrderLine> ordersToPlace, dynamic existingOrder, DateTime targetDate)
        {
            try
            {
                if (existingOrder != null && existingOrder.deleted != true)
                {
                    // Extract existing calendar_event_resource_id if available
                    int? existingCalendarEventResourceId = null;
                    if (existingOrder.calendar_event_resources != null && existingOrder.calendar_event_resources.Count > 0)
                    {
                        existingCalendarEventResourceId = existingOrder.calendar_event_resources[0].id;
                    }

                    // PATCH the existing order with new order lines
                    await PatchOrderData(token, ordersToPlace, (int)existingOrder.id, existingCalendarEventResourceId);
                }
                else
                {
                    // Create a new order as it doesn't exist
                    string referenceValue = $"MealOrder_{targetDate:yyyyMMdd}";
                    await CreateNewOrder(token, ordersToPlace, referenceValue, targetDate);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occurred while sending the request: {ex.Message}");
            }
        }

        // Empty the lines of an existing order
        public static async Task EmptyOrderLines(string token, int orderId, string orderTitle, DateTime targetDate)
        {
            var patchEndpoint = $"https://secure.orderstep.dk/public/api/v1/sale_orders/{orderId}/";

            try
            {
                client.DefaultRequestHeaders.Clear();
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);
                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                // Prepare the payload to empty the lines
                var patchPayload = new
                {
                    lead_customer_id = 307480,
                    @ref = $"MealOrder_{targetDate:yyyyMMdd}",
                    title = $"Emptying Order for {orderTitle}",
                    date = targetDate.ToString("yyyy-MM-dd"),
                    delivery_date = targetDate.ToString("yyyy-MM-dd"),
                    language = "fo",
                    lines = new List<object>() // Empty lines array
                };

                Console.WriteLine($"Patching order {orderId} to remove all lines.");
                Console.WriteLine(JsonConvert.SerializeObject(patchPayload, Formatting.Indented));

                var content = new StringContent(JsonConvert.SerializeObject(patchPayload), Encoding.UTF8, "application/json");

                // Send the PATCH request with retry logic
                HttpResponseMessage response = await SendWithRetryAsync(() => client.PatchAsync(patchEndpoint, content));

                if (response.IsSuccessStatusCode)
                {
                    var responseString = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Successfully emptied order {orderId}: {responseString}");
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Failed to empty order {orderId}. HTTP Status: {response.StatusCode}");
                    Console.WriteLine($"Error Details: {errorContent}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occurred while emptying order {orderId}: {ex.Message}");
            }
        }

        // Create a new order
        public static async Task CreateNewOrder(string token, List<OrderLine> ordersToPlace, string referenceValue, DateTime targetDate)
        {
            var orderEndpoint = "https://secure.orderstep.dk/public/api/v1/sale_orders/";

            try
            {
                client.DefaultRequestHeaders.Clear();
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);
                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                if (ordersToPlace.Count == 0)
                {
                    Console.WriteLine($"No order lines to send for reference {referenceValue}. Exiting...");
                    return;
                }

                // Prepare the order payload for creating a new order
                var orderPayload = new
                {
                    lead_customer_id = 307480, // Atlantic Airways ID
                    @ref = referenceValue,
                    title = $"Meal Order for {targetDate:yyyy-MM-dd}",
                    date = targetDate.ToString("yyyy-MM-dd"),
                    delivery_date = targetDate.ToString("yyyy-MM-dd"),
                    language = "fo", // Faroese
                    lines = ordersToPlace.Select((order, index) => new
                    {
                        pos = index + 1,
                        product_id = order.ProductId,
                        line_text = order.Description, // Already includes flightNumber if needed
                        qty = order.Qty.ToString("F2"), // Ensure qty is formatted correctly
                        unit_sales_price = order.UnitSalesPrice.ToString("F2"),
                        unit_cost_price = order.UnitCostPrice.ToString("F2"),
                        discount_per = order.DiscountPer
                    }).ToList(),
                    calendar_event_resources = new[]
                    {
                        new
                        {
                            title = $"Flogmatur {targetDate:yyyy-MM-dd}",
                            guests = 1,
                            start_datetime = $"{targetDate:yyyy-MM-dd}T08:00:00Z",
                            end_datetime = $"{targetDate:yyyy-MM-dd}T20:00:00Z",
                            calendar_id = 5,
                            calendar_resource_id = 55,
                            calendar_category_id = 21
                        }
                    }
                };

                var jsonPayload = JsonConvert.SerializeObject(orderPayload, Formatting.Indented);
                var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
                Console.WriteLine($"Sending payload to OrderStep API:\n{jsonPayload}");

                // Send the POST request with retry logic
                HttpResponseMessage response = await SendWithRetryAsync(() => client.PostAsync(orderEndpoint, content));

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
                Console.WriteLine($"Exception occurred while creating a new order: {ex.Message}");
            }
        }

        // PATCH the order data to OrderStep API if changes are detected
        public static async Task PatchOrderData(string token, List<OrderLine> orderLines, int orderId, int? existingCalendarEventResourceId)
        {
            var patchEndpoint = $"https://secure.orderstep.dk/public/api/v1/sale_orders/{orderId}/";

            try
            {
                client.DefaultRequestHeaders.Clear();
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);
                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                var targetDate = DateTime.UtcNow.AddDays(7);
                var referenceValue = $"MealOrder_{targetDate:yyyyMMdd}";

                // Prepare the payload for the PATCH request
                var patchPayload = new
                {
                    lead_customer_id = 307480,
                    @ref = referenceValue,
                    title = $"Updated Order {targetDate:yyyy-MM-dd}",
                    date = targetDate.ToString("yyyy-MM-dd"),
                    delivery_date = targetDate.ToString("yyyy-MM-dd"),
                    language = "fo",
                    lines = orderLines.Select(line => new
                    {
                        id = line.Id,
                        product_id = line.ProductId,
                        line_text = line.Description, // Already includes flightNumber if needed
                        qty = line.Qty.ToString("F2"),
                        unit_sales_price = line.UnitSalesPrice.ToString("F2"),
                        unit_cost_price = line.UnitCostPrice.ToString("F2"),
                        discount_per = line.DiscountPer
                    }).ToList(),
                    calendar_event_resources = new[]
                    {
                        new
                        {
                            id = existingCalendarEventResourceId,
                            title = $"Updated Flogmatur {targetDate:yyyy-MM-dd}",
                            guests = 1,
                            start_datetime = $"{targetDate:yyyy-MM-dd}T08:00:00Z",
                            end_datetime = $"{targetDate:yyyy-MM-dd}T20:00:00Z",
                            calendar_id = 5,
                            calendar_resource_id = 55,
                            calendar_category_id = 21
                        }
                    }
                };

                Console.WriteLine("PATCH payload:");
                Console.WriteLine(JsonConvert.SerializeObject(patchPayload, Formatting.Indented));

                var content = new StringContent(JsonConvert.SerializeObject(patchPayload), Encoding.UTF8, "application/json");

                // Send the PATCH request with retry logic
                HttpResponseMessage response = await SendWithRetryAsync(() => client.PatchAsync(patchEndpoint, content));

                if (response.IsSuccessStatusCode)
                {
                    var responseString = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Successfully patched order {orderId}: {responseString}");
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Failed to patch order {orderId}. HTTP Status: {response.StatusCode}");
                    Console.WriteLine($"Error Details: {errorContent}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occurred while patching the order: {ex.Message}");
            }
        }

        // Send HTTP requests with retry logic for handling rate limiting
        public static async Task<HttpResponseMessage> SendWithRetryAsync(Func<Task<HttpResponseMessage>> sendRequest, int maxRetries = 5, int initialDelaySeconds = 1)
        {
            int retryCount = 0;
            int delay = initialDelaySeconds * 1000; // Convert to milliseconds

            while (true)
            {
                HttpResponseMessage response = await sendRequest();

                if (response.StatusCode != (HttpStatusCode)429)
                {
                    // Not a rate limiting error, return the response
                    return response;
                }

                retryCount++;

                if (retryCount > maxRetries)
                {
                    Console.WriteLine("Max retry attempts reached. Failing the request.");
                    return response;
                }

                // Get Retry-After header if present
                int retryAfterSeconds = 1; // Default delay
                if (response.Headers.TryGetValues("Retry-After", out IEnumerable<string> values))
                {
                    string retryAfter = values.FirstOrDefault();
                    if (int.TryParse(retryAfter, out int parsedRetryAfter))
                    {
                        retryAfterSeconds = parsedRetryAfter;
                    }
                }

                Console.WriteLine($"Rate limited. Retrying in {retryAfterSeconds} seconds... (Attempt {retryCount} of {maxRetries})");
                await Task.Delay(retryAfterSeconds * 1000);
                // Optionally implement exponential backoff
                // delay *= 2;
            }
        }
    }
}
