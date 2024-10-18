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

    // Represents a single line item in an order (for application logic)
    public class OrderLine
    {
        public int? Id { get; set; }
        public int ProductId { get; set; }
        public decimal Qty { get; set; }
        public string Description { get; set; }
        public string FlightNumber { get; set; } // Added FlightNumber property
        public decimal UnitSalesPrice { get; set; }
        public decimal UnitCostPrice { get; set; }
        public decimal? DiscountPer { get; set; }
        public bool RequiresCalendarEvent { get; set; }
        public int? CalendarEventResourceId { get; set; } // To map to calendar_event_resource_id
    }

    // Represents calendar event resources within an order
    public class CalendarEventResource
    {
        public int id { get; set; }
        public string title { get; set; }
        public int guests { get; set; }
        public string start_datetime { get; set; }
        public string end_datetime { get; set; }
        public int calendar_id { get; set; }
        public int calendar_resource_id { get; set; }
        public int calendar_category_id { get; set; }
    }

    // Represents an order from OrderStep API
    public class Order
    {
        public int id { get; set; }

        [JsonProperty("ref")]
        public string Reference { get; set; }

        [JsonProperty("lines")]
        public List<OrderLineApiModel> Lines { get; set; }

        public bool deleted { get; set; }

        [JsonProperty("calendar_event_resources")]
        public List<CalendarEventResource> CalendarEventResources { get; set; }

        public string delivery_date { get; set; }
        // Add other relevant properties as needed
    }

    // Represents a line item as returned by the API
    public class OrderLineApiModel
    {
        [JsonProperty("id")]
        public int? Id { get; set; }

        [JsonProperty("product_id")]
        public int ProductId { get; set; }

        [JsonProperty("qty")]
        public string Qty { get; set; }

        [JsonProperty("line_text")]
        public string LineText { get; set; }

        [JsonProperty("unit_sales_price")]
        public string UnitSalesPrice { get; set; }

        [JsonProperty("unit_cost_price")]
        public string UnitCostPrice { get; set; }

        [JsonProperty("discount_per")]
        public decimal? DiscountPer { get; set; }

        [JsonProperty("calendar_event_resource_items")]
        public List<CalendarEventResourceItem> CalendarEventResourceItems { get; set; }
    }

    // Represents a calendar event resource item within a line
    public class CalendarEventResourceItem
    {
        [JsonProperty("title")]
        public string Title { get; set; }

        [JsonProperty("start_datetime")]
        public string StartDatetime { get; set; }

        [JsonProperty("end_datetime")]
        public string EndDatetime { get; set; }

        [JsonProperty("calendar_event_resource_id")]
        public int CalendarEventResourceId { get; set; }

        [JsonProperty("hide")]
        public bool Hide { get; set; }
    }

    // Helper class to deserialize the API response for orders
    public class OrderListResponse
    {
        public List<Order> results { get; set; }
        public string next { get; set; }
    }

    // Strongly-typed class for meal orders
    public class MealOrder
    {
        public string FlightNumber { get; set; }
        public DateTime Date { get; set; }
        public string Type { get; set; }
        public string MealType { get; set; }
        public int Quantity { get; set; }
        public string MealDeliveryCode { get; set; }
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

                        // Display meals grouped by flight number for the kitchen
                        DisplayMealsPerFlight(latestMealData);

                        Console.WriteLine("Attempting to retrieve the token...");
                        var token = await GetToken();

                        if (token != null)
                        {
                            Console.WriteLine("Token retrieval successful!");

                            // Dynamically set the target date based on the meal data
                            // Using 'Date' property from MealOrder
                            DateTime targetDate = latestMealData[0].Date;
                            string referenceValue = $"MealOrder_{targetDate:yyyyMMdd}";

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
                                bool anyChanges = await CheckAndUpdateOrders(token, ordersToPlace, existingOrder, targetDate, referenceValue);

                                if (anyChanges)
                                {
                                    Console.WriteLine("Changes detected, processing order updates.");
                                    await SendOrderData(token, ordersToPlace, existingOrder, targetDate, referenceValue);
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

        // Method to display meals grouped by flight number
        public static void DisplayMealsPerFlight(List<MealOrder> mealOrders)
        {
            if (mealOrders == null || mealOrders.Count == 0)
            {
                Console.WriteLine("No meal orders available to display.");
                return;
            }

            var groupedByFlight = mealOrders
                .GroupBy(m => m.FlightNumber)
                .OrderBy(g => g.Key); // Optional: Order flights alphabetically

            decimal totalQuantity = 0m;

            Console.WriteLine("\n=== Meals by Flight Number ===");

            foreach (var flightGroup in groupedByFlight)
            {
                Console.WriteLine($"\nFlight Number: {flightGroup.Key}");

                foreach (var meal in flightGroup)
                {
                    Console.WriteLine($" - {meal.MealType}: {meal.Quantity}");
                    totalQuantity += meal.Quantity;
                }
            }

            Console.WriteLine($"\n=== TOTAL MEALS: {totalQuantity} ===\n");
        }

        // Fetch the latest meal order data from SQL database
        public static List<MealOrder> FetchLatestMealOrderData()
        {
            var connectionString = "Server=aa-sql2;Database=PAX_DATA;Integrated Security=True;";
            var mealOrderDataList = new List<MealOrder>();

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
                HAVING 
                    SUM(Quantity) + CASE WHEN RowNum = 1 THEN SUM(NumberOfExtra) ELSE 0 END > 0
                ORDER BY 
                    2, 1, 3 DESC, 4;
                    ";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var mealOrderData = new MealOrder
                                {
                                    FlightNumber = reader["FlúgviNr"].ToString(),
                                    Date = DateTime.Parse(reader["Dato"].ToString()),
                                    Type = reader["Slag"].ToString(),
                                    MealType = reader["MatarSlag"].ToString(),
                                    Quantity = int.Parse(reader["Antal"].ToString()),
                                    MealDeliveryCode = reader["MealDeliveryCode"].ToString()
                                };

                                // Only add if MealDeliveryCode is not empty and quantity > 0
                                if (!string.IsNullOrWhiteSpace(mealOrderData.MealDeliveryCode) && mealOrderData.Quantity > 0)
                                {
                                    mealOrderDataList.Add(mealOrderData);
                                }
                                else
                                {
                                    Console.WriteLine($"Skipped line with FlightNumber: {mealOrderData.FlightNumber}, MealDeliveryCode: {mealOrderData.MealDeliveryCode}, Quantity: {mealOrderData.Quantity}");
                                }
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
            // **Verify the correct token endpoint from the API documentation**
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
        public static List<OrderLine> CompareAndCalculateOrders(List<MealOrder> mealOrders, List<dynamic> orderstepProducts)
        {
            var ordersToPlace = new List<OrderLine>();

            var groupedMealOrders = mealOrders
                .GroupBy(m => new { m.FlightNumber, m.MealDeliveryCode })
                .Select(g => new
                {
                    FlightNumber = g.Key.FlightNumber,
                    MealDeliveryCode = g.Key.MealDeliveryCode,
                    TotalQuantity = g.Sum(m => m.Quantity),
                    Date = g.First().Date
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

                    // Include Flight Number in the Description without duplicating "(flogmatur)"
                    string descriptionWithFlight = $"{groupedMeal.FlightNumber} - {matchingProduct.name} (flogmatur)";

                    // Determine if this line requires a calendar event
                    // Example: Only 'Crewmatur' (MealDeliveryCode: 207058) requires an event
                    bool requiresEvent = groupedMeal.MealDeliveryCode == "207058";

                    var orderLine = new OrderLine
                    {
                        Id = null,
                        ProductId = productId,
                        Qty = groupedMeal.TotalQuantity,
                        Description = descriptionWithFlight,
                        FlightNumber = groupedMeal.FlightNumber, // Assigning FlightNumber
                        UnitSalesPrice = unitSalesPrice,
                        UnitCostPrice = unitCostPrice,
                        DiscountPer = null,
                        RequiresCalendarEvent = requiresEvent
                    };

                    ordersToPlace.Add(orderLine);
                }
                else
                {
                    Console.WriteLine($"No matching product found for MealDeliveryCode: {groupedMeal.MealDeliveryCode}");
                }
            }

            // Final filtering to ensure no lines with Qty <= 0 are added (safety net)
            ordersToPlace = ordersToPlace.Where(o => o.Qty > 0).ToList();

            // Log the orders to place for debugging
            Console.WriteLine("Orders to Place:");
            foreach (var order in ordersToPlace)
            {
                Console.WriteLine(JsonConvert.SerializeObject(order));
            }

            return ordersToPlace;
        }

        // Fetch existing order data from OrderStep API
        public static async Task<Order> FetchExistingOrderData(string token, DateTime targetDate)
        {
            var orderEndpoint = "https://secure.orderstep.dk/public/api/v1/sale_orders/";
            Order existingOrder = null;

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
                    var jsonResponse = JsonConvert.DeserializeObject<OrderListResponse>(responseString);

                    // Iterate through results to find an order with the exact matching ref
                    foreach (var order in jsonResponse.results)
                    {
                        if (!string.IsNullOrEmpty(order.Reference) && order.Reference == referenceValue)
                        {
                            existingOrder = order;
                            Console.WriteLine("Existing order found:");
                            Console.WriteLine(JsonConvert.SerializeObject(existingOrder, Formatting.Indented));

                            break;
                        }
                    }

                    if (existingOrder == null)
                    {
                        Console.WriteLine("No existing order found with the specified reference.");
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
        public static async Task<bool> CheckAndUpdateOrders(string token, List<OrderLine> ordersToPlace, Order existingOrder, DateTime targetDate, string referenceValue)
        {
            bool anyChanges = false;

            if (existingOrder != null && existingOrder.deleted != true)
            {
                var existingLines = existingOrder.Lines ?? new List<OrderLineApiModel>();
                var existingCalendarEventResources = existingOrder.CalendarEventResources ?? new List<CalendarEventResource>();

                // Since we're using only one calendar_event_resource, get its ID
                int? existingCalendarEventResourceId = existingCalendarEventResources.FirstOrDefault()?.id;

                foreach (var orderToPlace in ordersToPlace)
                {
                    var flightNumber = orderToPlace.FlightNumber;
                    var matchingExistingLine = existingLines.FirstOrDefault(existingLine =>
                        existingLine.ProductId == orderToPlace.ProductId &&
                        existingLine.LineText.StartsWith(flightNumber, StringComparison.OrdinalIgnoreCase));

                    if (matchingExistingLine != null)
                    {
                        // Assign existing line ID
                        orderToPlace.Id = matchingExistingLine.Id;

                        // Assign existing unit prices and discount
                        if (decimal.TryParse(matchingExistingLine.UnitSalesPrice, out decimal existingUnitSalesPrice))
                            orderToPlace.UnitSalesPrice = existingUnitSalesPrice;

                        if (decimal.TryParse(matchingExistingLine.UnitCostPrice, out decimal existingUnitCostPrice))
                            orderToPlace.UnitCostPrice = existingUnitCostPrice;

                        orderToPlace.DiscountPer = matchingExistingLine.DiscountPer;

                        // If the line already exists, check for quantity differences
                        if (decimal.TryParse(matchingExistingLine.Qty, out decimal existingQty))
                        {
                            if (existingQty != orderToPlace.Qty)
                            {
                                Console.WriteLine($"Detected change in order for product {orderToPlace.ProductId} on flight {flightNumber}. Updating...");
                                anyChanges = true;
                            }
                            else
                            {
                                Console.WriteLine($"No changes detected for product {orderToPlace.ProductId} on flight {flightNumber}. Skipping update.");
                            }
                        }

                        // Assign the existing calendar_event_resource_id if required
                        if (orderToPlace.RequiresCalendarEvent)
                        {
                            if (existingCalendarEventResourceId.HasValue)
                            {
                                orderToPlace.CalendarEventResourceId = existingCalendarEventResourceId.Value;
                            }
                            else
                            {
                                Console.WriteLine($"No calendar_event_resource_id found for FlightNumber: {flightNumber}. Will need to create a new one.");
                                anyChanges = true; // Mark as changed to handle resource creation
                            }
                        }
                    }
                    else
                    {
                        // New product line to add
                        Console.WriteLine($"New product found: {orderToPlace.ProductId} for flight {orderToPlace.FlightNumber}. Marking for addition.");
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
        public static async Task SendOrderData(string token, List<OrderLine> ordersToPlace, Order existingOrder, DateTime targetDate, string referenceValue)
        {
            try
            {
                if (existingOrder != null && existingOrder.deleted != true)
                {
                    // Extract existing calendar_event_resource_id if available
                    List<int> existingCalendarEventResourceIds = new List<int>();
                    if (existingOrder.CalendarEventResources != null && existingOrder.CalendarEventResources.Count > 0)
                    {
                        existingCalendarEventResourceIds = existingOrder.CalendarEventResources.Select(c => c.id).ToList();
                    }

                    // Determine the original reference and date
                    string originalReference = existingOrder.Reference;
                    DateTime originalDate = DateTime.Parse(existingOrder.delivery_date);

                    // PATCH the existing order with new order lines
                    await PatchOrderData(token, ordersToPlace, existingOrder.id, existingCalendarEventResourceIds, originalDate, originalReference);
                }
                else
                {
                    // Create a new order as it doesn't exist
                    await CreateNewOrder(token, ordersToPlace, referenceValue, targetDate);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occurred while sending the request: {ex.Message}");
            }
        }

        // Create a new order (Single method; ensure no duplicates)
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

                // Check if any order lines require a calendar event
                bool anyLineRequiresCalendarEvent = ordersToPlace.Any(o => o.RequiresCalendarEvent);

                // Prepare a single calendar_event_resource if needed
                object calendarEventResourcePayload = anyLineRequiresCalendarEvent
                    ? new List<object>
                    {
                        new
                        {
                            title = $"Flight Meals for {targetDate:yyyy-MM-dd}",
                            guests = 5, // Example value, adjust as needed
                            start_datetime = $"{targetDate:yyyy-MM-dd}T07:00:00Z", // Example start time
                            end_datetime = $"{targetDate:yyyy-MM-dd}T21:00:00Z", // Example end time
                            calendar_id = 5, // Example value, adjust as needed
                            calendar_resource_id = 55, // Example value, adjust as needed
                            calendar_category_id = 21 // Example value, adjust as needed
                        }
                    }
                    : new List<object>(); // Empty list if no lines require calendar events

                // Prepare the order payload
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
                        line_text = order.Description,
                        qty = order.Qty.ToString("F2"),
                        unit_sales_price = order.UnitSalesPrice.ToString("F2"),
                        unit_cost_price = order.UnitCostPrice.ToString("F2"),
                        discount_per = order.DiscountPer,
                        // Initialize calendar_event_resource_items as empty; will populate after PATCH
                        calendar_event_resource_items = new List<object>()
                    }).ToList(),

                    // Add the single calendar_event_resource if any line requires it
                    calendar_event_resources = calendarEventResourcePayload
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

                    // Retrieve the created order to get calendar_event_resource_id
                    var createdOrder = await FetchNewlyCreatedOrder(token, referenceValue);
                    if (createdOrder != null)
                    {
                        // Extract the single calendar_event_resource_id
                        int? calendarEventResourceId = createdOrder.CalendarEventResources.FirstOrDefault()?.id;

                        if (calendarEventResourceId.HasValue)
                        {
                            // Assign the same calendar_event_resource_id to all lines that require it
                            foreach (var orderLine in ordersToPlace.Where(o => o.RequiresCalendarEvent))
                            {
                                orderLine.CalendarEventResourceId = calendarEventResourceId.Value;
                            }
                        }
                        else
                        {
                            Console.WriteLine("No calendar_event_resource_id found in the created order.");
                        }

                        // Map the created order's lines to application OrderLines to assign 'id's
                        foreach (var createdLine in createdOrder.Lines)
                        {
                            var matchingOrderLine = ordersToPlace.FirstOrDefault(o =>
                                o.ProductId == createdLine.ProductId &&
                                o.Description == createdLine.LineText);

                            if (matchingOrderLine != null)
                            {
                                matchingOrderLine.Id = createdLine.Id;
                            }
                        }

                        // Now, PATCH the order with the retrieved calendar_event_resource_id and line 'id's
                        await PatchOrderData(token, ordersToPlace, createdOrder.id, new List<int> { calendarEventResourceId.Value }, targetDate, referenceValue);
                    }
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

        // Fetch the newly created order using its reference value
        public static async Task<Order> FetchNewlyCreatedOrder(string token, string referenceValue)
        {
            var orderEndpoint = "https://secure.orderstep.dk/public/api/v1/sale_orders/";
            Order newOrder = null;

            try
            {
                client.DefaultRequestHeaders.Clear();
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);

                var requestUrl = $"{orderEndpoint}?ref={Uri.EscapeDataString(referenceValue)}";

                HttpResponseMessage response = await SendWithRetryAsync(() => client.GetAsync(requestUrl));

                if (response.IsSuccessStatusCode)
                {
                    var responseString = await response.Content.ReadAsStringAsync();
                    var jsonResponse = JsonConvert.DeserializeObject<OrderListResponse>(responseString);

                    // Iterate through results to find an order with the exact matching ref
                    foreach (var order in jsonResponse.results)
                    {
                        if (!string.IsNullOrEmpty(order.Reference) && order.Reference == referenceValue)
                        {
                            newOrder = order;
                            Console.WriteLine("Newly created order found:");
                            Console.WriteLine(JsonConvert.SerializeObject(newOrder, Formatting.Indented));
                            break;
                        }
                    }

                    if (newOrder == null)
                    {
                        Console.WriteLine("No newly created order found with the specified reference.");
                    }
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Failed to fetch newly created order. HTTP Status: {response.StatusCode}");
                    Console.WriteLine($"Error Details: {errorContent}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occurred while fetching newly created order: {ex.Message}");
            }

            return newOrder;
        }

        // PATCH the order data to OrderStep API if changes are detected
        public static async Task PatchOrderData(string token, List<OrderLine> orderLines, int orderId, List<int> calendarEventResourceIds, DateTime targetDate, string referenceValue)
        {
            var patchEndpoint = $"https://secure.orderstep.dk/public/api/v1/sale_orders/{orderId}/";

            try
            {
                client.DefaultRequestHeaders.Clear();
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);
                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                // Since we are using only one calendar_event_resource, ensure only one ID exists
                int? calendarEventResourceId = calendarEventResourceIds.FirstOrDefault();

                if (!calendarEventResourceId.HasValue)
                {
                    Console.WriteLine("No calendar_event_resource_id available for assignment.");
                    return;
                }

                Console.WriteLine($"Assigning Calendar Event Resource ID: {calendarEventResourceId.Value} to all lines.");

                // Extract unique flight numbers
                var flightNumbers = orderLines.Select(o => o.FlightNumber).Distinct().ToList();
                string flightsList = string.Join(", ", flightNumbers);

                // Prepare the payload for the PATCH request using original targetDate and referenceValue
                var patchPayload = new
                {
                    lead_customer_id = 307480,
                    @ref = referenceValue, // Use original reference
                    title = $"Flight Meals for {targetDate:yyyy-MM-dd} - Flights: {flightsList}",
                    date = targetDate.ToString("yyyy-MM-dd"),
                    delivery_date = targetDate.ToString("yyyy-MM-dd"),
                    language = "fo",
                    currency_code = "DKK", // **Added currency_code here**

                    lines = orderLines.Select(line => new
                    {
                        id = line.Id,
                        product_id = line.ProductId,
                        line_text = line.Description, // Includes Flight Number
                        qty = line.Qty.ToString("F2"),
                        unit_sales_price = line.UnitSalesPrice.ToString("F2"),
                        unit_cost_price = line.UnitCostPrice.ToString("F2"),
                        discount_per = line.DiscountPer,
                        // **Unconditionally assign calender_event_resource_items to all lines with flight number in title**
                        calender_event_resource_items = new List<object>
                {
                    new
                    {
                        title = $"Flight Meals for {targetDate:yyyy-MM-dd} - Flight {line.FlightNumber}",
                        start_datetime = $"{targetDate:yyyy-MM-dd}T08:00:00Z",
                        end_datetime = $"{targetDate:yyyy-MM-dd}T20:00:00Z",
                        calendar_event_resource_id = calendarEventResourceId.Value, // Assign the single ID
                        hide = false
                    }
                }
                    })
                    .Where(line => line.id.HasValue) // Ensure 'id' is not null
                    .ToList(),

                    // Add the single calendar_event_resource
                    calendar_event_resources = new List<object>
            {
                new
                {
                    id = calendarEventResourceId.Value, // Ensure this is the existing id
                    title = $"Flight Meals for {targetDate:yyyy-MM-dd} - Flights: {flightsList}",
                    guests = 5, // Example value, adjust as needed
                    start_datetime = $"{targetDate:yyyy-MM-dd}T07:00:00Z", // Example start time
                    end_datetime = $"{targetDate:yyyy-MM-dd}T21:00:00Z", // Example end time
                    calendar_id = 5, // Example value, adjust as needed
                    calendar_resource_id = 55, // Example value, adjust as needed
                    calendar_category_id = 21 // Example value, adjust as needed
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
                int retryAfterSeconds = initialDelaySeconds; // Default delay
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
            }
        }
    }
}
