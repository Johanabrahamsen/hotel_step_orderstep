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

        // New property to store the specific date for each order line
        public DateTime Date { get; set; }
    }

    // Represents an order from OrderStep API
    public class Order
    {
        public int id { get; set; }
        public string @ref { get; set; }
        public List<OrderLine> lines { get; set; }
        public bool deleted { get; set; }
        public List<CalendarEventResource> calendar_event_resources { get; set; }

    }

    // Represents calendar event resources within an order
    public class CalendarEventResource
    {
        public int? id { get; set; } // Made nullable
        public string title { get; set; }
        public int guests { get; set; }
        public string start_datetime { get; set; }
        public string end_datetime { get; set; }
        public int calendar_id { get; set; }
        public int calendar_resource_id { get; set; }
        public int calendar_category_id { get; set; }
    }


    // Helper class to deserialize the API response for orders
    public class OrderListResponse
    {
        public List<Order> results { get; set; }
        public string next { get; set; }
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

                            // Dynamically set the target dates based on the meal data
                            var groupedByDate = latestMealData.GroupBy(m => DateTime.Parse(m.date.ToString()))
                                                            .ToDictionary(g => g.Key, g => g.ToList());

                            // Retrieve products from OrderStep API
                            var orderstepProducts = await GetAtlanticAirwaysProducts(token);

                            foreach (var kvp in groupedByDate)
                            {
                                DateTime targetDate = kvp.Key;
                                var ordersForDate = CompareAndCalculateOrders(kvp.Value, orderstepProducts, targetDate);

                                if (ordersForDate.Count > 0)
                                {
                                    // Fetch existing order for the target date for comparison
                                    Console.WriteLine($"Fetching existing order for {targetDate:yyyy-MM-dd} from OrderStep API...");
                                    var existingOrder = await FetchExistingOrderData(token, targetDate);

                                    // Check and update orders based on the comparison
                                    bool anyChanges = await CheckAndUpdateOrders(token, ordersForDate, existingOrder, targetDate);

                                    if (anyChanges)
                                    {
                                        Console.WriteLine("Changes detected, processing order updates.");
                                        await SendOrderData(token, ordersForDate, existingOrder, targetDate);
                                    }
                                    else
                                    {
                                        Console.WriteLine("No changes detected. Skipping sending new orders.");
                                    }

                                    Console.WriteLine("Order data processed and updated successfully.");
                                }
                                else
                                {
                                    Console.WriteLine($"No valid orders to place for {targetDate:yyyy-MM-dd} after comparison.");
                                }
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
        public static void DisplayMealsPerFlight(List<dynamic> mealOrders)
        {
            if (mealOrders == null || mealOrders.Count == 0)
            {
                Console.WriteLine("No meal orders available to display.");
                return;
            }

            var groupedByFlight = mealOrders
                .Where(m => decimal.TryParse(m.quantity.ToString(), out decimal qty) && qty > 0) // Filter out meals with zero quantity
                .GroupBy(m => m.flightNumber)
                .OrderBy(g => g.Key); // Optional: Order flights alphabetically

            decimal totalQuantity = 0m;

            Console.WriteLine("\n=== Meals by Flight Number ===");

            foreach (var flightGroup in groupedByFlight)
            {
                Console.WriteLine($"\nFlight Number: {flightGroup.Key}");

                foreach (var meal in flightGroup)
                {
                    // Safely parse quantity
                    if (decimal.TryParse(meal.quantity.ToString(), out decimal qty))
                    {
                        Console.WriteLine($" - {meal.mealType}: {qty}");
                        totalQuantity += qty;
                    }
                    else
                    {
                        Console.WriteLine($" - {meal.mealType}: Invalid quantity");
                    }
                }
            }

            Console.WriteLine($"\n=== TOTAL MEALS: {totalQuantity} ===\n");
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
        public static List<OrderLine> CompareAndCalculateOrders(List<dynamic> mealOrders, List<dynamic> orderstepProducts, DateTime targetDate)
        {
            var ordersToPlace = new List<OrderLine>();

            var groupedMealOrders = mealOrders
                .GroupBy(m => new { FlightNumber = m.flightNumber, m.MealDeliveryCode })
                .Select(g => new
                {
                    FlightNumber = g.Key.FlightNumber,
                    MealDeliveryCode = g.Key.MealDeliveryCode,
                    TotalQuantity = g.Sum(m => int.Parse(m.quantity)),
                    Date = DateTime.Parse(g.First().date.ToString())
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

                    // Include Flight Number in the Description
                    string descriptionWithFlight = $"{groupedMeal.FlightNumber} - {matchingProduct.name}";

                    var orderLine = new OrderLine
                    {
                        Id = null,
                        ProductId = productId,
                        Qty = groupedMeal.TotalQuantity,
                        Description = descriptionWithFlight,
                        UnitSalesPrice = unitSalesPrice,
                        UnitCostPrice = unitCostPrice,
                        DiscountPer = null,
                        Date = groupedMeal.Date // Assign the specific date
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
                        if (!string.IsNullOrEmpty(order.@ref) && order.@ref == referenceValue)
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
        public static async Task<bool> CheckAndUpdateOrders(string token, List<OrderLine> ordersToPlace, Order existingOrder, DateTime targetDate)
        {
            bool anyChanges = false;

            if (existingOrder != null && existingOrder.deleted != true)
            {
                var existingLines = existingOrder.lines ?? new List<OrderLine>();
                foreach (var orderToPlace in ordersToPlace)
                {
                    var matchingExistingLine = existingLines.FirstOrDefault(existingLine =>
                        existingLine.ProductId == orderToPlace.ProductId &&
                        existingLine.Description.StartsWith(orderToPlace.Description.Split('-')[0].Trim())); // Match based on Flight Number

                    if (matchingExistingLine != null)
                    {
                        // Assign existing line ID
                        orderToPlace.Id = matchingExistingLine.Id;

                        // Assign existing unit prices and discount
                        orderToPlace.UnitSalesPrice = matchingExistingLine.UnitSalesPrice;
                        orderToPlace.UnitCostPrice = matchingExistingLine.UnitCostPrice;
                        orderToPlace.DiscountPer = matchingExistingLine.DiscountPer;

                        // If the line already exists, check for quantity differences
                        if (matchingExistingLine.Qty != orderToPlace.Qty)
                        {
                            Console.WriteLine($"Detected change in order for product {orderToPlace.ProductId} on flight {orderToPlace.Description.Split('-')[0].Trim()}. Updating...");
                            anyChanges = true;
                        }
                        else
                        {
                            Console.WriteLine($"No changes detected for product {orderToPlace.ProductId} on flight {orderToPlace.Description.Split('-')[0].Trim()}. Skipping update.");
                        }
                    }
                    else
                    {
                        // New product line to add
                        Console.WriteLine($"New product found: {orderToPlace.ProductId} for flight {orderToPlace.Description.Split('-')[0].Trim()}. Marking for addition.");
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
        public static async Task SendOrderData(string token, List<OrderLine> ordersToPlace, Order existingOrder, DateTime targetDate)
        {
            try
            {
                if (existingOrder != null && existingOrder.deleted != true)
                {
                    // Extract existing calendar_event_resource_ids
                    var existingCalendarEvents = existingOrder.calendar_event_resources ?? new List<CalendarEventResource>();

                    // Prepare new calendar events from order lines
                    var newCalendarEvents = ordersToPlace.Select(orderLine => new CalendarEventResource
                    {
                        title = $"Flogmatur {orderLine.Date:yyyy-MM-dd} - {orderLine.Description}",
                        guests = 1,
                        start_datetime = $"{orderLine.Date:yyyy-MM-dd}T08:00:00Z",
                        end_datetime = $"{orderLine.Date:yyyy-MM-dd}T20:00:00Z",
                        calendar_id = 5,
                        calendar_resource_id = 55,
                        calendar_category_id = 21
                        // 'id' remains null for new events
                    }).ToList();

                    // Merge existing and new calendar events
                    var mergedCalendarEvents = existingCalendarEvents.Select(e => new CalendarEventResource
                    {
                        id = e.id,
                        title = e.title,
                        guests = e.guests,
                        start_datetime = e.start_datetime,
                        end_datetime = e.end_datetime,
                        calendar_id = e.calendar_id,
                        calendar_resource_id = e.calendar_resource_id,
                        calendar_category_id = e.calendar_category_id
                    }).ToList();

                    // Add new calendar events without 'id' (they will be created)
                    mergedCalendarEvents.AddRange(newCalendarEvents.Select(e => new CalendarEventResource
                    {
                        id = null, // No 'id' for new events
                        title = e.title,
                        guests = e.guests,
                        start_datetime = e.start_datetime,
                        end_datetime = e.end_datetime,
                        calendar_id = e.calendar_id,
                        calendar_resource_id = e.calendar_resource_id,
                        calendar_category_id = e.calendar_category_id
                    }));

                    // PATCH the existing order with updated lines and merged calendar events
                    var calendarEventsAsObjects = mergedCalendarEvents.Cast<object>().ToList();
                    await PatchOrderData(token, ordersToPlace, existingOrder.id, calendarEventsAsObjects);
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
                    calendar_event_resources = new List<object>
                    {
                        // Main order calendar event
                        new
                        {
                            title = $"Weekly Meal Schedule for {targetDate:yyyy-MM-dd}",
                            guests = 0,
                            start_datetime = $"{targetDate:yyyy-MM-dd}T00:00:00Z",
                            end_datetime = $"{targetDate:yyyy-MM-dd}T23:59:59Z",
                            calendar_id = 5,
                            calendar_resource_id = 55,
                            calendar_category_id = 21
                        }
                    }
                };

                // Add individual calendar events for each OrderLine
                foreach (var orderLine in ordersToPlace)
                {
                    orderPayload.calendar_event_resources.Add(new
                    {
                        title = $"Flogmatur {orderLine.Date:yyyy-MM-dd} - {orderLine.Description}",
                        guests = 1,
                        start_datetime = $"{orderLine.Date:yyyy-MM-dd}T08:00:00Z",
                        end_datetime = $"{orderLine.Date:yyyy-MM-dd}T20:00:00Z",
                        calendar_id = 5,
                        calendar_resource_id = 55,
                        calendar_category_id = 21
                    });
                }

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
        public static async Task PatchOrderData(string token, List<OrderLine> orderLines, int orderId, List<object> existingCalendarEvents)
        {
            var patchEndpoint = $"https://secure.orderstep.dk/public/api/v1/sale_orders/{orderId}/";

            try
            {
                client.DefaultRequestHeaders.Clear();
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);
                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                // Prepare the payload for the PATCH request
                var patchPayload = new
                {
                    // Keep the existing @ref without modification
                    @ref = $"MealOrder_{DateTime.UtcNow:yyyyMMdd}", // Ensure @ref remains consistent if required

                    // It's unclear whether 'date' and 'delivery_date' should be updated or kept as-is
                    // Assuming they represent the week, we'll keep them as-is or set to targetDate

                    // Adjust 'date' and 'delivery_date' as per your requirements
                    // Here, we're setting them to the targetDate, but you might need to adjust accordingly
                    date = DateTime.UtcNow.ToString("yyyy-MM-dd"),
                    delivery_date = DateTime.UtcNow.ToString("yyyy-MM-dd"),
                    language = "fo",
                    lines = orderLines.Select(line => new
                    {
                        id = line.Id,
                        product_id = line.ProductId,
                        line_text = line.Description, // Includes Flight Number
                        qty = line.Qty.ToString("F2"),
                        unit_sales_price = line.UnitSalesPrice.ToString("F2"),
                        unit_cost_price = line.UnitCostPrice.ToString("F2"),
                        discount_per = line.DiscountPer
                    }).ToList(),
                    calendar_event_resources = existingCalendarEvents.Concat(orderLines.Select(orderLine => new
                    {
                        title = $"Flogmatur {orderLine.Date:yyyy-MM-dd} - {orderLine.Description}",
                        guests = 1,
                        start_datetime = $"{orderLine.Date:yyyy-MM-dd}T08:00:00Z",
                        end_datetime = $"{orderLine.Date:yyyy-MM-dd}T20:00:00Z",
                        calendar_id = 5,
                        calendar_resource_id = 55,
                        calendar_category_id = 21
                    })).ToArray()
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
