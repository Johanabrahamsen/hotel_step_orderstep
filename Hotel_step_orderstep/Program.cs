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

            if (!string.IsNullOrEmpty(latestMealData))
            {
                Console.WriteLine("Fetched latest meal data:");
                Console.WriteLine(latestMealData);

                // Get Token for API
                Console.WriteLine("Attempting to retrieve the token...");
                var token = await GetToken();

                if (token != null)
                {
                    Console.WriteLine("Token retrieval successful!");
                    Console.WriteLine($"Access Token: {token}");

                    // Retrieve lead/customer list
                    Console.WriteLine("Attempting to retrieve the list of customers or leads...");
                    await GetLeadsAndCustomers(token);

                    // Retrieve products list
                    Console.WriteLine("Attempting to retrieve the product list...");
                    await GetAtlanticAirwaysProducts(token);

                    // Prepare and send the order
                    //Console.WriteLine("Sending order to Orderstep API...");
                    //await SendOrderData(token, latestMealData);
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
    public static string FetchLatestMealOrderData()
    {
        var connectionString = "Server=aa-sql2;Database=PAX_DATA;Integrated Security=True;";
        var mealOrderDataList = new List<object>();

        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            try
            {
                Console.WriteLine("Preparing to connect to SQL Server...");
                connection.Open();
                Console.WriteLine("Connected to SQL Server successfully.");

                string query = @"
                SELECT d.flight_no as FlúgviNr, CAST(d.std as date) as Dato,
                CASE WHEN mbmt.OrderType=1 THEN 'Søla'
                     WHEN mbmt.OrderType=3 THEN 'Prepaid'
                     WHEN mbmt.OrderType=5 THEN 'Crew'
                     WHEN mbmt.OrderType=6 THEN 'Ekstra' END as Slag,
                mt.Name as MatarSlag, mbmt.Quantity as Antal
                FROM meal.Flight f
                INNER JOIN meal.FlightNr fn ON f.FlightNrId = fn.FlightNrId
                INNER JOIN meal.MealBooking mb ON mb.FlightId = f.FlightId
                INNER JOIN (
                    SELECT mb1.FlightId, MAX(mb1.mealbookingid) as MealBookingId
                    FROM meal.MealBooking mb1
                    GROUP BY mb1.FlightId) a ON a.FlightId = f.FlightId
                INNER JOIN meal.MealBookingMealType mbmt ON mbmt.MealBookingId = mb.MealBookingId
                INNER JOIN DEPARTURES d ON d.id = f.DepartureId
                INNER JOIN meal.MealType mt ON mbmt.MealTypeId = mt.MealTypeId
                WHERE CAST(f.FlightDate as date) = CAST(GETDATE() as date)
                AND mb.MealBookingId = a.MealBookingId
                ORDER BY 2, 1, 3 DESC, 4";

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
                                quantity = reader["Antal"].ToString()
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

        return JsonConvert.SerializeObject(mealOrderDataList);
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
        var customerEndpoint = "https://secure.orderstep.dk/public/api/v1/leads_customers/";

        try
        {
            client.DefaultRequestHeaders.Clear();
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
            client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);

            HttpResponseMessage response = await client.GetAsync(customerEndpoint);

            if (response.IsSuccessStatusCode)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                dynamic jsonResponse = JsonConvert.DeserializeObject(responseString);

                Console.WriteLine("Retrieved lead/customer list:");
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

    // Step 4: Retrieve products list from the API
    public static async Task GetAtlanticAirwaysProducts(string token)
    {
        var productsEndpoint = "https://secure.orderstep.dk/public/api/v1/products/";
        string nextPageUrl = productsEndpoint;

        try
        {
            client.DefaultRequestHeaders.Clear();
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
            client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);

            while (!string.IsNullOrEmpty(nextPageUrl))
            {
                HttpResponseMessage response = await client.GetAsync(nextPageUrl);

                if (response.IsSuccessStatusCode)
                {
                    var responseString = await response.Content.ReadAsStringAsync();
                    dynamic jsonResponse = JsonConvert.DeserializeObject(responseString);

                    Console.WriteLine("Retrieved Atlantic Airways product list:");

                    nextPageUrl = jsonResponse.next;
                }
                else if ((int)response.StatusCode == 429)
                {
                    var retryAfter = response.Headers.RetryAfter?.Delta?.TotalSeconds ?? 1;
                    Console.WriteLine($"Rate limit hit. Retrying in {retryAfter} seconds...");
                    await Task.Delay(TimeSpan.FromSeconds(retryAfter));
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
    }

    // Step 5: Send the fetched meal order data to OrderStep API
    //public static async Task SendOrderData(string token, string latestMealData)
    //{
    //    var orderEndpoint = "https://secure.orderstep.dk/public/api/v1/sale_orders/";

    //    try
    //    {
    //        client.DefaultRequestHeaders.Clear();
    //        client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
    //        client.DefaultRequestHeaders.Add("X-ORGANIZATION-ID", organizationId);

    //        var mealData = JsonConvert.DeserializeObject<dynamic>(latestMealData);
    //        string orderDate = mealData[0].date;
    //        string title = $"Flogmatur {orderDate}";

    //        var jsonPayload = new
    //        {
    //            lead_customer_id = 307480,
    //            title = title,
    //            language = "fo",
    //            date = orderDate,
    //            delivery_date = orderDate,
    //            lines = new[]
    //            {
    //                new
    //                {
    //                    product_id = 207056, // Product ID from the retrieved products list
    //                    qty = 5,
    //                }
    //            },
    //            calendar_event_resources = new[]
    //            {
    //                new
    //                {
    //                    title = title,
    //                    guests = 1,
    //                    start_datetime = $"{orderDate}T08:00:00Z",
    //                    end_datetime = $"{orderDate}T20:00:00Z",
    //                    calendar_id = 5,
    //                    calendar_resource_id = 55,
    //                    calendar_category_id = 21
    //                }
    //            }
    //        };

    //        var content = new StringContent(JsonConvert.SerializeObject(jsonPayload), Encoding.UTF8, "application/json");

    //        HttpResponseMessage response = await client.PostAsync(orderEndpoint, content);

    //        if (response.IsSuccessStatusCode)
    //        {
    //            var responseString = await response.Content.ReadAsStringAsync();
    //            Console.WriteLine($"Data successfully sent to OrderStep API: {responseString}");
    //        }
    //        else
    //        {
    //            var errorContent = await response.Content.ReadAsStringAsync();
    //            Console.WriteLine($"Failed to send data to OrderStep API. HTTP Status: {response.StatusCode}");
    //            Console.WriteLine($"Error Details: {errorContent}");
    //        }
    //    }
    //    catch (Exception ex)
    //    {
    //        Console.WriteLine($"Exception occurred while sending the request: {ex.Message}");
    //    }
    //}
}
