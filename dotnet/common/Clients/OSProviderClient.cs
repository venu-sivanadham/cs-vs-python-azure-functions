using System.Net.Http;
using System.Threading.Tasks;

namespace common.Clients
{
    public class OSProviderClient
    {
        // TODO: Maintain a pool of HttpClient objects for base addresses.
        private static HttpClient _httClient = new HttpClient();

        private readonly string _os_web_endpoint;
        public OSProviderClient(string endpoint)
        {
            _os_web_endpoint = endpoint;
        }

        public async Task<string> GetOSInfoAsync()
        {
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, _os_web_endpoint);
            var response = await _httClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            return await response.Content.ReadAsStringAsync();
        }
    }
}
