using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureAppConfiguration(config =>
        {
            config.AddEnvironmentVariables();
        })
    .ConfigureFunctionsWorkerDefaults()
    .Build();

host.Run();
