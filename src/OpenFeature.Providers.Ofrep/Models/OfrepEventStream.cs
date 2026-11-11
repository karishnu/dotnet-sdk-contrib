using System.Text.Json.Serialization;

namespace OpenFeature.Providers.Ofrep.Models;

internal sealed class OfrepEventStream
{
    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("url")]
    public string? Url { get; set; }

    [JsonPropertyName("endpoint")]
    public OfrepEventEndpoint? Endpoint { get; set; }

    [JsonPropertyName("inactivityDelaySec")]
    public int? InactivityDelaySec { get; set; }
}

internal sealed class OfrepEventEndpoint
{
    [JsonPropertyName("origin")]
    public string? Origin { get; set; }

    [JsonPropertyName("requestUri")]
    public string? RequestUri { get; set; }
}
