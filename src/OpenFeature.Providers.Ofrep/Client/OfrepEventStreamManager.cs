using System.Text;
#if NETFRAMEWORK
using System.Net.Http;
#endif
using System.Text.Json;
using Microsoft.Extensions.Logging;
using OpenFeature.Providers.Ofrep.Models;

namespace OpenFeature.Providers.Ofrep.Client;

internal sealed class OfrepEventStreamManager : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly TimeProvider _timeProvider;
    private readonly Action _onChange;
    private readonly ILogger _logger;
    private readonly TimeSpan? _inactivityOverride;
    private readonly Dictionary<string, CancellationTokenSource> _connections = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    private ITimer? _inactivityTimer;
    private bool _disposed;

    public OfrepEventStreamManager(HttpClient httpClient, TimeProvider timeProvider, Action onChange, ILogger logger,
        TimeSpan? inactivityOverride = null)
    {
        if (inactivityOverride <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(inactivityOverride));
        }
        this._httpClient = httpClient;
        this._timeProvider = timeProvider;
        this._onChange = onChange;
        this._logger = logger;
        this._inactivityOverride = inactivityOverride;
    }

    public void Update(List<OfrepEventStream>? entries)
    {
        var connections = new Dictionary<string, Uri>(StringComparer.Ordinal);
        TimeSpan? delay = this._inactivityOverride;
        if (entries != null)
        {
            foreach (var entry in entries)
            {
                if (entry.Type != "sse" || !TryResolve(entry, this._httpClient.BaseAddress, out var uri) ||
                    (this._httpClient.DefaultRequestHeaders.Any() &&
                     uri.GetLeftPart(UriPartial.Authority) != this._httpClient.BaseAddress?.GetLeftPart(UriPartial.Authority)))
                {
                    continue;
                }

                connections[uri.AbsoluteUri] = uri;
                if (!this._inactivityOverride.HasValue)
                {
                    var responseDelay = TimeSpan.FromSeconds(entry.InactivityDelaySec >= 1 ? entry.InactivityDelaySec.Value : 120);
                    if (delay == null || responseDelay < delay) delay = responseDelay;
                }
            }
        }

        lock (this._gate)
        {
            if (this._disposed) return;
            foreach (var key in this._connections.Keys.Except(connections.Keys).ToList())
            {
                this._connections[key].Cancel();
                this._connections.Remove(key);
            }

            foreach (var connection in connections)
            {
                if (this._connections.ContainsKey(connection.Key)) continue;
                var cancellation = new CancellationTokenSource();
                this._connections.Add(connection.Key, cancellation);
                _ = Task.Run(() => this.RunAsync(connection.Value, cancellation));
            }

            this._inactivityTimer?.Dispose();
            this._inactivityTimer = this._connections.Count == 0 ? null :
                this._timeProvider.CreateTimer(_ => this.OnInactive(), null, delay!.Value, Timeout.InfiniteTimeSpan);
        }
    }

    private void OnInactive()
    {
        lock (this._gate)
        {
            foreach (var connection in this._connections.Values) connection.Cancel();
            this._connections.Clear();
            this._inactivityTimer?.Dispose();
            this._inactivityTimer = null;
        }
    }

    private async Task RunAsync(Uri uri, CancellationTokenSource cancellation)
    {
        var token = cancellation.Token;
        var delay = TimeSpan.FromSeconds(1);
        string? lastEventId = null;
        try
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get, uri);
                    request.Headers.Accept.ParseAdd("text/event-stream");
                    if (!string.IsNullOrEmpty(lastEventId)) request.Headers.TryAddWithoutValidation("Last-Event-ID", lastEventId);
                    using var response = await this._httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
                    response.EnsureSuccessStatusCode();
#if NET5_0_OR_GREATER
                    using var stream = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false);
#else
                    using var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
#endif
                    using var registration = token.Register(() => stream.Dispose());
                    using var reader = new StreamReader(stream, Encoding.UTF8);
                    var data = new StringBuilder();
                    var isMessage = true;
                    while (!token.IsCancellationRequested)
                    {
#if NET8_0_OR_GREATER
                        var line = await reader.ReadLineAsync(token).ConfigureAwait(false);
#else
                        var line = await reader.ReadLineAsync().ConfigureAwait(false);
#endif
                        if (line == null) break;
                        if (line.Length == 0)
                        {
                            if (isMessage && data.Length > 0 && !token.IsCancellationRequested) this.Dispatch(data.ToString());
                            data.Clear();
                            isMessage = true;
                            continue;
                        }
                        if (line[0] == ':') continue;
                        var colon = line.IndexOf(':');
                        var field = colon < 0 ? line : line.Substring(0, colon);
                        var value = colon < 0 ? "" : line.Substring(colon + 1).TrimStart(' ');
                        switch (field)
                        {
                            case "data":
                                if (data.Length > 0) data.Append('\n');
                                data.Append(value);
                                break;
                            case "event":
                                isMessage = value == "message";
                                break;
                            case "id" when !value.Contains('\0'):
                                lastEventId = value;
                                break;
                        }
                    }
                    if (isMessage && data.Length > 0 && !token.IsCancellationRequested) this.Dispatch(data.ToString());
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { break; }
                catch (ObjectDisposedException) when (token.IsCancellationRequested) { break; }
                catch (Exception) when (!token.IsCancellationRequested)
                {
                    this._logger.LogWarning("OFREP event stream disconnected; reconnecting.");
                }

                if (token.IsCancellationRequested) break;
                try { await Task.Delay(delay, token).ConfigureAwait(false); }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { break; }
                delay = TimeSpan.FromSeconds(Math.Min(delay.TotalSeconds * 2, 30));
            }
        }
        finally
        {
            cancellation.Dispose();
        }
    }

    private void Dispatch(string data)
    {
        try
        {
            using var document = JsonDocument.Parse(data);
            if (document.RootElement.TryGetProperty("type", out var type) &&
                type.ValueKind == JsonValueKind.String && type.GetString() == "refetchEvaluation")
            {
                this._onChange();
            }
        }
        catch (JsonException) { }
    }

    private static bool TryResolve(OfrepEventStream entry, Uri? baseUri, out Uri uri)
    {
        uri = null!;
        var url = entry.Url;
        if (url == null && entry.Endpoint?.RequestUri is { } path && path.StartsWith("/", StringComparison.Ordinal))
        {
            var origin = entry.Endpoint.Origin ?? baseUri?.GetLeftPart(UriPartial.Authority);
            if (entry.Endpoint.Origin != null &&
                (!Uri.TryCreate(origin, UriKind.Absolute, out var originUri) ||
                 originUri.GetLeftPart(UriPartial.Authority).TrimEnd('/') != origin!.TrimEnd('/')))
            {
                return false;
            }
            if (origin != null) url = origin.TrimEnd('/') + path;
        }
        if (!Uri.TryCreate(url, UriKind.Absolute, out var resolved) ||
            (resolved.Scheme != Uri.UriSchemeHttp && resolved.Scheme != Uri.UriSchemeHttps) ||
            !string.IsNullOrEmpty(resolved.UserInfo))
        {
            return false;
        }
        uri = resolved;
        return true;
    }

    public void Dispose()
    {
        lock (this._gate)
        {
            if (this._disposed) return;
            this._disposed = true;
            this.OnInactive();
        }
    }
}
