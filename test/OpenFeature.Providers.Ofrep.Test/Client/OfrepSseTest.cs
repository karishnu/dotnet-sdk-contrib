#if NET8_0_OR_GREATER
using System.Net;
using System.Text;
using Microsoft.Extensions.Time.Testing;
using OpenFeature.Constant;
using OpenFeature.Model;
using OpenFeature.Providers.Ofrep.Client;
using OpenFeature.Providers.Ofrep.Configuration;
using Xunit;

namespace OpenFeature.Providers.Ofrep.Test.Client;

public class OfrepSseTest
{
    [Theory]
    [InlineData("\"url\":\"https://api.example.com/events\"")]
    [InlineData("\"endpoint\":{\"requestUri\":\"/events\"}")]
    public async Task Evaluation_ConnectsOnceAndPublishesConfigurationChanged(string endpoint)
    {
        using var handler = new SseHandler();
        using var client = new OfrepClient(new OfrepOptions("https://api.example.com"), handler);
        using var provider = new OfrepProvider(client);
        handler.EvaluationBody = $"{{\"value\":true,\"eventStreams\":[{{\"type\":\"unknown\",\"url\":\"https://api.example.com/ignored\"}},{{\"type\":\"sse\",{endpoint}}}]}}";

        await provider.ResolveBooleanValueAsync("flag", false);
        await provider.ResolveBooleanValueAsync("flag", false);
        await handler.Connected.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, handler.Connections);
        Assert.Equal("https://api.example.com/events", handler.LastEventUri?.AbsoluteUri);

        handler.Send("event: message\ndata: {\"type\":\"refetchEvaluation\"}\n\n");
        var eventPayload = await provider.GetEventChannel().Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        var change = Assert.IsType<ProviderEventPayload>(eventPayload);
        Assert.Equal(ProviderEventTypes.ProviderConfigurationChanged, change.Type);
    }

    [Fact]
    public async Task Inactivity_ClosesConnectionAndNextEvaluationReconnects()
    {
        var clock = new FakeTimeProvider();
        using var handler = new SseHandler { EvaluationBody = "{\"value\":true,\"eventStreams\":[{\"type\":\"sse\",\"url\":\"https://api.example.com/events\",\"inactivityDelaySec\":1}]}" };
        using var client = new OfrepClient(new OfrepOptions("https://api.example.com"), handler, timeProvider: clock);
        await client.EvaluateFlag("flag", false, EvaluationContext.Empty);
        await handler.Connected.Task.WaitAsync(TimeSpan.FromSeconds(5));
        clock.Advance(TimeSpan.FromSeconds(2));
        await client.EvaluateFlag("flag", false, EvaluationContext.Empty);
        await handler.Reconnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(2, handler.Connections);
    }

    [Fact]
    public async Task Inactivity_ClientOverrideTakesPrecedenceOverResponse()
    {
        var clock = new FakeTimeProvider();
        using var handler = new SseHandler { EvaluationBody = "{\"value\":true,\"eventStreams\":[{\"type\":\"sse\",\"url\":\"https://api.example.com/events\",\"inactivityDelaySec\":1}]}" };
        var options = new OfrepOptions("https://api.example.com") { InactivityDelay = TimeSpan.FromSeconds(10) };
        using var client = new OfrepClient(options, handler, timeProvider: clock);
        await client.EvaluateFlag("flag", false, EvaluationContext.Empty);
        await handler.Connected.Task.WaitAsync(TimeSpan.FromSeconds(5));
        clock.Advance(TimeSpan.FromSeconds(2));
        await client.EvaluateFlag("flag", false, EvaluationContext.Empty);
        Assert.Equal(1, handler.Connections);
    }

    [Fact]
    public async Task Inactivity_ResponseDelayCanExceedDefault()
    {
        var clock = new FakeTimeProvider();
        using var handler = new SseHandler { EvaluationBody = "{\"value\":true,\"eventStreams\":[{\"type\":\"sse\",\"url\":\"https://api.example.com/events\",\"inactivityDelaySec\":300}]}" };
        using var client = new OfrepClient(new OfrepOptions("https://api.example.com"), handler, timeProvider: clock);
        await client.EvaluateFlag("flag", false, EvaluationContext.Empty);
        await handler.Connected.Task.WaitAsync(TimeSpan.FromSeconds(5));
        clock.Advance(TimeSpan.FromSeconds(121));
        await client.EvaluateFlag("flag", false, EvaluationContext.Empty);
        Assert.Equal(1, handler.Connections);
    }

    [Fact]
    public async Task Evaluation_DoesNotForwardConfiguredHeadersToDifferentOrigin()
    {
        using var handler = new SseHandler { EvaluationBody = "{\"value\":true,\"eventStreams\":[{\"type\":\"sse\",\"url\":\"https://another.example.com/events\"}]}" };
        var options = new OfrepOptions("https://api.example.com") { Headers = new() { ["Authorization"] = "Bearer secret" } };
        using var client = new OfrepClient(options, handler);
        await client.EvaluateFlag("flag", false, EvaluationContext.Empty);
        Assert.Equal(0, handler.Connections);
    }

    [Fact]
    public async Task Evaluation_WithoutEventStreamsDoesNotConnect()
    {
        using var handler = new SseHandler { EvaluationBody = "{\"value\":true,\"eventStreams\":[{\"type\":\"unknown\",\"url\":\"https://api.example.com/events\"}]}" };
        using var client = new OfrepClient(new OfrepOptions("https://api.example.com"), handler);
        await client.EvaluateFlag("flag", false, EvaluationContext.Empty);
        Assert.Equal(0, handler.Connections);
    }

    [Fact]
    public async Task EventStreamFailureReconnects()
    {
        using var handler = new SseHandler { EvaluationBody = "{\"value\":true,\"eventStreams\":[{\"type\":\"sse\",\"url\":\"https://api.example.com/events\"}]}",
            FailFirstConnection = true };
        using var client = new OfrepClient(new OfrepOptions("https://api.example.com"), handler);
        await client.EvaluateFlag("flag", false, EvaluationContext.Empty);
        await handler.Reconnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(2, handler.Connections);
    }

    private sealed class SseHandler : HttpMessageHandler
    {
        private readonly System.Threading.Channels.Channel<string> _events = System.Threading.Channels.Channel.CreateUnbounded<string>();
        public string EvaluationBody { get; set; } = "{\"value\":true}";
        public bool FailFirstConnection { get; set; }
        public int Connections { get; private set; }
        public Uri? LastEventUri { get; private set; }
        public TaskCompletionSource<bool> Connected { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> Reconnected { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Send(string message) => this._events.Writer.TryWrite(message);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method == HttpMethod.Post)
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(this.EvaluationBody) });
            }

            this.Connections++;
            this.LastEventUri = request.RequestUri;
            this.Connected.TrySetResult(true);
            if (this.Connections > 1) this.Reconnected.TrySetResult(true);
            if (this.FailFirstConnection && this.Connections == 1)
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
            }
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StreamContent(new EventStream(this._events.Reader))
            });
        }
    }

    private sealed class EventStream(System.Threading.Channels.ChannelReader<string> events) : Stream
    {
        private byte[] _buffer = [];
        private int _offset;
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (this._offset >= this._buffer.Length)
            {
                this._buffer = Encoding.UTF8.GetBytes(await events.ReadAsync(cancellationToken));
                this._offset = 0;
            }
            var count = Math.Min(buffer.Length, this._buffer.Length - this._offset);
            this._buffer.AsMemory(this._offset, count).CopyTo(buffer);
            this._offset += count;
            return count;
        }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
#endif
