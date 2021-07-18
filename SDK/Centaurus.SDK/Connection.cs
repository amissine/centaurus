using Centaurus.Models;
using Centaurus.SDK.Models;
using Centaurus.Xdr;
using NLog;
using stellar_dotnet_sdk;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Centaurus.SDK
{
    internal class CentaurusConnection : IDisposable
    {
        public CentaurusConnection(Uri _websocketAddress, KeyPair _clientKeyPair, ConstellationInfo _constellationInfo)
        {
            clientKeyPair = _clientKeyPair ?? throw new ArgumentNullException(nameof(_clientKeyPair));
            websocketAddress = new Uri(_websocketAddress ?? throw new ArgumentNullException(nameof(_websocketAddress)), "centaurus");
            constellationInfo = _constellationInfo ?? throw new ArgumentNullException(nameof(_constellationInfo));

            //we don't need to create and sign heartbeat message on every sending
            hearbeatMessage = new Heartbeat().CreateEnvelope();
            hearbeatMessage.Sign(_clientKeyPair);

            cancellationTokenSource = new CancellationTokenSource();
            cancellationToken = cancellationTokenSource.Token;

            InitTimer();
        }
        static Logger logger = LogManager.GetCurrentClassLogger();

        private CancellationTokenSource cancellationTokenSource;
        private CancellationToken cancellationToken;

        public event Action<MessageEnvelope> OnMessage;

        public event Action<MessageEnvelope> OnSend;

        public event Action<Exception> OnException;

        public event Action OnClosed;

        public async Task EstablishConnection()
        {
            await webSocket.ConnectAsync(websocketAddress, cancellationToken);
            Listen();
        }

        public async Task CloseConnection(WebSocketCloseStatus status = WebSocketCloseStatus.NormalClosure, string desc = null)
        {
            try
            {
                if (webSocket.State == WebSocketState.Open)
                {
                    await sendMessageSemaphore.WaitAsync();
                    try
                    {
                        var timeoutTokenSource = new CancellationTokenSource(1000);
                        await webSocket.CloseAsync(status, desc, timeoutTokenSource.Token);
                        cancellationTokenSource.Cancel();
                    }
                    catch (WebSocketException exc)
                    {
                        //ignore client disconnection
                        if (exc.WebSocketErrorCode != WebSocketError.ConnectionClosedPrematurely
                            && exc.WebSocketErrorCode != WebSocketError.InvalidState)
                            throw;
                    }
                    catch (OperationCanceledException) { }
                    finally
                    {
                        sendMessageSemaphore.Release();
                    }
                }
            }
            catch (WebSocketException exc)
            {
                //ignore "closed-without-completing-handshake" error and invalid state error
                if (exc.WebSocketErrorCode != WebSocketError.ConnectionClosedPrematurely
                    && exc.WebSocketErrorCode != WebSocketError.InvalidState)
                    throw;
            }
            var allPendingRequests = Requests.Values;
            var closeException = new ConnectionCloseException(status, desc);
            foreach (var pendingRequest in allPendingRequests)
                pendingRequest.SetException(closeException);

            if (status != WebSocketCloseStatus.NormalClosure)
                _ = Task.Factory.StartNew(() => OnException?.Invoke(closeException));
        }

        public void AssignAccountId(int accountId)
        {
            this.accountId = accountId;
        }

        private SemaphoreSlim sendMessageSemaphore = new SemaphoreSlim(1);

        public async Task<CentaurusResponseBase> SendMessage(MessageEnvelope envelope)
        {
            AssignRequestId(envelope);
            AssignAccountId(envelope);
            if (!envelope.IsSignedBy(clientKeyPair))
                envelope.Sign(clientKeyPair);

            CentaurusResponse resultTask = null;
            if (envelope != hearbeatMessage
                && envelope.Message.MessageId != default)
            {
                resultTask = RegisterRequest(envelope);
            }


            await sendMessageSemaphore.WaitAsync();
            try
            {
                var serializedData = XdrConverter.Serialize(envelope);

                using var buffer = XdrBufferFactory.Rent();
                using var writer = new XdrBufferWriter(buffer.Buffer);
                XdrConverter.Serialize(envelope, writer);

                if (webSocket.State == WebSocketState.Open)
                    await webSocket.SendAsync(buffer.AsSegment(0, writer.Length), WebSocketMessageType.Binary, true, cancellationToken);

                _ = Task.Factory.StartNew(() => OnSend?.Invoke(envelope));

                heartbeatTimer?.Reset();
            }
            catch (WebSocketException e)
            {
                if (resultTask != null)
                    resultTask.SetException(e);

                await CloseConnection(WebSocketCloseStatus.ProtocolError, e.Message);
            }
            catch (Exception exc)
            {
                if (resultTask == null)
                    throw;
                resultTask.SetException(exc);
            }
            finally
            {
                sendMessageSemaphore.Release();
            }

            return resultTask ?? (CentaurusResponseBase)new VoidResponse();
        }

        public async Task CloseAndDispose(WebSocketCloseStatus status = WebSocketCloseStatus.NormalClosure, string desc = null)
        {
            await CloseConnection(status, desc);
            Dispose();
        }

        public void Dispose()
        {
            if (heartbeatTimer != null)
            {
                heartbeatTimer.Elapsed -= HeartbeatTimer_Elapsed;
                heartbeatTimer.Dispose();
                heartbeatTimer = null;
            }

            cancellationTokenSource?.Dispose();
            cancellationTokenSource = null;

            webSocket?.Dispose();
            webSocket = null;
        }

        #region Private Members

        private System.Timers.Timer heartbeatTimer = null;

        private void InitTimer()
        {
            heartbeatTimer = new System.Timers.Timer();
            heartbeatTimer.Interval = 5000;
            heartbeatTimer.AutoReset = false;
            heartbeatTimer.Elapsed += HeartbeatTimer_Elapsed;
        }

        private KeyPair clientKeyPair;
        private int accountId;
        private Uri websocketAddress;
        private ConstellationInfo constellationInfo;
        private MessageEnvelope hearbeatMessage;

        private void HeartbeatTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            _ = SendMessage(hearbeatMessage);
        }

        private ClientWebSocket webSocket = new ClientWebSocket();

        private void AssignRequestId(MessageEnvelope envelope)
        {
            var request = envelope.Message as RequestMessage;
            if (request == null || request.MessageId != 0)
                return;
            var currentTicks = DateTime.UtcNow.Ticks;
            request.RequestId = request is SequentialRequestMessage ? currentTicks : -currentTicks;
        }

        private void AssignAccountId(MessageEnvelope envelope)
        {
            var request = envelope.Message as RequestMessage;
            if (request == null || request.Account > 0)
                return;
            request.Account = accountId;
        }

        private CentaurusResponse RegisterRequest(MessageEnvelope envelope)
        {
            lock (Requests)
            {
                var messageId = envelope.Message.MessageId;
                var response = CentaurusResponse.Create(envelope);
                if (!Requests.TryAdd(messageId, response))
                    throw new Exception("Unable to add request to pending requests.");
                return response;
            }
        }

        private bool TryResolveRequest(MessageEnvelope envelope)
        {
            var resultMessage = envelope.Message as ResultMessage;

            if (resultMessage != null)
            {
                lock (Requests)
                {
                    var messageId = resultMessage.OriginalMessage.Message is RequestQuantum ?
                        ((RequestQuantum)resultMessage.OriginalMessage.Message).RequestMessage.MessageId :
                        resultMessage.OriginalMessage.Message.MessageId;
                    if (Requests.TryGetValue(messageId, out var task))
                    {
                        task.AssignResponse(envelope);
                        if (task.IsCompleted)
                            Requests.TryRemove(messageId, out _);
                        logger.Trace($"{envelope.Message.MessageType}:{messageId} result was set.");
                        return true;
                    }
                    else
                    {
                        logger.Trace($"Unable set result for msg with id {envelope.Message.MessageType}:{messageId}.");
                    }
                }
            }
            else
            {
                logger.Trace("Request is not result message.");
            }
            return false;
        }

        private MessageEnvelope DeserializeMessage(XdrBufferFactory.RentedBuffer buffer)
        {
            try
            {
                var reader = new XdrBufferReader(buffer.Buffer, buffer.Length);
                return XdrConverter.Deserialize<MessageEnvelope>(reader);
            }
            catch
            {
                throw new UnexpectedMessageException("Unable to deserialize message.");
            }
        }

        private async Task Listen()
        {
            try
            {
                while (webSocket.State != WebSocketState.Closed && webSocket.State != WebSocketState.Aborted && !cancellationToken.IsCancellationRequested)
                {
                    var result = await webSocket.GetWebsocketBuffer(WebSocketExtension.ChunkSize * 100, cancellationToken);
                    using (result.messageBuffer)
                        if (!cancellationToken.IsCancellationRequested)
                        {
                            //the client send close message
                            if (result.messageType == WebSocketMessageType.Close)
                            {
                                if (webSocket.State != WebSocketState.CloseReceived)
                                    continue;
                                await sendMessageSemaphore.WaitAsync();
                                try
                                {
                                    var timeoutTokenSource = new CancellationTokenSource(1000);
                                    await webSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Close", CancellationToken.None);
                                }
                                finally
                                {
                                    sendMessageSemaphore.Release();
                                    cancellationTokenSource.Cancel();
                                }
                            }
                            else
                            {
                                try
                                {
                                    HandleMessage(DeserializeMessage(result.messageBuffer));
                                }
                                catch (UnexpectedMessageException e)
                                {
                                    _ = Task.Factory.StartNew(() => OnException?.Invoke(e));
                                }
                            }
                        }
                }
            }
            catch (OperationCanceledException)
            { }
            catch (Exception e)
            {
                var closureStatus = WebSocketCloseStatus.InternalServerError;
                var desc = default(string);
                if (e is WebSocketException webSocketException
                    && webSocketException.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely) //connection already closed by the other side
                    return;
                else if (e is ConnectionCloseException closeException)
                {
                    closureStatus = closeException.Status;
                    desc = closeException.Description;
                }
                else if (e is System.FormatException)
                    closureStatus = WebSocketCloseStatus.ProtocolError;
                else
                    logger.Error(e);
                await CloseConnection(closureStatus, desc);
            }
            finally
            {
                //make sure the socket is closed
                if (webSocket.State != WebSocketState.Closed)
                    webSocket.Abort();
                _ = Task.Factory.StartNew(() => OnClosed?.Invoke());
            }
        }

        private ConcurrentDictionary<long, CentaurusResponse> Requests = new ConcurrentDictionary<long, CentaurusResponse>();

        private void HandleMessage(MessageEnvelope envelope)
        {
            TryResolveRequest(envelope);
            _ = Task.Factory.StartNew(() => OnMessage?.Invoke(envelope));
        }

        #endregion
    }
}
