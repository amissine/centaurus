using Centaurus.Models;
using Centaurus.SDK.Models;
using Centaurus.Xdr;
using NLog;
using NSec.Cryptography;
using stellar_dotnet_sdk;
using stellar_dotnet_sdk.xdr;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Centaurus.SDK
{
    public class CentaurusClient : IDisposable
    {
        static Logger logger = LogManager.GetCurrentClassLogger();
        public CentaurusClient(Uri alphaWebSocketAddress, KeyPair keyPair, ConstellationInfo constellationInfo)
        {
            this.alphaWebSocketAddress = alphaWebSocketAddress ?? throw new ArgumentNullException(nameof(alphaWebSocketAddress));
            this.keyPair = keyPair ?? throw new ArgumentNullException(nameof(keyPair));
            this.constellation = constellationInfo ?? throw new ArgumentNullException(nameof(constellationInfo));

            Network.Use(new Network(constellation.StellarNetwork.Passphrase));
        }

        public async Task Connect()
        {
            try
            {
                await connectionSemaphore.WaitAsync();

                if (IsConnected)
                    return;

                handshakeResult = new TaskCompletionSource<int>();

                connection = new CentaurusConnection(alphaWebSocketAddress, keyPair, constellation);
                SubscribeToEvents(connection);
                await connection.EstablishConnection();

                await handshakeResult.Task;

                await UpdateAccountData();

                IsConnected = true;
            }
            catch (Exception exc)
            {
                UnsubscribeFromEvents(connection);
                if (connection != null)
                    await connection.CloseAndDispose();
                connection = null;
                throw new Exception("Login failed.", exc);
            }
            finally
            {
                connectionSemaphore.Release();
            }
        }

        public async Task UpdateAccountData()
        {
            AccountData = await GetAccountData();
        }

        public async Task CloseConnection()
        {
            try
            {
                connectionSemaphore.Wait();
                UnsubscribeFromEvents(connection);
                if (connection != null)
                    await connection.CloseAndDispose();
                connection = null;
            }
            finally
            {

                connectionSemaphore.Release();
            }
        }

        public event Action<MessageEnvelope> OnMessage;

        public event Action<MessageEnvelope> OnSend;

        public event Action<Exception> OnException;

        public event Action<AccountDataModel> OnAccountUpdate;

        public event Action OnClosed;

        public async Task<AccountDataModel> GetAccountData(bool waitForFinalize = true)
        {
            var response = (CentaurusResponse)await connection.SendMessage(new AccountDataRequest().CreateEnvelope());
            var data = await (waitForFinalize ? response.ResponseTask : response.AcknowledgmentTask);
            var rawMessage = (AccountDataResponse)data.Message;
            var balances = rawMessage.Balances.Select(x => BalanceModel.FromBalance(x, constellation)).ToDictionary(k => k.AssetId, v => v);
            var orders = rawMessage.Orders.Select(x => OrderModel.FromOrder(x, constellation)).ToDictionary(k => k.OrderId, v => v);
            return new AccountDataModel
            {
                Balances = balances,
                Orders = orders
            };
        }

        public async Task<MessageEnvelope> Withdrawal(KeyPair destination, string amount, ConstellationInfo.Asset asset, bool waitForFinalize = true)
        {
            var paymentMessage = new WithdrawalRequest();
            var tx = await TransactionHelper.GetWithdrawalTx(keyPair, constellation, destination, amount, asset);

            paymentMessage.TransactionXdr = tx.ToArray();

            var response = (CentaurusResponse)await connection.SendMessage(paymentMessage.CreateEnvelope());
            var result = await (waitForFinalize ? response.ResponseTask : response.AcknowledgmentTask);
            var txResultMessage = result.Message as ITransactionResultMessage;
            if (txResultMessage is null)
                throw new Exception($"Unexpected result type '{result.Message.MessageType}'");

            tx.Sign(keyPair);
            foreach (var signature in txResultMessage.TxSignatures)
            {
                tx.Signatures.Add(signature.ToDecoratedSignature());
            }

            var submitResult = await tx.Submit(constellation);
            if (!submitResult.IsSuccess())
            {
                logger.Error($"Submit withdrawal failed. Result xdr: {submitResult.ResultXdr}");
            }
            return result;
        }

        public async Task<MessageEnvelope> CreateOrder(long amount, double price, OrderSide side, ConstellationInfo.Asset asset, bool waitForFinalize = true)
        {
            var order = new OrderRequest { Amount = amount, Price = price, Side = side, Asset = asset.Id };

            var response = (CentaurusResponse)await connection.SendMessage(order.CreateEnvelope());
            var result = await (waitForFinalize ? response.ResponseTask : response.AcknowledgmentTask);
            return result;
        }

        public async Task<MessageEnvelope> CancelOrder(ulong orderId, bool waitForFinalize = true)
        {
            var order = new OrderCancellationRequest { OrderId = orderId };
            var response = (CentaurusResponse)await connection.SendMessage(order.CreateEnvelope());
            var result = await (waitForFinalize ? response.ResponseTask : response.AcknowledgmentTask);
            return result;
        }

        public async Task<MessageEnvelope> MakePayment(KeyPair destination, long amount, ConstellationInfo.Asset asset, bool waitForFinalize = true)
        {
            var paymentMessage = new PaymentRequest { Amount = amount, Destination = destination, Asset = asset.Id };
            var response = (CentaurusResponse)await connection.SendMessage(paymentMessage.CreateEnvelope());
            var result = await (waitForFinalize ? response.ResponseTask : response.AcknowledgmentTask);
            return result;
        }

        public async Task Register(long amount, params KeyPair[] extraSigners)
        {
            //try to connect to make sure that pubkey is not registered yet
            try { await Connect(); } catch { }

            if (IsConnected)
                throw new Exception("Already registered.");

            if (amount < constellation.MinAccountBalance)
                throw new Exception($"Min allowed account balance is {amount}.");

            await Deposit(amount, constellation.Assets.First(a => a.Issuer == null), extraSigners);
            var tries = 0;
            while (true)
                try
                {
                    await Connect();
                    break;
                }
                catch
                {
                    if (++tries < 10)
                    {
                        await Task.Delay(3000);
                        continue;
                    }
                    throw new Exception("Unable to login. Maybe server is too busy. Try later.");
                }
        }

        public async Task Deposit(long amount, ConstellationInfo.Asset asset, params KeyPair[] extraSigners)
        {
            var tx = await TransactionHelper.GetDepositTx(keyPair, constellation, Amount.FromXdr(amount), asset);
            tx.Sign(keyPair);
            foreach (var signer in extraSigners)
                tx.Sign(signer);
            await tx.Submit(constellation);
        }

        public void Dispose()
        {
            UnsubscribeFromEvents(connection);
            CloseConnection().Wait();
        }

        #region Private members

        static CentaurusClient()
        {
            DynamicSerializersInitializer.Init();
        }

        private readonly SemaphoreSlim connectionSemaphore = new SemaphoreSlim(1);

        private CentaurusConnection connection;
        private Uri alphaWebSocketAddress;
        private KeyPair keyPair;
        private ConstellationInfo constellation;
        private TaskCompletionSource<int> handshakeResult;

        private List<long> processedEffectsMessages = new List<long>();

        private void RegisterNewEffectsMessage(long messageId)
        {
            processedEffectsMessages.Add(messageId);
            if (processedEffectsMessages.Count > 100_000)
                processedEffectsMessages.RemoveAt(0);
        }

        private void ResultMessageHandler(MessageEnvelope envelope)
        {
            lock (processedEffectsMessages)
            {
                if (AccountData == null
                   || !(envelope.Message is IEffectsContainer effectsMessage)
                   || processedEffectsMessages.Any(r => r == envelope.Message.MessageId))
                    return;
                RegisterNewEffectsMessage(envelope.Message.MessageId);
                try
                {
                    foreach (var effect in effectsMessage.Effects)
                    {
                        switch (effect)
                        {
                            case NonceUpdateEffect nonceUpdateEffect:
                                AccountData.Nonce = nonceUpdateEffect.Nonce;
                                break;
                            case BalanceCreateEffect balanceCreateEffect:
                                AccountData.AddBalance(balanceCreateEffect.Asset, constellation);
                                break;
                            case BalanceUpdateEffect balanceUpdateEffect:
                                AccountData.UpdateBalance(balanceUpdateEffect.Asset, balanceUpdateEffect.Amount);
                                break;
                            case OrderPlacedEffect orderPlacedEffect:
                                {
                                    AccountData.AddOrder(orderPlacedEffect.OrderId, orderPlacedEffect.Amount, orderPlacedEffect.Price, constellation);
                                    var decodedId = OrderIdConverter.Decode(orderPlacedEffect.OrderId);
                                    if (decodedId.Side == OrderSide.Buy)
                                        AccountData.UpdateLiabilities(0, orderPlacedEffect.QuoteAmount);
                                    else
                                        AccountData.UpdateLiabilities(decodedId.Asset, orderPlacedEffect.Amount);
                                }
                                break;
                            case OrderRemovedEffect orderRemoveEffect:
                                {
                                    AccountData.RemoveOrder(orderRemoveEffect.OrderId);
                                    var decodedId = OrderIdConverter.Decode(orderRemoveEffect.OrderId);
                                    if (decodedId.Side == OrderSide.Buy)
                                        AccountData.UpdateLiabilities(0, -orderRemoveEffect.QuoteAmount);
                                    else
                                        AccountData.UpdateLiabilities(decodedId.Asset, -orderRemoveEffect.Amount);
                                }
                                break;
                            case TradeEffect tradeEffect:
                                {
                                    AccountData.UpdateOrder(tradeEffect.OrderId, tradeEffect.AssetAmount);

                                    var decodedId = OrderIdConverter.Decode(tradeEffect.OrderId);
                                    if (decodedId.Side == OrderSide.Buy)
                                    {
                                        if (!tradeEffect.IsNewOrder)
                                            AccountData.UpdateLiabilities(0, -tradeEffect.QuoteAmount);
                                        AccountData.UpdateBalance(0, -tradeEffect.QuoteAmount);
                                        AccountData.UpdateBalance(decodedId.Asset, tradeEffect.AssetAmount);
                                    }
                                    else
                                    {
                                        if (!tradeEffect.IsNewOrder)
                                            AccountData.UpdateLiabilities(decodedId.Asset, -tradeEffect.AssetAmount);
                                        AccountData.UpdateBalance(decodedId.Asset, -tradeEffect.AssetAmount);
                                        AccountData.UpdateBalance(0, tradeEffect.QuoteAmount);
                                    }
                                }
                                break;
                            case WithdrawalCreateEffect withdrawalCreateEffect:
                                foreach (var withdrawalItem in withdrawalCreateEffect.Items)
                                {
                                    AccountData.UpdateLiabilities(withdrawalItem.Asset, withdrawalItem.Amount);
                                }
                                break;
                            case WithdrawalRemoveEffect withdrawalRemoveEffect:
                                foreach (var withdrawalItem in withdrawalRemoveEffect.Items)
                                {
                                    if (withdrawalRemoveEffect.IsSuccessful)
                                        AccountData.UpdateBalance(withdrawalItem.Asset, -withdrawalItem.Amount);
                                    AccountData.UpdateLiabilities(withdrawalItem.Asset, -withdrawalItem.Amount);
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    OnAccountUpdate?.Invoke(AccountData);
                }
                catch (Exception exc)
                {
                    OnException?.Invoke(exc);
                }
            }
        }

        private void SubscribeToEvents(CentaurusConnection connection)
        {
            connection.OnClosed += Connection_OnClosed;
            connection.OnException += Connection_OnException;
            connection.OnMessage += Connection_OnMessage;
            connection.OnSend += Connection_OnSend;
        }

        private void UnsubscribeFromEvents(CentaurusConnection connection)
        {
            if (connection == null)
                return;
            connection.OnClosed -= Connection_OnClosed;
            connection.OnException -= Connection_OnException;
            connection.OnMessage -= Connection_OnMessage;
            connection.OnSend -= Connection_OnSend;
        }

        private bool HandleHandshake(MessageEnvelope envelope)
        {
            if (envelope.Message is HandshakeInit)
            {
                logger.Trace("Handshake: started.");
                isConnecting = true;
                logger.Trace("Handshake: isConnecting is set to true.");
                try
                {
                    var responseTask = (CentaurusResponse)connection.SendMessage(envelope.Message.CreateEnvelope()).Result;
                    logger.Trace("Handshake: message is sent.");
                    var result = (HandshakeResult)responseTask.ResponseTask.Result.Message;
                    logger.Trace("Handshake: response awaited.");
                    if (result.Status != ResultStatusCodes.Success)
                        throw new Exception();
                    connection.AssignAccountId(result.AccountId);
                    handshakeResult.SetResult(result.AccountId);
                    logger.Trace("Handshake: result is set to true.");
                    IsConnected = true;
                    logger.Trace("Handshake: isConnected is set.");
                }
                catch (Exception exc)
                {
                    handshakeResult.TrySetException(exc);
                    logger.Trace("Handshake: exception is set.");
                }
                isConnecting = false;
                logger.Trace("Handshake: isConnecting is set to false.");
                return true;
            }
            return false;
        }

        public bool IsConnected { get; private set; }

        private bool isConnecting = false;

        private AccountDataModel accountData;
        public AccountDataModel AccountData
        {
            get => accountData;
            private set
            {
                accountData = value;
                OnAccountUpdate?.Invoke(accountData);
            }
        }

        private void Connection_OnSend(MessageEnvelope envelope)
        {
            if (IsConnected)
                OnSend?.Invoke(envelope);
        }

        private void Connection_OnMessage(MessageEnvelope envelope)
        {
            logger.Trace($"OnMessage: {envelope.Message.MessageType}");
            if (!isConnecting)
            {
                if (!IsConnected)
                {
                    logger.Trace($"OnMessage: not connected.");
                    HandleHandshake(envelope);
                }
                else
                {
                    logger.Trace($"OnMessage: connected.");
                    ResultMessageHandler(envelope);
                    OnMessage?.Invoke(envelope);
                }
            }
            else
            {
                logger.Trace($"OnMessage: connecting...");
            }
        }

        private void Connection_OnException(Exception exception)
        {
            OnException?.Invoke(exception);
        }

        private void Connection_OnClosed()
        {
            IsConnected = false;
            OnClosed?.Invoke();
        }

        #endregion
    }
}
