﻿using Centaurus.Domain;
using Centaurus.Models;
using NLog;
using stellar_dotnet_sdk;
using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Centaurus.Auditor
{
    public class Startup
    {
        private AuditorWebSocketConnection auditor;

        private bool isAborted = false;

        private Logger logger = LogManager.GetCurrentClassLogger();

        public Startup(AuditorSettings settings)
        {
            Global.Init(settings);
        }

        public async Task Run()
        {
            MessageHandlers<AuditorWebSocketConnection>.Init();

            Global.AppState.StateChanged += StateChanged;

            while (auditor == null)
            {
                var _auditor = new AuditorWebSocketConnection();
                try
                {
                    Subscribe(_auditor);
                    await _auditor.EstablishConnection();
                    auditor = _auditor;
                }
                catch (Exception exc)
                {
                    Unsubscribe(_auditor);
                    await CloseConnection(_auditor);

                    logger.Error(exc, "Unable establish connection. Retry in 5000ms");
                    Thread.Sleep(5000);
                }
            }
        }

        private async void StateChanged(object sender, ApplicationState state)
        {
            if (state == ApplicationState.Failed)
            {
                await Abort();
                await Global.SnapshotManager.SavePendingQuantums();

                //TODO: restart after some timeout
            }
        }

        public async Task Abort()
        {
            isAborted = true;
            Unsubscribe(auditor);
            await CloseConnection(auditor);
        }

        private void Subscribe(AuditorWebSocketConnection _auditor)
        {
            if (_auditor != null)
            {
                _auditor.OnConnectionStateChanged += OnConnectionStateChanged;
            }
        }

        private void Unsubscribe(AuditorWebSocketConnection _auditor)
        {
            if (_auditor != null)
            {
                _auditor.OnConnectionStateChanged -= OnConnectionStateChanged;
            }
        }

        private void OnConnectionStateChanged(object sender, ConnectionState e)
        {
            switch (e)
            {
                case ConnectionState.Ready:
                    Ready((BaseWebSocketConnection)sender);
                    break;
                case ConnectionState.Closed:
                    Close((BaseWebSocketConnection)sender);
                    break;
                default:
                    break;
            }
        }

        private async Task CloseConnection(AuditorWebSocketConnection _auditor)
        {
            if (_auditor != null)
            {
                await _auditor?.CloseConnection();
                _auditor?.Dispose();
            }
        }

        private void Ready(BaseWebSocketConnection e)
        {
            Global.AppState.State = ApplicationState.Ready;
        }

        private async void Close(BaseWebSocketConnection e)
        {
            Global.AppState.State = ApplicationState.Running;
            Unsubscribe(auditor);
            await CloseConnection(auditor);
            if (!isAborted)
                _ = Run();
        }
    }
}
