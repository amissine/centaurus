using System;
using Centaurus.SDK.Models;
using NUnit.Framework;
using stellar_dotnet_sdk;

namespace Centaurus.SDK
{
    public class SDKTests
    {
        [Test, Explicit]
        public async void BasicSdkTest()
        {
            var alphaUri = new Uri("https://alpha.test.centaurus.stellar.expert");
            var constellationInfo = await CentaurusApi.GetConstellationInfo(alphaUri);
            var client = new CentaurusClient(alphaUri, KeyPair.FromSecretSeed("SD626X4FW2GDP4HRCPNOIBVKQ7AANGCKU3UWXT6B7TZFQC5CFWQBCZ2E"), constellationInfo);
            await client.Connect();

            var info = await client.GetAccountData();
        }
    }
}