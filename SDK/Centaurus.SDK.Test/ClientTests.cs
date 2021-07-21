using System;
using System.Threading.Tasks;
using Centaurus.SDK.Models;
using NUnit.Framework;
using stellar_dotnet_sdk;

namespace Centaurus.SDK
{
    public class SDKTests
    {
        [Test, Explicit]
        public async Task BasicSdkTest()
        {
            var alphaEndpoint = "alpha.test.centaurus.stellar.expert";
            var constellationInfo = await CentaurusApi.GetConstellationInfo(new Uri("https://"+alphaEndpoint));
            var client = new CentaurusClient(new Uri("wss://" + alphaEndpoint), KeyPair.FromSecretSeed("SD626X4FW2GDP4HRCPNOIBVKQ7AANGCKU3UWXT6B7TZFQC5CFWQBCZ2E"), constellationInfo);
            await client.Connect();

            var info = await client.GetAccountData();
        }
    }
}