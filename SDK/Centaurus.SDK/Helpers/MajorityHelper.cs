using Centaurus.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Centaurus.SDK
{
    public static class MajorityHelper
    {
        public static int GetMajorityCount(int totalAuditorsCount)
        {
            return (int)(totalAuditorsCount / 2.0) + 1;
        }

        public static bool HasMajority(this MessageEnvelope envelope, int totalAuditorsCount)
        {
            //imply that signatures are unique and were validated beforehand
            var auditorsSignaturesCount = envelope.Signatures.Count;
            return auditorsSignaturesCount >= GetMajorityCount(totalAuditorsCount);
        }
    }
}
