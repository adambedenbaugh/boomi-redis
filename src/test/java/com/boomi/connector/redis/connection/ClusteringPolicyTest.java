package com.boomi.connector.redis.connection;

import org.junit.Test;
import static org.junit.Assert.*;

public class ClusteringPolicyTest {

    @Test
    public void fromValueMapsEachStoredValue() {
        assertEquals(ClusteringPolicy.NON_CLUSTERED, ClusteringPolicy.fromValue("NonClustered"));
        assertEquals(ClusteringPolicy.OSS_CLUSTERED, ClusteringPolicy.fromValue("OSSClustered"));
    }

    @Test
    public void fromValueMapsLegacyEnterpriseValueToNonClustered() {
        // "Enterprise Clustered" was retired from the UI; it always used the single-endpoint
        // (standalone) path, so already-deployed connections storing it must keep working.
        assertEquals(ClusteringPolicy.NON_CLUSTERED, ClusteringPolicy.fromValue("EnterpriseClustered"));
        assertEquals(ClusteringPolicy.NON_CLUSTERED, ClusteringPolicy.fromValue("  enterpriseclustered "));
        assertFalse(ClusteringPolicy.fromValue("EnterpriseClustered").isOssCluster());
    }

    @Test
    public void fromValueIsCaseInsensitiveAndTrims() {
        assertEquals(ClusteringPolicy.OSS_CLUSTERED, ClusteringPolicy.fromValue("  ossclustered "));
    }

    @Test
    public void fromValueDefaultsToNonClusteredWhenBlankOrNull() {
        assertEquals(ClusteringPolicy.NON_CLUSTERED, ClusteringPolicy.fromValue(null));
        assertEquals(ClusteringPolicy.NON_CLUSTERED, ClusteringPolicy.fromValue("   "));
    }

    @Test
    public void fromValueThrowsDescriptiveOnUnknown() {
        try {
            ClusteringPolicy.fromValue("Bogus");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Bogus"));
            assertTrue(e.getMessage().contains("NonClustered"));
        }
    }

    @Test
    public void isOssClusterOnlyForOss() {
        assertTrue(ClusteringPolicy.OSS_CLUSTERED.isOssCluster());
        assertFalse(ClusteringPolicy.NON_CLUSTERED.isOssCluster());
    }
}
