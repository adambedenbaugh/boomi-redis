package com.boomi.connector.redis.connection;

/**
 * Redis topology as declared on the connection. Determines which Jedis client the
 * factory builds: OSS uses JedisCluster (client-side sharding, follows MOVED/ASK);
 * Non-clustered and Enterprise both use a standalone/pooled Jedis (Enterprise's proxy
 * hides sharding behind a single endpoint, so no client-side routing is needed).
 */
public enum ClusteringPolicy {

    NON_CLUSTERED("NonClustered"),
    OSS_CLUSTERED("OSSClustered"),
    ENTERPRISE_CLUSTERED("EnterpriseClustered");

    private final String value;

    ClusteringPolicy(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * @param value the stored connection-property value
     * @return the matching policy, or NON_CLUSTERED when null/blank
     * @throws IllegalArgumentException on an unrecognized non-blank value
     */
    public static ClusteringPolicy fromValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return NON_CLUSTERED;
        }
        for (ClusteringPolicy policy : values()) {
            if (policy.value.equalsIgnoreCase(value.trim())) {
                return policy;
            }
        }
        throw new IllegalArgumentException("Unknown Clustering Policy: '" + value
                + "'. Expected one of NonClustered, OSSClustered, EnterpriseClustered.");
    }

    /** True only for OSS clustering, which requires a cluster-aware (JedisCluster) client. */
    public boolean isOssCluster() {
        return this == OSS_CLUSTERED;
    }

    @Override
    public String toString() {
        return value;
    }
}
