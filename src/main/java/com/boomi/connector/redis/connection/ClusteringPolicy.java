package com.boomi.connector.redis.connection;

/**
 * Redis topology as declared on the connection. Determines which Jedis client the
 * factory builds:
 * <ul>
 *   <li>{@code OSS_CLUSTERED} -> {@code JedisCluster} (client-side sharding, follows MOVED/ASK).</li>
 *   <li>{@code NON_CLUSTERED} -> standalone/pooled {@code Jedis} (a single node, or a proxy-fronted
 *       cluster that presents one endpoint, e.g. the Redis Enterprise clustering policy).</li>
 * </ul>
 * The retired "Enterprise Clustered" option ({@code EnterpriseClustered}) always used the
 * single-endpoint path; {@link #fromValue(String)} maps that legacy stored value to
 * {@link #NON_CLUSTERED} so already-deployed connections keep working after it was removed
 * from the UI.
 */
public enum ClusteringPolicy {

    NON_CLUSTERED("NonClustered"),
    OSS_CLUSTERED("OSSClustered");

    /** Legacy stored value for the retired "Enterprise Clustered" option; treated as NON_CLUSTERED. */
    private static final String LEGACY_ENTERPRISE_VALUE = "EnterpriseClustered";

    private final String value;

    ClusteringPolicy(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * @param value the stored connection-property value
     * @return the matching policy; NON_CLUSTERED when null/blank or the legacy "EnterpriseClustered" value
     * @throws IllegalArgumentException on an unrecognized non-blank value
     */
    public static ClusteringPolicy fromValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return NON_CLUSTERED;
        }
        String trimmed = value.trim();
        if (LEGACY_ENTERPRISE_VALUE.equalsIgnoreCase(trimmed)) {
            return NON_CLUSTERED;
        }
        for (ClusteringPolicy policy : values()) {
            if (policy.value.equalsIgnoreCase(trimmed)) {
                return policy;
            }
        }
        throw new IllegalArgumentException("Unknown Clustering Policy: '" + value
                + "'. Expected one of NonClustered, OSSClustered.");
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
