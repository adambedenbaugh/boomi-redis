package com.boomi.connector.redis.authentication;

/**
 * Enumeration of supported authentication types for Redis connections.
 * 
 */
public enum AuthenticationType {

    NONE("None"),
    BASIC("Basic"),
    MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL("MicrosoftEntraClientSecretCredential");
    
    private final String value;
    
    AuthenticationType(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
    /**
     * Creates an AuthenticationType from a configuration string value
     * @param value the configuration string value
     * @return the corresponding AuthenticationType; NONE when null/blank
     * @throws IllegalArgumentException on an unrecognized non-blank value
     */
    public static AuthenticationType fromValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return NONE;
        }

        for (AuthenticationType type : values()) {
            if (type.value.equalsIgnoreCase(value.trim())) {
                return type;
            }
        }

        throw new IllegalArgumentException("Unknown Authentication Type: '" + value
                + "'. Expected one of None, Basic, MicrosoftEntraClientSecretCredential.");
    }
    
    /**
     * Checks if this authentication type requires a username and password
     * @return true if username/password is required
     */
    public boolean requiresCredentials() {
        return this == BASIC || this == MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL;
    }
    
    /**
     * Checks if this authentication type uses Microsoft Entra ID
     * @return true if this is a Microsoft Entra authentication type
     */
    public boolean isMicrosoftEntra() {
        return this == MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL;
    }
    
    @Override
    public String toString() {
        return value;
    }
}
