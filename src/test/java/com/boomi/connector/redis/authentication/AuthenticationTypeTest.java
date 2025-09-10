package com.boomi.connector.redis.authentication;

/**
 * Simple test to validate AuthenticationType enum functionality
 */
public class AuthenticationTypeTest {
    
    public static void main(String[] args) {
        System.out.println("Testing AuthenticationType enum...");
        
        // Test fromValue method
        AuthenticationType none = AuthenticationType.fromValue("None");
        AuthenticationType basic = AuthenticationType.fromValue("Basic");
        AuthenticationType entra = AuthenticationType.fromValue("MicrosoftEntraClientSecretCredential");
        
        System.out.println("NONE: " + none + " - requiresCredentials: " + none.requiresCredentials() + " - isMicrosoftEntra: " + none.isMicrosoftEntra());
        System.out.println("BASIC: " + basic + " - requiresCredentials: " + basic.requiresCredentials() + " - isMicrosoftEntra: " + basic.isMicrosoftEntra());
        System.out.println("ENTRA: " + entra + " - requiresCredentials: " + entra.requiresCredentials() + " - isMicrosoftEntra: " + entra.isMicrosoftEntra());
        
        // Test invalid value (should default to NONE)
        AuthenticationType invalid = AuthenticationType.fromValue("Invalid");
        System.out.println("INVALID: " + invalid + " - requiresCredentials: " + invalid.requiresCredentials() + " (defaults to NONE)");
        
        // Test default behavior
        AuthenticationType defaultType = AuthenticationType.fromValue(null);
        System.out.println("DEFAULT (null): " + defaultType + " - requiresCredentials: " + defaultType.requiresCredentials());
        
        defaultType = AuthenticationType.fromValue("");
        System.out.println("DEFAULT (empty): " + defaultType + " - requiresCredentials: " + defaultType.requiresCredentials());
        
        System.out.println("AuthenticationType enum test completed successfully!");
    }
}
