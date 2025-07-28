package com.boomi.connector.caching.authentication;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class MicrosoftEntraClientSecretCredential {

    private String token;
    private String username;

    /**
     * Constructs a Microsoft Entra Client Secret Credential.
     *
     * @param tenantId The tenant ID of the Azure Active Directory.
     * @param clientId The client ID of the application.
     * @param clientSecret The client secret of the application.
     */
    public MicrosoftEntraClientSecretCredential(String tenantId, String clientId, String clientSecret) {
        ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
            .clientId(clientId)
            .clientSecret(clientSecret)
            .tenantId(tenantId)
            .build();

        token = clientSecretCredential
                    .getToken(new TokenRequestContext()
                        .addScopes("https://redis.azure.com/.default")).block().getToken();
        username = extractUsernameFromToken(token);

    }


    /**
     * Gets the token.
     *
     * @return The token.
     */
    public String getToken() {
        return token;
    }
    
    /**
     * Gets the username extracted from the token.
     *
     * @return The username.
     */
    public String getUsername() {
        return username;
    }


    private static String extractUsernameFromToken(String token) {
        String[] parts = token.split("\\.");
        String base64 = parts[1];

        switch (base64.length() % 4) {
            case 2:
                base64 += "==";
                break;
            case 3:
                base64 += "=";
                break;
        }

        byte[] jsonBytes = Base64.getDecoder().decode(base64);
        String json = new String(jsonBytes, StandardCharsets.UTF_8);
        JsonObject jwt = JsonParser.parseString(json).getAsJsonObject();

        return jwt.get("oid").getAsString();
    }
}
