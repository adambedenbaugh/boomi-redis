package com.boomi.connector.redis.authentication;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.logging.Logger;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;


public class MicrosoftEntraClientSecretCredential {
    
    private static final Logger logger = Logger.getLogger(MicrosoftEntraClientSecretCredential.class.getName());

    private String token;
    private String username;
    private long expiresAtMillis;

    /**
     * Constructs a Microsoft Entra Client Secret Credential.
     *
     * @param tenantId The tenant ID of the Azure Active Directory.
     * @param clientId The client ID of the application.
     * @param clientSecret The client secret of the application.
     */
    public MicrosoftEntraClientSecretCredential(String tenantId, String clientId, String clientSecret) {

        String responseBody = "";
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            String url = "https://login.microsoftonline.com/" + tenantId + "/oauth2/v2.0/token";
            HttpPost post = new HttpPost(url);
            post.setHeader("Content-Type", "application/x-www-form-urlencoded");
            post.setEntity(new StringEntity(
                "grant_type=client_credentials&client_id=" + clientId + 
                "&client_secret=" + clientSecret + 
                "&scope=https://redis.azure.com/.default", 
                StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = httpClient.execute(post)) {
                int statusCode = response.getStatusLine().getStatusCode();
                logger.info("Response Status Code: " + statusCode);
                
                if (statusCode != 200) {
                    throw new IOException("Failed to obtain token. Status code: " + statusCode + ". Response: " + EntityUtils.toString(response.getEntity()));
                }
                
                if (response.getEntity() != null) {
                    responseBody = EntityUtils.toString(response.getEntity());
                } else {
                    throw new IOException("Microsoft Entra response is null");
                }
            }
        } catch (IOException e) {
            logger.severe("Failed to obtain Microsoft Entra token: " + e.getMessage());
            throw new RuntimeException("Failed to obtain Microsoft Entra token", e);
        }

        JsonObject jsonResponse = JsonParser.parseString(responseBody).getAsJsonObject();

        token = jsonResponse.get("access_token").getAsString();
        if (token == null || token.isEmpty()) {
            throw new IllegalArgumentException("Failed to obtain access token.");
        }
        username = extractUsernameFromToken(token);
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("Failed to extract username from token.");
        }
        
        // Extract expires_in and convert to expiration time in milliseconds
        if (jsonResponse.has("expires_in")) {
            int expiresInSeconds = jsonResponse.get("expires_in").getAsInt();
            expiresAtMillis = System.currentTimeMillis() + (expiresInSeconds * 1000L);
        } else {
            throw new IllegalArgumentException("Token response does not contain expires_in field.");
        }
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
     * Gets the username extracted from the token. By extracting the 'oid' claim the username does not need to be supplied. 
     *
     * @return The username.
     */
    public String getUsername() {
        return username;
    }
    
    /**
     * Gets the expiration time of the token in milliseconds since epoch.
     *
     * @return The expiration time in milliseconds.
     */
    public long getExpiresAtMillis() {
        return expiresAtMillis;
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
