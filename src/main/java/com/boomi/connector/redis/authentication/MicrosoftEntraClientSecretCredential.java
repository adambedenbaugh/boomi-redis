package com.boomi.connector.redis.authentication;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;


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
                System.out.println("Response Status Code: " + statusCode);
                
                if (statusCode != 200) {
                    throw new IOException("Failed to obtain token, status code: " + statusCode);
                }
                
                if (response.getEntity() != null) {
                    responseBody = EntityUtils.toString(response.getEntity());
                } else {
                    throw new IOException("Microsoft Entra response is null");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
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
