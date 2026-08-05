package com.boomi.connector.redis.authentication;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.OAuth2Token;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import redis.clients.jedis.DefaultRedisCredentials;
import redis.clients.jedis.RedisCredentials;
import redis.clients.jedis.RedisCredentialsProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Supplies Redis credentials per physical connection for None / Basic / Microsoft Entra auth.
 * For Entra, delegates token acquisition (and expiry-aware refresh) to Boomi's OAuth2Context on
 * every call, so pooled connections always authenticate with a current token. Token fetches are
 * serialized (single-flight) so a burst of new connections cannot stampede the token endpoint.
 */
public class BoomiRedisCredentialsProvider implements RedisCredentialsProvider {

    private static final Logger logger = Logger.getLogger(BoomiRedisCredentialsProvider.class.getName());

    private final AuthenticationType authType;
    private final String basicUser;
    private final String basicPassword;
    private final OAuth2Context oauth2Context;
    private final Object refreshLock = new Object();

    public BoomiRedisCredentialsProvider(AuthenticationType authType, String basicUser,
                                         String basicPassword, OAuth2Context oauth2Context) {
        if (authType == null) {
            throw new ConnectorException("Authentication type is missing. Set the connection's "
                    + "Authentication Type to None, Basic, or Microsoft Entra.");
        }
        if (authType == AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL && oauth2Context == null) {
            throw new ConnectorException("Microsoft Entra authentication requires an OAuth 2.0 "
                    + "credential component. Select one in the connection's Microsoft Entra OAuth 2.0 field.");
        }
        this.authType = authType;
        this.basicUser = basicUser;
        this.basicPassword = basicPassword;
        this.oauth2Context = oauth2Context;
    }

    @Override
    public RedisCredentials get() {
        switch (authType) {
            case NONE:
                return null;
            case BASIC:
                if (basicPassword == null) {
                    throw new ConnectorException("Basic authentication requires a password. "
                            + "Set the connection's Password field.");
                }
                return new DefaultRedisCredentials(basicUser, basicPassword.toCharArray());
            case MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL:
                return entraCredentials();
            default:
                throw new ConnectorException("Unsupported authentication type: " + authType);
        }
    }

    private RedisCredentials entraCredentials() {
        synchronized (refreshLock) {
            OAuth2Token token;
            try {
                token = oauth2Context.getOAuth2Token(false);
            } catch (IOException e) {
                logger.log(Level.WARNING, "Failed to obtain Microsoft Entra token from Boomi OAuth2 "
                        + "credential component", e);
                throw new ConnectorException("Could not obtain a Microsoft Entra access token. "
                        + "Verify the OAuth 2.0 credential component's client ID/secret, token URL, "
                        + "and scope. Cause: " + e.getMessage(), e);
            }
            if (token == null || token.getAccessToken() == null) {
                throw new ConnectorException("Boomi returned no Microsoft Entra access token. "
                        + "Re-authorize the OAuth 2.0 credential component and confirm the grant type "
                        + "is Client Credentials.");
            }
            String accessToken = token.getAccessToken();
            String oid = extractOid(accessToken);
            return new DefaultRedisCredentials(oid, accessToken.toCharArray());
        }
    }

    /** Extracts the 'oid' claim; Azure Cache for Redis requires it as the AUTH username. */
    private String extractOid(String token) {
        String[] parts = token.split("\\.");
        if (parts.length < 2) {
            throw new ConnectorException("Microsoft Entra access token is malformed (not a JWT). "
                    + "Confirm the OAuth 2.0 credential component targets the Entra token endpoint.");
        }
        String base64 = parts[1];
        switch (base64.length() % 4) {
            case 2: base64 += "=="; break;
            case 3: base64 += "="; break;
            default: break;
        }
        try {
            byte[] jsonBytes = Base64.getDecoder().decode(base64);
            String json = new String(jsonBytes, StandardCharsets.UTF_8);
            JsonObject jwt = JsonParser.parseString(json).getAsJsonObject();
            if (jwt.get("oid") == null || jwt.get("oid").isJsonNull()) {
                throw new ConnectorException("Microsoft Entra access token has no 'oid' claim. "
                        + "Ensure the App Registration has been granted a Redis data-access role so "
                        + "the token carries an object id.");
            }
            return jwt.get("oid").getAsString();
        } catch (ConnectorException e) {
            throw e;
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, "Failed to decode Microsoft Entra access token payload", e);
            throw new ConnectorException("Could not decode the Microsoft Entra access token payload. "
                    + "Cause: " + e.getMessage(), e);
        }
    }
}
