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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Supplies Redis credentials per physical connection for None / Basic / Microsoft Entra auth.
 * For Entra, delegates token acquisition (and expiry-aware refresh) to Boomi's OAuth2Context on
 * every call, so pooled connections always authenticate with a current token. Token fetches are
 * serialized (single-flight) so a burst of new connections cannot stampede the token endpoint.
 *
 * <p><b>Entra token fetches always flow through the newest execution's OAuth2Context.</b> A
 * shared pooled client ({@code JedisPool}/{@code JedisCluster}) outlives the execution that built
 * it, but the provider baked into that client captured the <i>building</i> execution's
 * OAuth2Context - and a completed execution's context stops yielding fresh tokens, which surfaces
 * on a real Atom as {@code WRONGPASS invalid username-password pair} once the original token
 * expires (observed 2026-08-11: second pooled execution failed after the ~1h token lifetime while
 * unpooled executions kept working). Every execution constructs a provider even when it reuses a
 * shared client, so the constructor registers that execution's live context in
 * {@link #CURRENT_CONTEXTS} keyed by the credential identity, and {@link #entraCredentials()}
 * reads the newest one at fetch time.
 */
public class BoomiRedisCredentialsProvider implements RedisCredentialsProvider {

    private static final Logger logger = Logger.getLogger(BoomiRedisCredentialsProvider.class.getName());

    /**
     * Newest OAuth2Context seen per credential identity (clientId + clientSecret + accessTokenUrl).
     * Bounded by the number of distinct OAuth credential configurations ever seen in the JVM.
     * Never logged - the key contains the client secret.
     */
    private static final ConcurrentMap<String, OAuth2Context> CURRENT_CONTEXTS = new ConcurrentHashMap<>();

    private final AuthenticationType authType;
    private final String basicUser;
    private final String basicPassword;
    private final OAuth2Context oauth2Context;
    private final String contextKey;
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
        if (authType == AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL) {
            this.contextKey = contextKey(oauth2Context);
            // This constructor runs once per execution (even when the execution reuses a shared
            // pooled client), so the registered context is always the live execution's.
            CURRENT_CONTEXTS.put(contextKey, oauth2Context);
        } else {
            this.contextKey = null;
        }
    }

    private static String contextKey(OAuth2Context ctx) {
        return nullSafe(ctx.getClientId()) + '\n' + nullSafe(ctx.getClientSecret())
                + '\n' + nullSafe(ctx.getAccessTokenUrl());
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    /** Test-only: resets the per-credential context registry (static state isolation). */
    static void clearCurrentContexts() {
        CURRENT_CONTEXTS.clear();
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
            // Always fetch through the newest execution's context for this credential (see class
            // javadoc); fall back to the constructor's context only if the registry was cleared.
            OAuth2Context current = CURRENT_CONTEXTS.getOrDefault(contextKey, oauth2Context);
            OAuth2Token token;
            try {
                token = current.getOAuth2Token(false);
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
        try {
            byte[] jsonBytes = Base64.getUrlDecoder().decode(base64);
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
