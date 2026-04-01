package com.boomi.connector.redis.authentication;

import org.junit.Test;

import static com.boomi.connector.redis.authentication.AuthenticationType.*;
import static org.junit.Assert.*;

public class AuthenticationTypeTest {

    @Test
    public void testFromValueReturnsCorrectTypes() {
        assertEquals(NONE, fromValue("None"));
        assertEquals(BASIC, fromValue("Basic"));
        assertEquals(MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL, fromValue("MicrosoftEntraClientSecretCredential"));
    }

    @Test
    public void testFromValueIsCaseInsensitive() {
        assertEquals(NONE, fromValue("none"));
        assertEquals(BASIC, fromValue("BASIC"));
        assertEquals(MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL, fromValue("microsoftentraClientSecretCredential"));
    }

    @Test
    public void testFromValueDefaultsToNoneForUnknownInput() {
        assertEquals(NONE, fromValue("bogus"));
        assertEquals(NONE, fromValue("OAuth"));
        assertEquals(NONE, fromValue("token"));
    }

    @Test
    public void testFromValueDefaultsToNoneForNullOrEmpty() {
        assertEquals(NONE, fromValue(null));
        assertEquals(NONE, fromValue(""));
        assertEquals(NONE, fromValue("   "));
    }

    @Test
    public void testRequiresCredentials() {
        assertFalse(NONE.requiresCredentials());
        assertTrue(BASIC.requiresCredentials());
        assertTrue(MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL.requiresCredentials());
    }

    @Test
    public void testIsMicrosoftEntra() {
        assertFalse(NONE.isMicrosoftEntra());
        assertFalse(BASIC.isMicrosoftEntra());
        assertTrue(MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL.isMicrosoftEntra());
    }

    @Test
    public void testGetValue() {
        assertEquals("None", NONE.getValue());
        assertEquals("Basic", BASIC.getValue());
        assertEquals("MicrosoftEntraClientSecretCredential", MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL.getValue());
    }

    @Test
    public void testToString() {
        assertEquals("None", NONE.toString());
        assertEquals("Basic", BASIC.toString());
        assertEquals("MicrosoftEntraClientSecretCredential", MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL.toString());
    }
}
