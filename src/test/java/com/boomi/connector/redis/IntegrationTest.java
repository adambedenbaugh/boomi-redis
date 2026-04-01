package com.boomi.connector.redis;

/**
 * Marker interface for integration tests that require a live Redis or Azure Redis instance.
 * Apply with @Category(IntegrationTest.class) on the test class.
 * These tests are excluded from the standard ./gradlew test run.
 * Run them separately with ./gradlew integrationTest.
 */
public interface IntegrationTest {}
