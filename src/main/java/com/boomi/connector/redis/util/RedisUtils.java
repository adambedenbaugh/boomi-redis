package com.boomi.connector.redis.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RedisUtils {

	/**
	 * Utility to convert InputStream to String
	 */
	public static String inputStreamToString(InputStream is) throws IOException {
		try (BufferedReader buffer = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
			return buffer.lines().collect(Collectors.joining("\n"));
		}
	}

	/**
	 * Utility to convert String to InputStream
	 */
	public static InputStream stringToInputStream(String str) throws IOException {
		return new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * Escapes SCAN/KEYS glob metacharacters (\, *, ?, [, ]) in a literal string so it can be safely
	 * combined with an appended wildcard without changing which keys the pattern matches.
	 */
	public static String escapeGlobMetacharacters(String literal) {
		if (literal == null) {
			return null;
		}
		StringBuilder escaped = new StringBuilder(literal.length());
		for (int i = 0; i < literal.length(); i++) {
			char c = literal.charAt(i);
			if (c == '\\' || c == '*' || c == '?' || c == '[' || c == ']') {
				escaped.append('\\');
			}
			escaped.append(c);
		}
		return escaped.toString();
	}

	/**
	 * Removes the prefix from a key
	 */
	public static String removePrefix(String key, String prefix) {
		if (key != null && prefix != null && key.startsWith(prefix)) {
			return key.substring(prefix.length());
		}
		return key;
	}

	/**
	 * Parse JSON string and return JsonObject
	 */
	public static JsonObject parseJson(String jsonString) throws Exception {
		JsonElement element = JsonParser.parseString(jsonString);
		return element.getAsJsonObject();
	}

	/**
	 * Get string value from JSON object by key, returns null if not found
	 */
	public static String getJsonStringValue(JsonObject jsonObject, String key) throws Exception {
		JsonElement element = jsonObject.get(key);
		if (element != null && !element.isJsonNull()) {
			return element.getAsString();
		}
		return null;
	}
}
