package com.boomi.connector.redis.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.boomi.util.IOUtil;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RedisUtils {

	/**
	 * Utility to convert InputStream to String
	 * @param is
	 * @return
	 * @throws IOException
	 */
	public static String inputStreamToString(InputStream is) throws IOException {
		try (BufferedReader buffer = new BufferedReader(new InputStreamReader(is))) {
			return buffer.lines().collect(Collectors.joining("\n"));
		}
	}

	/**
	 * Utility to convert String to InputStream
	 * @param str
	 * @return
	 * @throws IOException
	 */
	public static InputStream stringToInputStream(String str) throws IOException {
		return new ByteArrayInputStream(str.getBytes());
	}

	public static Document parse(InputStream input) throws ParserConfigurationException, SAXException, IOException {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setNamespaceAware(true);
			return dbf.newDocumentBuilder().parse(input);
		}
		finally {
			IOUtil.closeQuietly(input);
		}
	}

	public static List<Element> getNodes(Document doc, String xpath) throws Exception {
		List<Element> elements = Collections.synchronizedList(new ArrayList<Element>());
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodes = (NodeList)xPath.evaluate(xpath, doc, XPathConstants.NODESET);
		for(int i=0;i<xpath.length();i++) {
			Element e = (Element) nodes.item(i);
			elements.add(e);
		}
		return elements;
	}

	public static String getFirstNodeTextContent(Document doc, String xpath) throws Exception {
		List<Element> elements = getNodes(doc, xpath);
		return elements.get(0).getTextContent();
	}



	public static String toString(Document doc) {
		try {
			StringWriter sw = new StringWriter();
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
			transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");

			transformer.transform(new DOMSource(doc), new StreamResult(sw));
			return sw.toString();
		} catch (Exception ex) {
			throw new RuntimeException("Error converting to String", ex);
		}
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
	 * @param jsonString
	 * @return JsonObject
	 * @throws Exception
	 */
	public static JsonObject parseJson(String jsonString) throws Exception {
		JsonElement element = JsonParser.parseString(jsonString);
		return element.getAsJsonObject();
	}

	/**
	 * Get string value from JSON object by key
	 * @param jsonObject
	 * @param key
	 * @return string value or null if not found
	 * @throws Exception
	 */
	public static String getJsonStringValue(JsonObject jsonObject, String key) throws Exception {
		JsonElement element = jsonObject.get(key);
		if (element != null && !element.isJsonNull()) {
			return element.getAsString();
		}
		return null;
	}


}
