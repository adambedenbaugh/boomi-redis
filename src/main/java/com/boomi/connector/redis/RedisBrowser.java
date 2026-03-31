package com.boomi.connector.redis;

import java.net.URL;
import java.util.Collection;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.util.BaseBrowser;


public class RedisBrowser extends BaseBrowser {

	static Logger logger = Logger.getLogger(RedisBrowser.class.getName());
	protected RedisBrowser(RedisConnection conn) {
		super(conn);
	}

	@Override
	public ObjectTypes getObjectTypes() {
		ObjectTypes types = new ObjectTypes();
		ObjectType objectType = new ObjectType();
		switch (this.getContext().getOperationType()) {
			case GET:
				objectType.withId("Get")
					.withLabel("Get");
				return types.withTypes(objectType);

			case UPSERT:
				objectType.withId("Upsert")
					.withLabel("Upsert");
				return types.withTypes(objectType);

			case DELETE:
				objectType.withId("Delete")
					.withLabel("Delete");
				return types.withTypes(objectType);
				
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId,
			Collection<ObjectDefinitionRole> roles) {
		try {
			URL url = this.getClass().getClassLoader().getResource("schemas/" + objectTypeId.toLowerCase() + ".schema.json");
			if (url == null) {
				throw new ConnectorException("Schema file not found: schemas/" + objectTypeId.toLowerCase() + ".schema.json");
			}
			
			// Read the JSON schema file as a string. The JSON schemas are stored in src/main/resources/schemas
			java.io.InputStream inputStream = url.openStream();
			java.util.Scanner scanner = null;
			String jsonSchemaContent = "";
			try {
				scanner = new java.util.Scanner(inputStream, "UTF-8").useDelimiter("\\A");
				jsonSchemaContent = scanner.hasNext() ? scanner.next() : "";
			} finally {
				if (scanner != null) {
					scanner.close();
				}
				inputStream.close();
			}
			
			ObjectDefinition def = new ObjectDefinition();
			def.setElementName("");
			def.setJsonSchema(jsonSchemaContent);  // Set as string, not Document
			def.setOutputType(ContentType.JSON);
			def.setInputType(ContentType.JSON);
        	ObjectDefinitions defs = new ObjectDefinitions();
        	defs.getDefinitions().add(def);
			return defs;

        }
        catch (Exception e) {
            throw new ConnectorException(e);
        }
	}

	@Override
	public RedisConnection getConnection() {
		return (RedisConnection) super.getConnection();
	}
}