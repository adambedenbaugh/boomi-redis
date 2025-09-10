package com.boomi.connector.redis;

import java.net.URL;
import java.util.Collection;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.redis.util.RedisUtils;
import com.boomi.connector.util.BaseBrowser;

/**
 * 
 * @author anthony.rabiaza@gmail.com
 *
 */
public class RedisBrowser extends BaseBrowser {

	private static final String TYPE_ELEMENT = "type";

	protected RedisBrowser(RedisConnection conn) {
		super(conn);
	}

	@Override
	public ObjectTypes getObjectTypes() {
		try {
			URL url = this.getClass().getClassLoader().getResource("metadata.xml");
			Document typeDoc = RedisUtils.parse(url.openStream());
			NodeList typeList = typeDoc.getElementsByTagName(TYPE_ELEMENT);
			ObjectTypes types = new ObjectTypes();
			for (int i = 0; i < typeList.getLength(); ++i) {
				Element typeEl = (Element) typeList.item(i);
				String typeName = typeEl.getTextContent().trim();
				ObjectType type = new ObjectType();
				type.setId(typeName);
				types.getTypes().add(type);
			}
			return types;
		} catch (Exception e) {
			throw new ConnectorException(e);
		}
	}

	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId,
			Collection<ObjectDefinitionRole> roles) {
		try {
			URL url = this.getClass().getClassLoader().getResource(objectTypeId.toLowerCase() + ".xsd");
            Document defDoc = RedisUtils.parse(url.openStream());
            ObjectDefinitions defs = new ObjectDefinitions();
            ObjectDefinition def = new ObjectDefinition();
            def.setSchema(defDoc.getDocumentElement());
            def.setElementName(objectTypeId);
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

	public static void main(String[] args) {
		ObjectTypes o = new RedisBrowser(null).getObjectTypes();
		System.out.println(o);
	}
}