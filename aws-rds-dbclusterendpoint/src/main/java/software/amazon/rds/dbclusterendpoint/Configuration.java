package software.amazon.rds.dbclusterendpoint;

import org.json.JSONObject;
import org.json.JSONTokener;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-rds-dbclusterendpoint.json");
    }

    public JSONObject resourceSchemaJsonObject() {
        return new JSONObject(new JSONTokener(this.getClass().getClassLoader().getResourceAsStream(schemaFilename)));
    }
}
