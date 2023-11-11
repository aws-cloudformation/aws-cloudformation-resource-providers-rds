package software.amazon.rds.dbsnapshot;

import org.json.JSONObject;
import org.json.JSONTokener;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-rds-dbsnapshot.json");
    }

    public JSONObject resourceSchemaJsonObject() {
        return new JSONObject(
                new JSONTokener(this.getClass().getClassLoader().getResourceAsStream(schemaFilename)));
    }
}
