package software.amazon.rds.dbclustersnapshot;

import org.json.JSONObject;
import org.json.JSONTokener;

class Configuration extends BaseConfiguration {

    public JSONObject resourceSchemaJsonObject() {
        return new JSONObject(
                new JSONTokener(this.getClass().getClassLoader().getResourceAsStream(schemaFilename)));
    }

    public Configuration() {
        super("aws-rds-dbclustersnapshot.json");
    }
}
