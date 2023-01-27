package software.amazon.rds.dbparametergroup;

import org.json.JSONObject;
import org.json.JSONTokener;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-rds-dbparametergroup.json");
    }

    public JSONObject resourceSchemaJsonObject() {
        return new JSONObject(
                new JSONTokener(this.getClass().getClassLoader().getResourceAsStream(schemaFilename)));
    }
}
