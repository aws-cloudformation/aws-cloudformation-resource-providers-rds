package software.amazon.rds.customdbengineversion;

import org.json.JSONObject;
import org.json.JSONTokener;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-rds-customdbengineversion.json");
    }

    public JSONObject resourceSchemaJsonObject() {
        return new JSONObject(new JSONTokener(this.getClass().getClassLoader().getResourceAsStream(schemaFilename)));
    }
}
