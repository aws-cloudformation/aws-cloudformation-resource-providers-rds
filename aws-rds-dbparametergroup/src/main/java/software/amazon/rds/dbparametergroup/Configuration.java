package software.amazon.rds.dbparametergroup;

import java.util.Map;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.json.JSONTokener;

import com.amazonaws.util.CollectionUtils;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-rds-dbparametergroup.json");
    }

    public JSONObject resourceSchemaJsonObject() {
        return new JSONObject(
                new JSONTokener(this.getClass().getClassLoader().getResourceAsStream(schemaFilename)));
    }
}
