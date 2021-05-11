package software.amazon.rds.dbparametergroup;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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

    public Map<String, String> resourceDefinedTags(final ResourceModel model) {
        return Optional.ofNullable(model.getTags())
                .orElse(Collections.emptyList())
                .stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue, (v1, v2) -> v2));
    }
}
