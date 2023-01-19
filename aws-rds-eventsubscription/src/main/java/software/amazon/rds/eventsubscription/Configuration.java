package software.amazon.rds.eventsubscription;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.json.JSONTokener;

import com.amazonaws.util.CollectionUtils;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-rds-eventsubscription.json");
    }

    public JSONObject resourceSchemaJsonObject() {
        return new JSONObject(new JSONTokener(this.getClass().getClassLoader().getResourceAsStream(schemaFilename)));
    }

    public Map<String, String> resourceDefinedTags(final ResourceModel model) {
        if (CollectionUtils.isNullOrEmpty(model.getTags()))
            return null;

        final Map<String, String> tagMap = new HashMap<>();
        for (final Tag tag : model.getTags()) {
            tagMap.put(tag.getKey(), tag.getValue());
        }
        return tagMap;
    }
}
