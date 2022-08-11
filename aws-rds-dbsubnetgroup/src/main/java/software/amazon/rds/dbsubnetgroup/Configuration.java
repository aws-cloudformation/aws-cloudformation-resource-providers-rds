package software.amazon.rds.dbsubnetgroup;


import java.util.HashMap;
import java.util.Map;

import com.amazonaws.util.CollectionUtils;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-rds-dbsubnetgroup.json");
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
