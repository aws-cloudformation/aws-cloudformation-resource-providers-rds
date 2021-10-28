package software.amazon.rds.dbsubnetgroup;


import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.util.CollectionUtils;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-rds-dbsubnetgroup.json");
    }

    public Map<String, String> resourceDefinedTags(final ResourceModel resourceModel) {
        if (CollectionUtils.isNullOrEmpty(resourceModel.getTags()))
            return null;

        return resourceModel.getTags()
                .stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue, (v1, v2) -> v2));
    }
}
