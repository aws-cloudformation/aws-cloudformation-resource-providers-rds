package software.amazon.rds.dbsecuritygroup;

import java.util.Map;
import java.util.stream.Collectors;

import junit.framework.Assert;

import org.junit.jupiter.api.Test;

public class ConfigurationTest extends AbstractTestBase {

    @Test
    public void resourceDefinedTags_Success() {
        final Configuration configuration = new Configuration();
        final Map<String, String> resourceTags = configuration.resourceDefinedTags(RESOURCE_MODEL);
        Assert.assertNotNull(resourceTags);
        Assert.assertEquals(
                TAG_SET.stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue)),
                resourceTags
        );
    }

    @Test
    public void resourceDefinedTags_EmptyTags() {
        final Configuration configuration = new Configuration();
        final Map<String, String> resourceTags = configuration.resourceDefinedTags(RESOURCE_MODEL_NO_TAGS);
        Assert.assertNull(resourceTags);
    }
}
