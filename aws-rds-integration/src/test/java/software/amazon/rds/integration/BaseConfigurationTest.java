package software.amazon.rds.integration;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static software.amazon.rds.integration.AbstractHandlerTest.INTEGRATION_ACTIVE_MODEL;

public class BaseConfigurationTest {

    @Test
    public void resourceDefinedTags_withNullTags_returnEmpty() {
        Configuration conf = new Configuration();
        Assertions.assertNull(conf.resourceDefinedTags(INTEGRATION_ACTIVE_MODEL.toBuilder().tags(null).build()));
    }

    @Test
    public void resourceDefinedTags_withNonNullTags_returnTags() {
        Configuration conf = new Configuration();
        Assertions.assertEquals(
                ImmutableMap.of("k1", "kv1"),
                conf.resourceDefinedTags(INTEGRATION_ACTIVE_MODEL)
        );
    }
}
