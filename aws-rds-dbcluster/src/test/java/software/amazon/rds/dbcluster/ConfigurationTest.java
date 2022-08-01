package software.amazon.rds.dbcluster;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

class ConfigurationTest {

    @Test
    public void test_resourceDefinedTags_tagWithEmptyValue() {
        final Configuration configuration = new Configuration();
        final Map<String, String> tags = configuration.resourceDefinedTags(ResourceModel.builder()
                .tags(ImmutableSet.of(new Tag("tag-key", null)))
                .build());
        Assertions.assertThat(tags).containsExactly(Assertions.entry("tag-key", null));
    }
}
