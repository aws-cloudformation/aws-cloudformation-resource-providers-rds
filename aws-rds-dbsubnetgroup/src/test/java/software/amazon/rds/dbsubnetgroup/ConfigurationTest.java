package software.amazon.rds.dbsubnetgroup;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;

class ConfigurationTest {

    @Test
    public void duplicateTagKeysLastWins() {
        final Configuration configuration = new Configuration();
        final String tagKeyA = "TagKeyA";
        final String tagValue1 = "TagValue1";
        final String tagValue2 = "TagValue2";

        final String tagKeyB = "TagKey3";
        final String tagValue3 = "TagValue3";

        final Map<String, String> tags = configuration.resourceDefinedTags(ResourceModel.builder().tags(Arrays.asList(
                new Tag(tagKeyA, tagValue1),
                new Tag(tagKeyA, tagValue2),
                new Tag(tagKeyB, tagValue3))).build());

        assertThat(tags).hasSize(2);
        assertThat(tags).containsExactly(
                Assertions.entry(tagKeyA, tagValue2),
                Assertions.entry(tagKeyB, tagValue3));
    }

    @Test
    public void noTags() {
        final Configuration configuration = new Configuration();
        final Map<String, String> tags = configuration.resourceDefinedTags(ResourceModel.builder().build());

        assertThat(tags).isNull();
    }

    @Test
    public void test_resourceDefinedTags_tagWithEmptyValue() {
        final Configuration configuration = new Configuration();
        final Map<String, String> tags = configuration.resourceDefinedTags(ResourceModel.builder()
                .tags(ImmutableList.of(new Tag("tag-key", null)))
                .build());
        Assertions.assertThat(tags).containsExactly(Assertions.entry("tag-key", null));
    }
}
