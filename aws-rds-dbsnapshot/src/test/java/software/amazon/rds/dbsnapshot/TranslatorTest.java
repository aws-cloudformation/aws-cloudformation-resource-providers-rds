package software.amazon.rds.dbsnapshot;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;

public class TranslatorTest {

    @Test
    public void test_translateProcessorFeaturesFromSdk_Null() {
        assertThat(Translator.translateProcessorFeaturesFromSdk(null)).isNull();
    }

    @Test
    public void test_translateProcessorFeaturesFromSdk() {
        final String featureName = RandomStringUtils.randomAlphabetic(32);
        final String featureValue = RandomStringUtils.randomAlphabetic(32);
        List<ProcessorFeature> processorFeatures = Translator.translateProcessorFeaturesFromSdk(ImmutableList.of(
                software.amazon.awssdk.services.rds.model.ProcessorFeature.builder()
                        .name(featureName)
                        .value(featureValue)
                        .build()
        ));
        assertThat(processorFeatures).containsExactly(ProcessorFeature.builder()
                .name(featureName)
                .value(featureValue)
                .build());
    }

    @Test
    public void test_translateTagsFromSdk_Null() {
        assertThat(Translator.translateTagsFromSdk(null)).isNull();
    }

    @Test
    public void test_translateTagsFromSdk() {
        final String tagKey = RandomStringUtils.randomAlphabetic(32);
        final String tagValue = RandomStringUtils.randomAlphabetic(32);
        List<Tag> tags = Translator.translateTagsFromSdk(ImmutableList.of(
                software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tagKey)
                        .value(tagValue)
                        .build()
        ));
        assertThat(tags).containsExactly(Tag.builder()
                .key(tagKey)
                .value(tagValue)
                .build());
    }

    @Test
    public void test_translateTagsToSdk_Null() {
        assertThat(Translator.translateTagsToSdk(null)).isEqualTo(Collections.emptySet());
    }

    @Test
    public void test_translateTagsToSdk() {
        final String tagKey = RandomStringUtils.randomAlphabetic(32);
        final String tagValue = RandomStringUtils.randomAlphabetic(32);
        Set<software.amazon.awssdk.services.rds.model.Tag> tags = Translator.translateTagsToSdk(ImmutableList.of(
                Tag.builder()
                        .key(tagKey)
                        .value(tagValue)
                        .build()
        ));
        assertThat(tags).containsExactly(software.amazon.awssdk.services.rds.model.Tag.builder()
                .key(tagKey)
                .value(tagValue)
                .build());
    }

    @Test
    public void test_translateDBSnapshotFromSdk_SetInstanceCreateTime() {
        assertThat(Translator.translateDBSnapshotFromSdk(software.amazon.awssdk.services.rds.model.DBSnapshot.builder()
                        .instanceCreateTime(Instant.now())
                        .build())
                .getInstanceCreateTime()
        ).isNotBlank();
    }

    @Test
    public void test_translateDBSnapshotFromSdk_SetOriginalSnapshotCreateTime() {
        assertThat(Translator.translateDBSnapshotFromSdk(software.amazon.awssdk.services.rds.model.DBSnapshot.builder()
                        .originalSnapshotCreateTime(Instant.now())
                        .build())
                .getOriginalSnapshotCreateTime()
        ).isNotBlank();
    }

    @Test
    public void test_translateDBSnapshotFromSdk_SetSnapshotCreateTime() {
        assertThat(Translator.translateDBSnapshotFromSdk(software.amazon.awssdk.services.rds.model.DBSnapshot.builder()
                        .snapshotCreateTime(Instant.now())
                        .build())
                .getSnapshotCreateTime()
        ).isNotBlank();
    }

    @Test
    public void test_translateDBSnapshotFromSdk_SetSnapshotDatabaseTime() {
        assertThat(Translator.translateDBSnapshotFromSdk(software.amazon.awssdk.services.rds.model.DBSnapshot.builder()
                        .snapshotDatabaseTime(Instant.now())
                        .build())
                .getSnapshotDatabaseTime()
        ).isNotBlank();
    }
}
