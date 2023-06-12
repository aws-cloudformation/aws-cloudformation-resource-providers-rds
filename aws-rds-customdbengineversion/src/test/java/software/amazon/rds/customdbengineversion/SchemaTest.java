package software.amazon.rds.customdbengineversion;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import software.amazon.cloudformation.resource.ResourceTypeSchema;
import software.amazon.rds.common.util.DriftDetector;
import software.amazon.rds.common.util.Mutation;

class SchemaTest {

    private static final ResourceTypeSchema resourceSchema = ResourceTypeSchema.load(
            new Configuration().resourceSchemaJsonObject()
    );

    @Test
    void testDrift_Engine_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .engine("Engine")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engine("engine")
                .build();

        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_EngineVersion_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .engineVersion("EngineVersion")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engineVersion("engineversion")
                .build();

        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_KmsKeyId_ExpandArn() {
        final ResourceModel input = ResourceModel.builder()
                .kMSKeyId("test-kms-key-id")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .kMSKeyId("arn:aws:kms:us-east-1:123456789012:key/test-kms-key-id")
                .build();
        assertResourceNotDrifted(input, output, resourceSchema);
    }

    private static <T> void assertResourceNotDrifted(final T input, final T output, final ResourceTypeSchema schema) {
        final DriftDetector driftDetector = new DriftDetector(schema);
        final Map<String, Mutation> drift = driftDetector.detectDrift(input, output);
        assertThat(drift).isEmpty();
    }
}
