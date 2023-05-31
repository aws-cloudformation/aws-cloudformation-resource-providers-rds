package software.amazon.rds.dbclusterendpoint;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import software.amazon.cloudformation.resource.ResourceTypeSchema;
import software.amazon.rds.common.util.DriftDetector;
import software.amazon.rds.common.util.Mutation;

class SchemaTest {

    private static final ResourceTypeSchema resourceSchema = ResourceTypeSchema.load(
            new Configuration().resourceSchemaJsonObject()
    );

    @Test
    void testDrift_DBClusterIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBClusterIdentifier("DBClusterIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBClusterIdentifier("dbclusteridentifier")
                .build();

        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_DBClusterEndpointIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBClusterEndpointIdentifier("DBClusterEndpointIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBClusterEndpointIdentifier("dbclusterendpointidentifier")
                .build();

        assertResourceNotDrifted(input, output, resourceSchema);
    }

    private static <T> void assertResourceNotDrifted(final T input, final T output, final ResourceTypeSchema schema) {
        final DriftDetector driftDetector = new DriftDetector(schema);
        final Map<String, Mutation> drift = driftDetector.detectDrift(input, output);
        assertThat(drift).isEmpty();
    }
}
