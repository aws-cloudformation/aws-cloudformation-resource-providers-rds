package software.amazon.rds.dbclusterendpoint;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import software.amazon.rds.test.common.schema.ResourceDriftTestHelper;

public class SchemaTest {

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

    @Test
    public void testDrift_DBClusterIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBClusterIdentifier("DBClusterIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBClusterIdentifier("dbclusteridentifier")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_DBClusterEndpointIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBClusterEndpointIdentifier("DBClusterEndpointIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBClusterEndpointIdentifier("dbclusterendpointidentifier")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }
}
