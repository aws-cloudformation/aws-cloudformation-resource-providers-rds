package software.amazon.rds.customdbengineversion;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import software.amazon.rds.test.common.schema.ResourceDriftTestHelper;

public class SchemaTest {

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

    @Test
    public void testDrift_Engine_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .engine("Engine")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engine("engine")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_EngineVersion_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .engineVersion("EngineVersion")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engineVersion("engineversion")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_KmsKeyId_ExpandArn() {
        final ResourceModel input = ResourceModel.builder()
                .kMSKeyId("test-kms-key-id")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .kMSKeyId("arn:aws:kms:us-east-1:123456789012:key/test-kms-key-id")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

}
