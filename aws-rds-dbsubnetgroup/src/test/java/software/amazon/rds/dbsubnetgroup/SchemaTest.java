package software.amazon.rds.dbsubnetgroup;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import software.amazon.rds.test.common.schema.ResourceDriftTestHelper;

public class SchemaTest {
    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

    @Test
    public void testDrift_DBSubnetGroupName_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBSubnetGroupName("DBSubnetGroupName")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBSubnetGroupName("dbsubnetgroupname")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }
}
