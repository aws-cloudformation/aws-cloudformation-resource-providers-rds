package software.amazon.rds.dbclusterparametergroup;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import software.amazon.rds.test.common.schema.ResourceDriftTestHelper;

public class SchemaTest {

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

    @Test
    public void testDrift_DBClusterParameterGroupName_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBClusterParameterGroupName("DBClusterParameterGroupName")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBClusterParameterGroupName("dbclusterparametergroupname")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }
}
