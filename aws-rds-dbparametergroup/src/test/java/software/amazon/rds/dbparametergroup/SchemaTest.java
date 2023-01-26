package software.amazon.rds.dbparametergroup;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import software.amazon.rds.test.common.schema.ResourceDriftTestHelper;

public class SchemaTest {

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

    @Test
    public void testDrift_DBParameterGroupName_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBParameterGroupName("DBParameterGroupName")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBParameterGroupName("dbparametergroupname")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }
}
