package software.amazon.rds.optiongroup;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import software.amazon.rds.test.common.schema.ResourceDriftTestHelper;

public class SchemaTest {

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

    @Test
    public void testDrift_OptionGroupName_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .optionGroupName("OptionGroupName")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .optionGroupName("optiongroupname")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }
}
