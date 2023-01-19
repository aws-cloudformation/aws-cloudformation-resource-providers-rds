package software.amazon.rds.eventsubscription;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import software.amazon.rds.test.common.schema.ResourceDriftTestHelper;

public class SchemaTest {

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

    @Test
    public void testDrift_SubscriptionName_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .subscriptionName("SubscriptionName")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .subscriptionName("subscriptionname")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }
}
