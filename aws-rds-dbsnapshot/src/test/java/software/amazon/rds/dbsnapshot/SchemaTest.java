package software.amazon.rds.dbsnapshot;

import org.assertj.core.api.Assertions;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import software.amazon.rds.test.common.schema.ResourceDriftTestHelper;

public class SchemaTest {
    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

    @Test
    public void testDrift_DBInstanceIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBInstanceIdentifier("DBInstanceIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBInstanceIdentifier("dbinstanceidentifier")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_DBSnapshotIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBSnapshotIdentifier("DBSnapshotIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBSnapshotIdentifier("dbsnapshotidentifier")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

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

    @Test
    public void testDrift_KmsKeyId_ExpandArn() {
        final ResourceModel input = ResourceModel.builder()
                .kmsKeyId("test-kms-key-id")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .kmsKeyId("arn:aws:kms:us-east-1:123456789012:key/test-kms-key-id")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_EngineVersion_MajorVersionOnly() {
        final ResourceModel input = ResourceModel.builder()
                .engineVersion("5")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engineVersion("5.6.40")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_EngineVersion_MajorAndMinorVersion() {
        final ResourceModel input = ResourceModel.builder()
                .engineVersion("5.6")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engineVersion("5.6.40")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_EngineVersion_MinorMismatch_ShouldDrift() {
        final ResourceModel input = ResourceModel.builder()
                .engineVersion("5.6")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engineVersion("5.7.40")
                .build();
        Assertions.assertThatThrownBy(() -> {
            ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    public void testDrift_SourceDBSnapshotIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .sourceDBSnapshotIdentifier("SourceDBSnapshotIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .sourceDBSnapshotIdentifier("sourcedbsnapshotidentifier")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }
}
