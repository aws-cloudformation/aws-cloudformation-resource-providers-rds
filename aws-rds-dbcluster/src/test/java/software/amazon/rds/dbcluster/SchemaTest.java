package software.amazon.rds.dbcluster;

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
    void testDrift_DBClusterParameterGroupName_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBClusterParameterGroupName("DBClusterParameterGroupName")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBClusterParameterGroupName("dbclusterparametergroupname")
                .build();

        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_DBSubnetGroupName_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBSubnetGroupName("DBSubnetGroupName")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBSubnetGroupName("dbsubnetgroupname")
                .build();

        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_SnapshotIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .snapshotIdentifier("SnapshotIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .snapshotIdentifier("snapshotidentifier")
                .build();

        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_SourceDBClusterIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .sourceDBClusterIdentifier("SourceDBClusterIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .sourceDBClusterIdentifier("sourcedbclusteridentifier")
                .build();

        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_KmsKeyId_ExpandArn() {
        final ResourceModel input = ResourceModel.builder()
                .kmsKeyId("test-kms-key-id")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .kmsKeyId("arn:aws:kms:us-east-1:123456789012:key/test-kms-key-id")
                .build();
        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_PerformanceInsightsKmsKeyId_ExpandArn() {
        final ResourceModel input = ResourceModel.builder()
                .performanceInsightsKmsKeyId("test-kms-key-id")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .performanceInsightsKmsKeyId("arn:aws:kms:us-east-1:123456789012:key/test-kms-key-id")
                .build();
        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_MasterUserSecret_KmsKeyId_ExpandArn() {
        final ResourceModel input = ResourceModel.builder()
                .masterUserSecret(MasterUserSecret.builder()
                        .kmsKeyId("test-kms-key-id")
                        .build())
                .build();
        final ResourceModel output = ResourceModel.builder()
                .masterUserSecret(MasterUserSecret.builder()
                        .kmsKeyId("arn:aws:kms:us-east-1:123456789012:key/test-kms-key-id")
                        .build())
                .build();
        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_PreferredMaintenanceWindow_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .preferredMaintenanceWindow("Sat:06:01-Sat:08:01")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .preferredMaintenanceWindow("sat:06:01-sat:08:01")
                .build();
        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_EnableHttpEndpoint_Serverless_Drifted() {
        final ResourceModel input = ResourceModel.builder()
                .enableHttpEndpoint(true)
                .engineMode("serverless")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .enableHttpEndpoint(false)
                .engineMode("serverless")
                .build();
        Assertions.assertThatThrownBy(() -> {
            assertResourceNotDrifted(input, output, resourceSchema);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    void testDrift_EnableHttpEndpoint_Aurora_Postgresql_Drifted() {
        final ResourceModel input = ResourceModel.builder()
                .enableHttpEndpoint(true)
                .engine("aurora-postgresql")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .enableHttpEndpoint(false)
                .engine("aurora-postgresql")
                .build();
        Assertions.assertThatThrownBy(() -> {
            assertResourceNotDrifted(input, output, resourceSchema);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    void testDrift_EnableHttpEndpoint_Aurora_Mysql_Drifted() {
        final ResourceModel input = ResourceModel.builder()
                .enableHttpEndpoint(true)
                .engine("aurora-mysql")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .enableHttpEndpoint(false)
                .engine("aurora-mysql")
                .build();
        Assertions.assertThatThrownBy(() -> {
            assertResourceNotDrifted(input, output, resourceSchema);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    void testDrift_EnableHttpEndpoint_Provisioned() {
        final ResourceModel input = ResourceModel.builder()
                .enableHttpEndpoint(true)
                .engineMode("provisioned")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .enableHttpEndpoint(false)
                .engineMode("provisioned")
                .build();
        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_EnableHttpEndpoint_NoEngineMode() {
        final ResourceModel input = ResourceModel.builder()
                .enableHttpEndpoint(true)
                .build();
        final ResourceModel output = ResourceModel.builder()
                .enableHttpEndpoint(false)
                .build();
        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_EngineVersion_MajorVersionOnly() {
        final ResourceModel input = ResourceModel.builder()
                .engineVersion("5")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engineVersion("5.6.40")
                .build();
        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_EngineVersion_MajorAndMinorVersion() {
        final ResourceModel input = ResourceModel.builder()
                .engineVersion("5.6")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engineVersion("5.6.40")
                .build();
        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_EngineVersion_MinorMismatch_ShouldDrift() {
        final ResourceModel input = ResourceModel.builder()
                .engineVersion("5.6")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engineVersion("5.7.40")
                .build();
        Assertions.assertThatThrownBy(() -> {
            assertResourceNotDrifted(input, output, resourceSchema);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    void testDrift_Engine_CaseMismatch_ShouldNotDrift() {
        final ResourceModel input = ResourceModel.builder()
                .engine("aurora-MySQL")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engine("aurora-mysql")
                .build();

        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_NetworkType_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .networkType("IPV4")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .networkType("ipv4")
                .build();
        assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    void testDrift_StorageType_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .storageType("GP3")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .storageType("gp3")
                .build();
        assertResourceNotDrifted(input, output, resourceSchema);
    }

    private static <T> void assertResourceNotDrifted(final T input, final T output, final ResourceTypeSchema schema) {
        final DriftDetector driftDetector = new DriftDetector(schema);
        final Map<String, Mutation> drift = driftDetector.detectDrift(input, output);
        assertThat(drift).isEmpty();
    }
}
