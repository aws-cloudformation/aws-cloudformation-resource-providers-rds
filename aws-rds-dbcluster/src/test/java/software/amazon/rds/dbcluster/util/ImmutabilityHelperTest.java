package software.amazon.rds.dbcluster.util;


import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import lombok.Builder;
import software.amazon.rds.dbcluster.ResourceModel;

class ImmutabilityHelperTest {

    @Builder
    private static class ResourceModelTestCase {
        public ResourceModel previous;
        public ResourceModel desired;
        public boolean expect;
    }

    @Test
    public void test_isGlobalClusterMutable() {
        final List<ResourceModelTestCase> tests = Arrays.asList(
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().globalClusterIdentifier(null).build())
                        .desired(ResourceModel.builder().globalClusterIdentifier(null).build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().globalClusterIdentifier(null).build())
                        .desired(ResourceModel.builder().globalClusterIdentifier("global-cluster-identifier").build())
                        .expect(false)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().globalClusterIdentifier("global-cluster-identifier").build())
                        .desired(ResourceModel.builder().globalClusterIdentifier(null).build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().globalClusterIdentifier("global-cluster-identifier").build())
                        .desired(ResourceModel.builder().globalClusterIdentifier("global-cluster-identifier").build())
                        .expect(true)
                        .build()
        );

        for (final ResourceModelTestCase test : tests) {
            assertThat(ImmutabilityHelper.isGlobalClusterMutable(test.previous, test.desired)).isEqualTo(test.expect);
        }
    }

    @Test
    public void test_isEngineMutable() {
        final List<ResourceModelTestCase> tests = Arrays.asList(
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine(null).build())
                        .desired(ResourceModel.builder().engine(null).build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("mysql").build())
                        .desired(ResourceModel.builder().engine("mysql").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("mysql").build())
                        .desired(ResourceModel.builder().engine("postgres").build())
                        .expect(false)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("aurora").build())
                        .desired(ResourceModel.builder().engine("aurora-mysql").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("aurora").build())
                        .desired(ResourceModel.builder().engine("aurora").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("aurora").build())
                        .desired(ResourceModel.builder().engine("postgres").build())
                        .expect(false)
                        .build()
        );

        for (final ResourceModelTestCase test : tests) {
            assertThat(ImmutabilityHelper.isEngineMutable(test.previous, test.desired)).isEqualTo(test.expect);
        }
    }

    @Test
    public void test_isServerlessChangeMutable() {
        final List<ResourceModelTestCase> tests = Arrays.asList(
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engineMode("not-serverless").build())
                        .desired(ResourceModel.builder().engineMode("not-serverless").preferredMaintenanceWindow("maintenance-window").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engineMode("not-serverless").build())
                        .desired(ResourceModel.builder().engineMode("not-serverless").preferredBackupWindow("maintenance-window").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engineMode("not-serverless").build())
                        .desired(ResourceModel.builder().engineMode("not-serverless").enableIAMDatabaseAuthentication(true).build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engineMode("serverless").build())
                        .desired(ResourceModel.builder().engineMode("serverless").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engineMode("serverless").preferredMaintenanceWindow("old").build())
                        .desired(ResourceModel.builder().engineMode("serverless").preferredMaintenanceWindow("old").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engineMode("serverless").preferredMaintenanceWindow("old").build())
                        .desired(ResourceModel.builder().engineMode("serverless").preferredMaintenanceWindow("new").build())
                        .expect(false)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engineMode("serverless").preferredBackupWindow("old").build())
                        .desired(ResourceModel.builder().engineMode("serverless").preferredBackupWindow("old").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engineMode("serverless").preferredBackupWindow("old").build())
                        .desired(ResourceModel.builder().engineMode("serverless").preferredBackupWindow("new").build())
                        .expect(false)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engineMode("serverless").enableIAMDatabaseAuthentication(true).build())
                        .desired(ResourceModel.builder().engineMode("serverless").enableIAMDatabaseAuthentication(true).build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engineMode("serverless").enableIAMDatabaseAuthentication(false).build())
                        .desired(ResourceModel.builder().engineMode("serverless").enableIAMDatabaseAuthentication(true).build())
                        .expect(false)
                        .build()
        );

        for (final ResourceModelTestCase test : tests) {
            assertThat(ImmutabilityHelper.isServerlessChangeMutable(test.previous, test.desired)).isEqualTo(test.expect);
        }
    }
}
