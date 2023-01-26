package software.amazon.rds.dbinstance.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import lombok.Builder;
import software.amazon.rds.dbinstance.ResourceModel;

class ImmutabilityHelperTest {

    @Builder
    private static class EngineTestCase {
        public String previous;
        public String desired;
        public boolean expect;
    }

    @Builder
    private static class ResourceModelTestCase {
        public ResourceModel previous;
        public ResourceModel desired;
        public boolean expect;
    }

    @Test
    public void test_isUpgradeToOracleSE2() {
        final List<EngineTestCase> tests = Arrays.asList(
                EngineTestCase.builder().previous("oracle-se").desired("oracle-se2").expect(true).build(),
                EngineTestCase.builder().previous("oracle-se1").desired("oracle-se2").expect(true).build(),
                EngineTestCase.builder().previous(null).desired("oracle-se2").expect(false).build(),
                EngineTestCase.builder().previous(null).desired(null).expect(false).build(),
                EngineTestCase.builder().previous("oracle-se").desired(null).expect(false).build(),
                EngineTestCase.builder().previous("foo").desired("bar").expect(false).build()
        );
        for (final EngineTestCase test : tests) {
            final ResourceModel previous = ResourceModel.builder()
                    .engine(test.previous)
                    .build();
            final ResourceModel desired = ResourceModel.builder()
                    .engine(test.desired)
                    .build();
            assertThat(ImmutabilityHelper.isUpgradeToOracleSE2(previous, desired)).isEqualTo(test.expect);
        }
    }

    @Test
    public void test_isUpgradeToAuroraMySQL() {
        final List<EngineTestCase> tests = Arrays.asList(
                EngineTestCase.builder().previous("aurora").desired("aurora-mysql").expect(true).build(),
                EngineTestCase.builder().previous(null).desired("aurora-mysql").expect(false).build(),
                EngineTestCase.builder().previous("aurora").desired(null).expect(false).build(),
                EngineTestCase.builder().previous("aurora").desired("aurora-postgres").expect(false).build(),
                EngineTestCase.builder().previous(null).desired(null).expect(false).build(),
                EngineTestCase.builder().previous("foo").desired("bar").expect(false).build()
        );
        for (final EngineTestCase test : tests) {
            final ResourceModel previous = ResourceModel.builder()
                    .engine(test.previous)
                    .build();
            final ResourceModel desired = ResourceModel.builder()
                    .engine(test.desired)
                    .build();
            assertThat(ImmutabilityHelper.isUpgradeToAuroraMySQL(previous, desired)).isEqualTo(test.expect);
        }
    }

    @Test
    public void test_isPerformanceInsightsKMSKeyIdMutable() {
        final List<ResourceModelTestCase> tests = Arrays.asList(
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().build())
                        .desired(ResourceModel.builder().build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().performanceInsightsKMSKeyId("key-1").build())
                        .desired(ResourceModel.builder().build())
                        .expect(false)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().performanceInsightsKMSKeyId("key-1").build())
                        .desired(ResourceModel.builder().performanceInsightsKMSKeyId("key-2").build())
                        .expect(false)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().performanceInsightsKMSKeyId("key-1").build())
                        .desired(ResourceModel.builder().performanceInsightsKMSKeyId("key-1").build())
                        .expect(true)
                        .build()
        );
        for (final ResourceModelTestCase test : tests) {
            assertThat(ImmutabilityHelper.isPerformanceInsightsKMSKeyIdMutable(test.previous, test.desired)).isEqualTo(test.expect);
        }
    }

    @Test
    public void test_isEngineMutable() {
        final List<ResourceModelTestCase> tests = Arrays.asList(
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("mysql").build())
                        .desired(ResourceModel.builder().engine("mysql").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("aurora").build())
                        .desired(ResourceModel.builder().engine("aurora-mysql").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("aurora").build())
                        .desired(ResourceModel.builder().engine("aurora-postgres").build())
                        .expect(false)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("oracle-se").build())
                        .desired(ResourceModel.builder().engine("oracle-se2").build())
                        .expect(true)
                        .build()
        );
        for (final ResourceModelTestCase test : tests) {
            assertThat(ImmutabilityHelper.isChangeMutable(test.previous, test.desired)).isEqualTo(test.expect);
        }
    }

    @Test
    public void test_isAvailabilityZoneChangeMutable() {
        final List<ResourceModelTestCase> tests = Arrays.asList(
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().availabilityZone("old").build())
                        .desired(ResourceModel.builder().availabilityZone("old").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().availabilityZone("old").build())
                        .desired(ResourceModel.builder().availabilityZone("new").build())
                        .expect(false)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().availabilityZone("old").build())
                        .desired(ResourceModel.builder().availabilityZone(null).multiAZ(true).build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().multiAZ(true).build())
                        .desired(ResourceModel.builder().multiAZ(false).build())
                        .expect(true)
                        .build());
        for (final ResourceModelTestCase test : tests) {
            assertThat(ImmutabilityHelper.isChangeMutable(test.previous, test.desired)).isEqualTo(test.expect);
        }
    }

    @Test
    public void test_isSourceDBIdentifierMutable() {
        final List<ResourceModelTestCase> tests = Arrays.asList(
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().sourceDBInstanceIdentifier("old").build())
                        .desired(ResourceModel.builder().sourceDBInstanceIdentifier("old").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().sourceDBInstanceIdentifier("old").build())
                        .desired(ResourceModel.builder().sourceDBInstanceIdentifier("").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().sourceDBInstanceIdentifier("old").build())
                        .desired(ResourceModel.builder().sourceDBInstanceIdentifier(null).build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().sourceDBInstanceIdentifier("old").build())
                        .desired(ResourceModel.builder().sourceDBInstanceIdentifier("new").build())
                        .expect(false)
                        .build());
        for (final ResourceModelTestCase test : tests) {
            assertThat(ImmutabilityHelper.isChangeMutable(test.previous, test.desired)).isEqualTo(test.expect);
        }
    }

    @Test
    public void test_isDBSnapshotIdentifierMutable() {
        final List<ResourceModelTestCase> tests = Arrays.asList(
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().dBSnapshotIdentifier("old").build())
                        .desired(ResourceModel.builder().dBSnapshotIdentifier("old").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().dBSnapshotIdentifier("old").build())
                        .desired(ResourceModel.builder().dBSnapshotIdentifier("").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().dBSnapshotIdentifier("old").build())
                        .desired(ResourceModel.builder().dBSnapshotIdentifier(null).build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().dBSnapshotIdentifier("old").build())
                        .desired(ResourceModel.builder().dBSnapshotIdentifier("new").build())
                        .expect(false)
                        .build());
        for (final ResourceModelTestCase test : tests) {
            assertThat(ImmutabilityHelper.isChangeMutable(test.previous, test.desired)).isEqualTo(test.expect);
        }
    }

    @Test
    public void test_isDBClusterSnapshotIdentifierMutable() {
        final List<ResourceModelTestCase> tests = Arrays.asList(
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().dBClusterSnapshotIdentifier("old").build())
                        .desired(ResourceModel.builder().dBClusterSnapshotIdentifier("old").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().dBClusterSnapshotIdentifier("old").build())
                        .desired(ResourceModel.builder().dBClusterSnapshotIdentifier("").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().dBClusterSnapshotIdentifier("old").build())
                        .desired(ResourceModel.builder().dBClusterSnapshotIdentifier(null).build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().dBClusterSnapshotIdentifier("old").build())
                        .desired(ResourceModel.builder().dBClusterSnapshotIdentifier("new").build())
                        .expect(false)
                        .build());
        for (final ResourceModelTestCase test : tests) {
            assertThat(ImmutabilityHelper.isChangeMutable(test.previous, test.desired)).isEqualTo(test.expect);
        }
    }

    @Test
    public void test_isAvailabilityZoneMutable_RemoveAvailabilityZoneAttributeMultiAZ() {
        final ResourceModel previous = ResourceModel.builder()
                .availabilityZone("test-availability-zone")
                .multiAZ(true)
                .build();
        final ResourceModel desired = ResourceModel.builder()
                .availabilityZone(null)
                .multiAZ(true)
                .build();
        assertThat(ImmutabilityHelper.isAvailabilityZoneChangeMutable(previous, desired)).isTrue();
    }

    @Test
    public void test_isAvailabilityZoneMutable_RemoveAvailabilityZoneAttributeNoMultiAZ() {
        final ResourceModel previous = ResourceModel.builder()
                .availabilityZone("test-availability-zone")
                .multiAZ(null)
                .build();
        final ResourceModel desired = ResourceModel.builder()
                .availabilityZone(null)
                .multiAZ(null)
                .build();
        assertThat(ImmutabilityHelper.isAvailabilityZoneChangeMutable(previous, desired)).isFalse();
    }
}
