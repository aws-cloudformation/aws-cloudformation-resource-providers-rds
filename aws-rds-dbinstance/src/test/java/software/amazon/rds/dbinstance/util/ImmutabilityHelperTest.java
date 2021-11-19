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
    public void test_isAZMutable() {
        final List<ResourceModelTestCase> tests = Arrays.asList(
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().build())
                        .desired(ResourceModel.builder().build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().availabilityZone("multi-az").build())
                        .desired(ResourceModel.builder().availabilityZone("multi-az").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().availabilityZone("foo").build())
                        .desired(ResourceModel.builder().multiAZ(true).build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().availabilityZone("foo").build())
                        .desired(ResourceModel.builder().multiAZ(false).build())
                        .expect(false)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().build())
                        .desired(ResourceModel.builder().multiAZ(true).availabilityZone("multi-az").build())
                        .expect(false)
                        .build()
        );
        for (final ResourceModelTestCase test : tests) {
            assertThat(ImmutabilityHelper.isAZMutable(test.previous, test.desired)).isEqualTo(test.expect);
        }
    }

    @Test
    public void test_isPerformanceInsightsMutable() {
        final List<ResourceModelTestCase> tests = Arrays.asList(
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().build())
                        .desired(ResourceModel.builder().build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().enablePerformanceInsights(false).build())
                        .desired(ResourceModel.builder().build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().enablePerformanceInsights(true).build())
                        .desired(ResourceModel.builder().enablePerformanceInsights(false).build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().enablePerformanceInsights(true).build())
                        .desired(ResourceModel.builder().enablePerformanceInsights(true).build())
                        .expect(true)
                        .build()
        );
        for (final ResourceModelTestCase test : tests) {
            assertThat(ImmutabilityHelper.isPerformanceInsightsMutable(test.previous, test.desired)).isEqualTo(test.expect);
        }
    }

    @Test
    public void test_isChangeImmutable() {
        final List<ResourceModelTestCase> tests = Arrays.asList(
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("mysql").build())
                        .desired(ResourceModel.builder().engine("mysql").build())
                        .expect(false)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("aurora").build())
                        .desired(ResourceModel.builder().engine("aurora-mysql").build())
                        .expect(false)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("aurora").build())
                        .desired(ResourceModel.builder().engine("aurora-postgres").build())
                        .expect(true)
                        .build(),
                ResourceModelTestCase.builder()
                        .previous(ResourceModel.builder().engine("oracle-se").build())
                        .desired(ResourceModel.builder().engine("oracle-se2").build())
                        .expect(false)
                        .build()
        );
        for (final ResourceModelTestCase test : tests) {
            assertThat(ImmutabilityHelper.isChangeImmutable(test.previous, test.desired)).isEqualTo(test.expect);
        }
    }
}
