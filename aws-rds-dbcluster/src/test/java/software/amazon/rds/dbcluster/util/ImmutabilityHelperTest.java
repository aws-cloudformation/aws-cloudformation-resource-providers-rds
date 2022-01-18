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
                        .expect(false)
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

}
