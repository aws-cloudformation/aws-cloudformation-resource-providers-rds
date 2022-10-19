package software.amazon.rds.dbclusterparametergroup;

import com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.cloudformation.exceptions.CfnInternalFailureException;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

public class TranslatorTest {

    @Test
    public void describeDbClusterParametersFilteredRequest_throwIfParametersEmpty() {
        Assertions.assertThatThrownBy(() -> {
            Translator.describeDbClusterParametersFilteredRequest(
                    ResourceModel.builder()
                            .parameters(Collections.emptyMap())
                            .build(),
                    null
            );
        }).isInstanceOf(CfnInternalFailureException.class);
    }

    @Test
    public void describeDbClusterParametersFilteredRequest_shouldSetParameterNameFilters() {
        final DescribeDbClusterParametersRequest request = Translator.describeDbClusterParametersFilteredRequest(
                ResourceModel.builder()
                        .dBClusterParameterGroupName("DBClusterParameterGroup")
                        .parameters(ImmutableMap.of("key1", "value1", "key2", "value2"))
                        .build(),
                null);
        assertThat(request.filters().size()).isEqualTo(1);
        final Filter filter = request.filters().get(0);
        assertThat(filter.name()).isEqualTo("parameter-name");
        assertThat(filter.values()).containsExactlyInAnyOrder("key1", "key2");
    }
}
