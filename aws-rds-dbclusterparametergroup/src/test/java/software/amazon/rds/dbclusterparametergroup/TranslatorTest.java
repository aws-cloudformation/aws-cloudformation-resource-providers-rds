package software.amazon.rds.dbclusterparametergroup;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.cloudformation.exceptions.CfnInternalFailureException;

public class TranslatorTest {

    @Test
    public void describeDbClusterParametersFilteredRequest_throwIfParametersEmpty() {
        Assertions.assertThatThrownBy(() -> {
            Translator.describeDbClusterParametersFilteredRequest(
                    ResourceModel.builder().build(),
                    Collections.emptyList(),
                    null
            );
        }).isInstanceOf(CfnInternalFailureException.class);
    }

    @Test
    public void describeDbClusterParametersFilteredRequest_shouldSetParameterNameFilters() {
        final DescribeDbClusterParametersRequest request = Translator.describeDbClusterParametersFilteredRequest(
                ResourceModel.builder().dBClusterParameterGroupName("DBClusterParameterGroup").build(),
                ImmutableList.of("key1", "key2"),
                null
        );
        assertThat(request.filters().size()).isEqualTo(1);
        final Filter filter = request.filters().get(0);
        assertThat(filter.name()).isEqualTo("parameter-name");
        assertThat(filter.values()).containsExactlyInAnyOrder("key1", "key2");
    }
}
