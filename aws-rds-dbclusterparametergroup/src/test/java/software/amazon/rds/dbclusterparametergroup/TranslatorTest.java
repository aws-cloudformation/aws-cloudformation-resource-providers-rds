package software.amazon.rds.dbclusterparametergroup;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.Filter;

import static org.assertj.core.api.Assertions.assertThat;

public class TranslatorTest {

    @Test
    public void describeDBClusterParameterGroup_shouldNotHaveFilters_whenParametersAreEmpty() {
        final DescribeDbClusterParametersRequest request = Translator.describeDbClusterParametersRequest(
                ResourceModel.builder()
                        .dBClusterParameterGroupName("DBClusterParameterGroup")
                        .build(),
                null);
        assertThat(request.hasFilters()).isFalse();
    }

    @Test
    public void describeDBClusterParameterGroup_shouldHaveFilters_whenParametersArePresent() {
        final DescribeDbClusterParametersRequest request = Translator.describeDbClusterParametersRequest(
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
