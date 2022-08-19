package software.amazon.rds.dbparametergroup.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.units.qual.A;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Builder;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.rds.dbparametergroup.ResourceModel;

class ParameterGrouperTest {

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


    private Parameter constructSimpleParameter(String parameterName) {
        return Parameter.builder()
                .parameterName(parameterName)
                .parameterValue("dummy-value")
                .build();
    }

    private Map<String, Parameter> setUpParametersToUpdate(String [] parameterNames) {
        Map<String, Parameter> parametersToUpdate = new LinkedHashMap<>();
        for (String parameterName : parameterNames) {
            parametersToUpdate.put(parameterName, constructSimpleParameter(parameterName));
        }

        return parametersToUpdate;
    }

    private List<List<Parameter>> setUpExpectedPartition(String [] parameterNames, Set<Integer> partitionBreak) {
        List<List<Parameter>> partitions = new ArrayList<>();
        List<Parameter> partition = new ArrayList<>();
        for (int i = 0; i < parameterNames.length ; i++) {
            partition.add(constructSimpleParameter(parameterNames[i]));
            if (partitionBreak.contains(i+1)) {
                partitions.add(partition);
                partition = new ArrayList<>();
            }
        }
        partitions.add(partition);
        return partitions;
    }

    @Test
    public void test_withDependentAndIndependentParameters() {
        int partitionSize = 3;
        Map<String, Parameter> parametersToUpdate = setUpParametersToUpdate(new String[] {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"});
        final List<Set<String>> DEPENDENCIES = ImmutableList.of(
                ImmutableSet.of("A", "C", "L"),
                ImmutableSet.of("E", "F"),
                ImmutableSet.of("I"),
                ImmutableSet.of("M")
        );

        List<List<Parameter>> partitions = ParameterGrouper.partition(parametersToUpdate, DEPENDENCIES, partitionSize);
        List<List<Parameter>> expectedPartitions = setUpExpectedPartition(new String[] {"A", "C", "B", "E", "F", "I", "D", "G", "H", "J"}, ImmutableSet.of(3,6,9));
        assertThat(partitions.size()).isEqualTo(4);
        assertThat(partitions).isEqualTo(expectedPartitions);
    }

    @Test
    public void test_withOnlyDependent() {
        int partitionSize = 3;
        Map<String, Parameter> parametersToUpdate = setUpParametersToUpdate(new String[] {"K", "A", "C", "E", "F", "I", "L", "M"});
        final List<Set<String>> DEPENDENCIES = ImmutableList.of(
                ImmutableSet.of("A", "C", "L"),
                ImmutableSet.of("E", "F"),
                ImmutableSet.of("I", "M"),
                ImmutableSet.of("K")
        );

        List<List<Parameter>> partitions = ParameterGrouper.partition(parametersToUpdate, DEPENDENCIES, partitionSize);
        ImmutableSet<Integer> a = ImmutableSet.of(0);
        List<List<Parameter>> expectedPartitions = setUpExpectedPartition(new String[] {"K", "A", "C", "L", "E", "F", "I", "M"}, ImmutableSet.of(1,4,6));
        assertThat(partitions.size()).isEqualTo(4);
        assertThat(partitions).isEqualTo(expectedPartitions);
    }

    @Test
    public void test_withOnlyIndependentParameters() {
        int partitionSize = 3;
        Map<String, Parameter> parametersToUpdate = setUpParametersToUpdate(new String[] {"N", "O", "P", "Q"});
        final List<Set<String>> DEPENDENCIES = ImmutableList.of(
                ImmutableSet.of("A", "C", "L"),
                ImmutableSet.of("E", "F"),
                ImmutableSet.of("I", "M"),
                ImmutableSet.of("K")
        );

        List<List<Parameter>> partitions = ParameterGrouper.partition(parametersToUpdate, DEPENDENCIES, partitionSize);
        ImmutableSet<Integer> a = ImmutableSet.of(0);
        List<List<Parameter>> expectedPartitions = setUpExpectedPartition(new String[] {"N", "O", "P", "Q"}, ImmutableSet.of(3));
        assertThat(partitions.size()).isEqualTo(2);
        assertThat(partitions).isEqualTo(expectedPartitions);
    }
}
