package software.amazon.rds.dbparametergroup.util;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    private List<List<Parameter>> setUpExpectedPartition(String [] parameterNames, int partitionSize) {
        List<List<Parameter>> partitions = new ArrayList<>();
        List<Parameter> partition = null;
        for (int i = 0; i < parameterNames.length ; i++) {
            if (partition == null || i%partitionSize == 0) {
                partition = new ArrayList<>();
            }
            partition.add(constructSimpleParameter(parameterNames[i]));
            if (partition.size() == partitionSize || i+1 == parameterNames.length) {
                partitions.add(partition);
            }
        }
        return partitions;
    }

    @Test
    public void test() {
        int partitionSize = 3;
        Map<String, Parameter> parametersToUpdate = setUpParametersToUpdate(new String[] {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"});
        List<String[]> dependantKeyGroups = new ArrayList<>();
        dependantKeyGroups.add(new String[] {"A", "C", "L"});
        dependantKeyGroups.add(new String[] {"E", "F"});
        dependantKeyGroups.add(new String[] {"I"});
        dependantKeyGroups.add(new String[] {"M"});

        List<List<Parameter>> partitions = ParameterGrouper.partition(parametersToUpdate, dependantKeyGroups, partitionSize);
        List<List<Parameter>> expectedPartitions = setUpExpectedPartition(new String[] {"A", "C", "I", "E", "F", "B", "D", "G", "H", "J"}, partitionSize);
        assertThat(partitions.size()).isEqualTo(4);
        assertThat(partitions).isEqualTo(expectedPartitions);
    }
}
