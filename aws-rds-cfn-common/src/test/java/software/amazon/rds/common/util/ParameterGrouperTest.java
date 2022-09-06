package software.amazon.rds.common.util;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.rds.common.test.AbstractTestBase.ALPHA;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.rds.common.test.AbstractTestBase;

class ParameterGrouperTest {
    protected final String NON_PRESENT_DEPENDANT_PARAMETER = "this parameter won't be found";
    protected final int PARAMETER_NAME_LEN = 10;

    private Parameter constructSimpleParameter(String parameterName) {
        return Parameter.builder()
                .parameterName(parameterName)
                .parameterValue("dummy-value")
                .build();
    }

    private Map<String, Parameter> setUpParametersToUpdate(List<String> parameterNames) {
        Map<String, Parameter> parametersToUpdate = new LinkedHashMap<>();
        for (String parameterName : parameterNames) {
            parametersToUpdate.put(parameterName, constructSimpleParameter(parameterName));
        }

        return parametersToUpdate;
    }

    private List<List<Parameter>> setUpExpectedPartition(String [] parameterNames, List<Integer> partitionBreak) {
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

    private List<String> generateRandomStringList(int listLen, int wordLen, String alphabet) {
        return Stream.generate(() -> AbstractTestBase.randomString(wordLen, alphabet)).limit(listLen).collect(Collectors.toList());
    }

    private String[] buildMockExceptionArrayFromExceptionOrder(List<String> randomParameterKeys, List<Integer> expectationOrder) {
        List<String> expectation = new ArrayList<>();
        for (int index : expectationOrder) {
            expectation.add(randomParameterKeys.get(index));
        }
        return expectation.toArray(new String[expectation.size()]);
    }

    private List<Set<String>> buildMockDependencies(List<String> randomParameterKeys, List<List<Integer>> dependenciesAsIndexesOfParameters) {
        List<Set<String>> mockDependencies = new LinkedList<>();
        for (List<Integer> dependencyIndexs : dependenciesAsIndexesOfParameters) {
            Set<String> s = new LinkedHashSet<>();
            for (int index : dependencyIndexs) {
                if (index >= 0) {
                    s.add(randomParameterKeys.get(index));
                } else {
                    s.add(NON_PRESENT_DEPENDANT_PARAMETER);
                }
            }
            mockDependencies.add(s);
        }
        return mockDependencies;
    }

    public void test_helper(int partitionSize, List<List<Integer>> dependenciesAsIndexesOfParameters, List<Integer> expectationOrder, List<Integer> expectationPartitions) {
        int listLen = expectationOrder.size();
        List<String>  randomParameterKeys = generateRandomStringList(listLen, PARAMETER_NAME_LEN, ALPHA);
        Map<String, Parameter> parametersToUpdate = setUpParametersToUpdate(randomParameterKeys);
        final List<Set<String>> dependencies = buildMockDependencies(randomParameterKeys,
                dependenciesAsIndexesOfParameters
        );

        List<List<Parameter>> partitions = ParameterGrouper.partition(parametersToUpdate, dependencies, partitionSize);
        List<List<Parameter>> expectedPartitions = setUpExpectedPartition(buildMockExceptionArrayFromExceptionOrder(randomParameterKeys,
                expectationOrder
        ), expectationPartitions);
        assertThat(partitions.size()).isEqualTo(expectationPartitions.size()+1);
        assertThat(partitions).isEqualTo(expectedPartitions);
    }

    @Test
    public void test_withDependentAndIndependentParameters() {
        int partitionSize = 3;
        test_helper(partitionSize,
                ImmutableList.of(
                        ImmutableList.of(0, 2, -1),
                        ImmutableList.of(4, 5),
                        ImmutableList.of(8),
                        ImmutableList.of(-1)
                ),
                ImmutableList.of(
                        0,2,1,
                        4,5,8,
                        3,6,7,
                        9
                ), ImmutableList.of(3,6,9)
        );
    }

    @Test
    public void test_withOnlyDependent() {
        int partitionSize = 3;
        test_helper(partitionSize,
                ImmutableList.of(
                        ImmutableList.of(1, 2, 6),
                        ImmutableList.of(3, 4),
                        ImmutableList.of(5, 7),
                        ImmutableList.of(0)
                ),
                ImmutableList.of(
                        0,
                        1,2,6,
                        3,4,
                        5,7
                ), ImmutableList.of(1,4,6)
        );
    }

    @Test
    public void test_withOnlyIndependentParameters() {
        int partitionSize = 3;
        test_helper(partitionSize,
                ImmutableList.of(
                        ImmutableList.of(-1, -1, -1),
                        ImmutableList.of(-1, -1),
                        ImmutableList.of(-1, -1),
                        ImmutableList.of(-1)
                ),
                ImmutableList.of(
                        0,1,2,
                        3
                ), ImmutableList.of(3)
        );
    }
}
