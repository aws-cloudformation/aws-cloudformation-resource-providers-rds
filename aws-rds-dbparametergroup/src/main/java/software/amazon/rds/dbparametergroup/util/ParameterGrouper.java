package software.amazon.rds.dbparametergroup.util;

import software.amazon.awssdk.services.rds.model.Parameter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ParameterGrouper {
    protected static final String COLLATION_SERVER = "collation_server";
    protected static final String CHARACTER_SET = "character_set";
    protected static final String GTID_MODE = "gtid-mode";
    protected static final String ENFORECE_GTID_CONSISTENCY = "enforce_gtid_consistency";

    private static void fillPartitionWithIndependentParameters(Map<String, Parameter> parametersToUpdate, List<Parameter> partition, int partitionSize) {
        Iterator<Map.Entry<String, Parameter>> iterator = parametersToUpdate.entrySet().iterator();
        while (partition.size() < partitionSize && iterator.hasNext()) {
            Map.Entry<String, Parameter> entry = iterator.next();
            partition.add(entry.getValue());
            iterator.remove();
        }
    }

    private static void addNewPartitionWithDependantParameters(List<List<Parameter>> partitions, List<List<Parameter>> dependantParameters, int partitionSize) {
        List<Parameter> newPartition = new ArrayList<>();
        List<List<Parameter>> addToPartition;
        int newPartitionSize = 0;
        do {
            addToPartition = new ArrayList<>();
            for (List<Parameter> parameters : dependantParameters) {
                if (newPartitionSize + parameters.size() <= partitionSize) {
                    addToPartition.add(parameters);
                    newPartitionSize += parameters.size();
                }
            }

            newPartition.addAll(
                    addToPartition.stream()
                            .flatMap(List::stream)
                            .collect(Collectors.toList())
            );
            dependantParameters.removeAll(addToPartition);
        } while (!addToPartition.isEmpty());
        partitions.add(newPartition);
    }

    private static List<List<Parameter>> getDependantParameters(Map<String, Parameter> parametersToUpdate, List<String[]> dependantParameterKeyGroups) {
        List<List<Parameter>> dependantParameters = new ArrayList<List<Parameter>>();

        for (String[] group : dependantParameterKeyGroups) {
            List<Parameter> subList = new ArrayList<>();
            for (String parameterName : group) {
                if (parametersToUpdate.get(parameterName) != null) {
                    subList.add(parametersToUpdate.remove(parameterName));
                }
            }
            if (subList.size() > 0) {
                dependantParameters.add(subList);
            }
        }
        return dependantParameters;
    }

    public static List<String[]> getKnownDependantKeyGroups() {
        List<String[]> knownDependantKeyGroups = new ArrayList<>();
        knownDependantKeyGroups.add(new String[] {COLLATION_SERVER, CHARACTER_SET});
        knownDependantKeyGroups.add(new String[] {GTID_MODE, ENFORECE_GTID_CONSISTENCY});
        return knownDependantKeyGroups;
    }

    public static List<List<Parameter>> partition(Map<String, Parameter> parametersToUpdate, List<String[]> dependantKeyGroups, int partitionSize) {
        int numberOfParameterToPartition = parametersToUpdate.size();
        List<List<Parameter>> dependantParameters = getDependantParameters(parametersToUpdate, dependantKeyGroups);
        List<List<Parameter>> partitions = new ArrayList<>();

        while (numberOfParameterToPartition > 0) {
            addNewPartitionWithDependantParameters(partitions, dependantParameters, partitionSize);
            fillPartitionWithIndependentParameters(parametersToUpdate, partitions.get(partitions.size()-1), partitionSize);
            numberOfParameterToPartition -= partitions.get(partitions.size()-1).size();
        }
        return partitions;
    }
}
