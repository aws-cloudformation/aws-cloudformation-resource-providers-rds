package software.amazon.rds.dbparametergroup;

import software.amazon.awssdk.services.rds.model.Parameter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParameterGrouper implements AbstractGrouper<String, Parameter> {
    private void fillPartitionWithIndependentParameters(Map<String, Parameter> parametersToUpdate, List<Parameter> partition, int partitionSize) {
        for (String key : parametersToUpdate.keySet()) {
            if (partition.size() == partitionSize) {
                return;
            }
            partition.add(parametersToUpdate.remove(key));
        }
    }

    private void addNewPartitionWithDependantParameters(List<List<Parameter>> partitions, List<List<Parameter>> dependantParameters, int partitionSize) {
        boolean updateOccurred = false;
        List<Parameter> newPartition = new ArrayList<>();
        do {
            updateOccurred = false;
            for (int i = 0; i < dependantParameters.size(); i++) {
                List<Parameter> parametersToPartition = dependantParameters.get(i);
                if (newPartition.size() + parametersToPartition.size() <= partitionSize) {
                    newPartition.addAll(parametersToPartition);
                    dependantParameters.remove(parametersToPartition);
                    updateOccurred = true;
                    i--;
                }
            }
        } while (updateOccurred);
        partitions.add(newPartition);
    }

    private List<List<Parameter>> getDependantParameters(Map<String, Parameter> parametersToUpdate, List<String[]> dependantParameterKeys) {
        List<List<Parameter>> dependantParameters = new ArrayList<List<Parameter>>();

        for (String[] group : dependantParameterKeys) {
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

    @Override
    public List<List<Parameter>> partition(Map<String, Parameter> parametersToUpdate, int partitionSize, List<String[]> dependantKeyGroups) {
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