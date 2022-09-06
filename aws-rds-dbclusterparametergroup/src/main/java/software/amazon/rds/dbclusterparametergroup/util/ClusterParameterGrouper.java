package software.amazon.rds.dbclusterparametergroup.util;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import lombok.NonNull;
import software.amazon.awssdk.services.rds.model.Parameter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterParameterGrouper {
    private static Map<String, Set<String>> buildDependencyIndex(final List<Set<String>> dependencies) {
        final Map<String, Set<String>> dependencyIndex = new HashMap<>();
        for (final Set<String> group : dependencies) {
            for (final String groupParamName : group) {
                dependencyIndex.put(groupParamName, group);
            }
        }

        return dependencyIndex;
    }

    private static void addAsDependantParamGroup(
            final Map<String, Parameter> params,
            final List<List<Parameter>> paramGroups,
            final Set<String> added,
            final Set<String> group
    ) {
        final List<Parameter> groupParams = new ArrayList<>();
        for (final String groupParamName : group) {
            if (params.containsKey(groupParamName)) {
                assert (!added.contains(groupParamName));
                groupParams.add(params.get(groupParamName));
                added.add(groupParamName);
            }
        }
        paramGroups.add(groupParams);
    }

    private static void addAsIndependentParam(
                                               final Map<String, Parameter> params,
                                               final List<List<Parameter>> paramGroups,
                                               final Set<String> added,
                                               final String paramName
    ) {
        paramGroups.get(0).add(params.get(paramName));
        added.add(paramName);
    }

    private static List<List<Parameter>> partitionParamGroups(
            final List<List<Parameter>> paramGroups,
            final int partitionSize
    ) {
        final List<List<Parameter>> partitioned = new ArrayList<>();

        final int paramsProvided = paramGroups.stream().reduce(0, (acc, group) -> acc + group.size(), Integer::sum);

        PeekingIterator<List<Parameter>> paramGroupIterator = Iterators.peekingIterator(paramGroups.iterator());
        final List<Parameter> independentParams = paramGroupIterator.next();
        Iterator<Parameter> independentParamIterator = independentParams.iterator();

        int paramsAdded = 0;

        while (paramsAdded < paramsProvided) {
            final List<Parameter> currentPartition = new ArrayList<>();
            while (paramGroupIterator.hasNext()) {
                final int nextParamGroupSize = paramGroupIterator.peek().size();
                // Ensure a dependant group fits in a single partition.
                assert (nextParamGroupSize <= partitionSize);
                if (partitionSize - currentPartition.size() >= nextParamGroupSize) {
                    currentPartition.addAll(paramGroupIterator.next());
                    paramsAdded += nextParamGroupSize;
                } else {
                    break;
                }
            }
            while (currentPartition.size() < partitionSize && independentParamIterator.hasNext()) {
                currentPartition.add(independentParamIterator.next());
                paramsAdded++;
            }
            partitioned.add(currentPartition);
        }

        return partitioned;
    }

    public static List<List<Parameter>> partition(
            @NonNull final Map<String, Parameter> params,
            final List<Set<String>> dependencies,
            final int partitionSize
    ) {
        final Map<String, Set<String>> dependencyIndex = buildDependencyIndex(dependencies);

        final List<List<Parameter>> paramGroups = new ArrayList<>();

        // The first group conventionally stores all independent parameters.
        paramGroups.add(new ArrayList<>());

        final Set<String> added = new HashSet<>();

        for (Map.Entry<String, Parameter> entry : params.entrySet()) {
            final String paramName = entry.getKey();
            if (added.contains(paramName)) {
                continue;
            }
            if (dependencyIndex.containsKey(paramName)) {
                addAsDependantParamGroup(params, paramGroups, added, dependencyIndex.get(paramName));
            } else {
                addAsIndependentParam(params, paramGroups, added, paramName);
            }
        }

        return partitionParamGroups(paramGroups, partitionSize);
    }
}
