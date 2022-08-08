package software.amazon.rds.dbparametergroup;

import java.util.List;
import java.util.Map;

interface AbstractGrouper<K, V> {
    Iterable<List<V>> partition(Map<K, V> itemsToGroup, int partitionSize, List<String[]> groupings);
}
