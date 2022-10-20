package software.amazon.rds.common.util;

import com.amazonaws.util.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DifferenceUtils {

    public static <E, T extends List<E>> List<E> diff(final T prev, final T upd) {
        if ((CollectionUtils.isNullOrEmpty(prev) && CollectionUtils.isNullOrEmpty(upd)) || (Objects.deepEquals(prev, upd))) {
            return DefaultSdkAutoConstructList.getInstance();
        }
        return upd;
    }

    public static <K, V, T extends Map<K, V>> Map<K, V> diff(final T prev, final T upd) {
        if ((MapUtils.isEmpty(prev) && MapUtils.isEmpty(upd)) || (Objects.deepEquals(prev, upd))) {
            return DefaultSdkAutoConstructMap.getInstance();
        }
        return upd;
    }

    public static <T> T diff(T prev, T upd) {
        if (Objects.deepEquals(prev, upd)) {
            return null;
        }
        return upd;
    }
}
