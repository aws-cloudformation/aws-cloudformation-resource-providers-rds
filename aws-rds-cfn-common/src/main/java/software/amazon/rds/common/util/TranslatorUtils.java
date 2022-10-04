package software.amazon.rds.common.util;

import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TranslatorUtils {

    public static <E, T extends List<E>> List<E> difference(final T prev, final T upd) {
        if ((prev == null && upd == null) || (Objects.deepEquals(prev, upd))) {
            return DefaultSdkAutoConstructList.getInstance();
        }
        return upd;
    }

    public static <K, V, T extends Map<K, V>> Map<K, V> difference(final T prev, final T upd) {
        if ((prev == null && upd == null) || (Objects.deepEquals(prev, upd))) {
            return DefaultSdkAutoConstructMap.getInstance();
        }
        return upd;
    }

    public static <T> T difference(T prev, T upd) {
        if (Objects.deepEquals(prev, upd)) {
            return null;
        }
        return upd;
    }
}
