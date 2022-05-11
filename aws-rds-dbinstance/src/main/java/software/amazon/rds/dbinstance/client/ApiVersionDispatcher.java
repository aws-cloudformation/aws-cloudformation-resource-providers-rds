package software.amazon.rds.dbinstance.client;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiPredicate;

public class ApiVersionDispatcher<M, C> {

    /**
     * Testers are kept in a {@code SortedMap} so we keep a semantics of a natural sorting by {@code ApiVersion}.
     */
    private final Map<ApiVersion, BiPredicate<M, C>> versionTesters = new TreeMap<>(((Comparator<ApiVersion>) ApiVersion::compareTo).reversed());

    public ApiVersionDispatcher<M, C> register(final ApiVersion version, final BiPredicate<M, C> tester) {
        versionTesters.put(version, tester);
        return this;
    }

    /**
     * This method resolves a particular instance of Model and Context into an {@code ApiVersion}.
     * The resolution is traversing the version tester tree bottom-up (reversed order of {@code ApiVersion} enum).
     * @param model An instance of Model.
     * @param context An instance of Context.
     * @return Returns the highest {@code ApiVersion} matching the input model and context. If no matchers triggered, DEFAULT version is returned (hence no need to register a match-all tester for DEFAULT version).
     */
    public ApiVersion dispatch(final M model, final C context) {
        for (final Map.Entry<ApiVersion, BiPredicate<M, C>> entry : versionTesters.entrySet()) {
            if (entry.getValue().test(model, context)) {
                return entry.getKey();
            }
        }
        return ApiVersion.DEFAULT;
    }
}
