package software.amazon.rds.common.logging;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;

import org.assertj.core.util.Sets;
import org.json.JSONObject;

import lombok.RequiredArgsConstructor;

public class ReflectionToJson {
    final int MAX_DEPTH = 5;
    private Set<Object> visited;
    private Predicate<String> acceptParameter;

    public ReflectionToJson(Predicate<String> acceptParameter) {
        visited = Sets.newHashSet();
        this.acceptParameter = acceptParameter;
    }
    
    public JSONObject buildJson(Object obj) throws IllegalAccessException {
        return buildJson(obj, 0);
    }
    
    private JSONObject buildJson(Object obj, int depth) throws IllegalAccessException {
        if (obj == null || visited.contains(obj) || depth > MAX_DEPTH) {
            return null;
        }

        visited.add(obj);
        Class<?> objClass = obj.getClass();
        JSONObject result = new JSONObject();
        for (Field field : objClass.getDeclaredFields()) {
            processFieldRecursively(result, field, obj, depth);
        }
        return result;
    }

    private void processFieldRecursively(final JSONObject result, final Field field, final Object obj, final int depth) throws IllegalAccessException {
        //Getting field name and value
        field.setAccessible(true);
        String fieldName = field.getName();
        Object fieldValue = field.get(obj);
        field.setAccessible(false);
        if (shouldSkipParameter(fieldName) || fieldValue == null) {
            return;
        }
        //put into json if concrete. If not recurse by calling buildJson 
        Class<?> fieldType = fieldValue.getClass();
        if (fieldType.isPrimitive() || isConcreteObject(fieldValue)) {
            result.accumulate(fieldName, fieldValue);
        } else {
            result.accumulate(fieldName, buildJson(fieldValue, depth + 1));
        }
    }

    private boolean shouldSkipParameter(String parameterName) {
        return Arrays.stream(SkippedParameters.values()).anyMatch(parameter -> parameter.equals(parameterName));
    }

    private boolean isConcreteObject(Object obj) {
        return Arrays.stream(ConcreteTypes.values()).anyMatch(type -> type.isInstanceOf(obj));
    }


}
