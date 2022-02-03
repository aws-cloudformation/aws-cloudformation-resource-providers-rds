package software.amazon.rds.common.printer;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

public class FilteredJsonPrinter implements JsonPrinter {
    final static String PWD = "pwd";
    public static final String STACK_TRACE = "StackTrace";

    @JsonFilter(PWD)
    static class PropertyFilterMixIn {
    }

    final private String[] filterFields;

    final protected ObjectMapper mapper;
    final protected ObjectWriter writer;

    public FilteredJsonPrinter(String... filterFields) {
        this.filterFields = filterFields;
        mapper = new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategy.UPPER_CAMEL_CASE)
                .enable(SerializationFeature.INDENT_OUTPUT)
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        mapper.addMixIn(Object.class, PropertyFilterMixIn.class);
        mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        FilterProvider filter = new SimpleFilterProvider()
                .addFilter(PWD, SimpleBeanPropertyFilter.serializeAllExcept(filterFields));
        writer = mapper.writer(filter);
    }

    @Override
    public String print(final Object obj) throws JsonProcessingException {
        return writer.writeValueAsString(obj);

    }

    @Override
    public String print(final Throwable throwable) {
        try {
            //throwable is not serializable
            String jsonThrowable = ReflectionToStringBuilder.toString(throwable, ToStringStyle.JSON_STYLE);
            JSONObject jsonObject = new JSONObject(jsonThrowable);
            jsonObject.append(STACK_TRACE, ExceptionUtils.getStackTrace(throwable));
            return jsonObject.toString();
        } catch (Exception exception) {
            return String.format("<failed to print object> %s", exception);
        }
    }
}
