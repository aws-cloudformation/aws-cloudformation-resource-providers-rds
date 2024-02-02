package software.amazon.rds.common.printer;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class FilteredJsonPrinter implements JsonPrinter {
    final static String PWD = "pwd";
    public static final String STACK_TRACE = "StackTrace";
    private static final String EMPTY_JSON = "{}";

    @JsonFilter(PWD)
    static class PropertyFilterMixIn {
    }

    final protected ObjectMapper mapper;
    final protected ObjectWriter writer;

    public FilteredJsonPrinter(String... filterFields) {
        mapper = new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.UPPER_CAMEL_CASE)
                .enable(SerializationFeature.INDENT_OUTPUT)
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        mapper.registerModule(new JavaTimeModule());
        mapper.addMixIn(Object.class, PropertyFilterMixIn.class);
        mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        FilterProvider filter = new SimpleFilterProvider()
                .addFilter(PWD, SimpleBeanPropertyFilter.serializeAllExcept(filterFields));
        writer = mapper.writer(filter);
    }

    @Override
    public String print(final Object obj) throws JsonProcessingException {
        return obj == null ? EMPTY_JSON : writer.writeValueAsString(obj);
    }

    @Override
    public String print(final Throwable throwable) {
        try {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw, true);
            throwable.printStackTrace(pw);
            return sw.getBuffer().toString();
        } catch (Exception exception) {
            return String.format("<failed to print object> %s", exception);
        }
    }
}
