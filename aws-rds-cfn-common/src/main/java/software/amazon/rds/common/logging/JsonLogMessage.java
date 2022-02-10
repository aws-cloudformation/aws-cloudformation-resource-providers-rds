package software.amazon.rds.common.logging;

import java.io.IOException;
import java.util.LinkedHashMap;

import org.json.JSONObject;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import software.amazon.rds.common.printer.JsonPrinter;

@RequiredArgsConstructor(staticName = "newLogMessage")
public class JsonLogMessage implements LogMessage {

    @NonNull
    private final JsonPrinter jsonPrinter;
    private final LinkedHashMap<String, Object> jsonMessage = new LinkedHashMap<>();

    @Override
    public void append(Object object) throws IOException {
        JSONObject jsonObject = new JSONObject(jsonPrinter.print(object));
        putJsonObject(jsonObject);
    }

    @Override
    public void append(Throwable throwable) {
        JSONObject jsonObject = new JSONObject(jsonPrinter.print(throwable));
        putJsonObject(jsonObject);
    }

    @Override
    public void append(String message, Object object) {
        jsonMessage.put(message, object);
    }

    @SneakyThrows
    @Override
    public String toString() {
        return jsonPrinter.print(jsonMessage);
    }

    private void putJsonObject(final JSONObject jsonObject) {
        for (String field : jsonObject.keySet()) {
            jsonMessage.put(field, jsonObject.get(field));
        }
    }
}
