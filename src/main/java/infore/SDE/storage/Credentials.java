package infore.SDE.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.data.Json;

import java.io.File;
import java.io.IOException;

public class Credentials {
    private String AWS_ACCESS_KEY_ID;
    private String AWS_SECRET_ACCESS_KEY;

    public Credentials(String filename) throws IOException {
        // here we read from a json
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(new File(filename));
        AWS_ACCESS_KEY_ID = node.get("AWS_ACCESS_KEY_ID").asText();
        AWS_SECRET_ACCESS_KEY = node.get("AWS_SECRET_ACCESS_KEY").asText();
    }
    // Getters and setters
    public String getAWS_ACCESS_KEY_ID() {
        return AWS_ACCESS_KEY_ID;
    }

    public void setAWS_ACCESS_KEY_ID(String AWS_ACCESS_KEY_ID) {
        this.AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID;
    }

    public String getAWS_SECRET_ACCESS_KEY() {
        return AWS_SECRET_ACCESS_KEY;
    }

    public void setAWS_SECRET_ACCESS_KEY(String AWS_SECRET_ACCESS_KEY) {
        this.AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY;
    }
}
