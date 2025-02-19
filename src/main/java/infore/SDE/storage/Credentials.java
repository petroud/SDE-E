package infore.SDE.storage;

public class Credentials {
    private String AWS_ACCESS_KEY_ID;
    private String AWS_SECRET_ACCESS_KEY;

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
