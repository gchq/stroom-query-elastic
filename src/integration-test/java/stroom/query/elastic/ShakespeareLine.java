package stroom.query.elastic;

public class ShakespeareLine {
    public static final String PLAY_NAME = "play_name";
    public static final String LINE_ID = "line_id";
    public static final String SPEAKER = "speaker";
    public static final String TEXT_ENTRY = "text_entry";
    public static final String SPEECH_NUMBER = "speech_number";

    private String playName;
    private String lineId;
    private String speaker;
    private String textEntry;
    private int speechNumber;

    public String getSpeaker() {
        return speaker;
    }

    public String getPlayName() {
        return playName;
    }

    public String getLineId() {
        return lineId;
    }

    public String getTextEntry() {
        return textEntry;
    }

    public int getSpeechNumber() {
        return speechNumber;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ShakespeareLine{");
        sb.append("playName='").append(playName).append('\'');
        sb.append(", lineId='").append(lineId).append('\'');
        sb.append(", speaker='").append(speaker).append('\'');
        sb.append(", textEntry='").append(textEntry).append('\'');
        sb.append(", speechNumber=").append(speechNumber);
        sb.append('}');
        return sb.toString();
    }

    public static class Builder {
        private final ShakespeareLine instance;

        public Builder() {
            this.instance = new ShakespeareLine();
        }

        public Builder speaker(final String value) {
            this.instance.speaker = value;
            return this;
        }

        public Builder textEntry(final String value) {
            this.instance.textEntry = value;
            return this;
        }

        public Builder playName(final String value) {
            this.instance.playName = value;
            return this;
        }

        public Builder lineId(final String value) {
            this.instance.lineId = value;
            return this;
        }

        public Builder speechNumber(final int value) {
            this.instance.speechNumber = value;
            return this;
        }

        public ShakespeareLine build() {
            return instance;
        }
    }
}
