package ML.typical;

import java.io.Serializable;

public class SMSWord implements Serializable {
    private String label;
    private String[] words;

    public SMSWord(String label, String[] words) {
        this.label = label;
        this.words = words;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String[] getWords() {
        return words;
    }

    public void setWords(String[] words) {
        this.words = words;
    }
}
