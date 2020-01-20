package Stream;

import java.io.Serializable;

public class JavaRecord implements Serializable {
    private String word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
