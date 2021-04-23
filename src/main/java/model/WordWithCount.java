package model;

/**
 * WordWithCount is a POJO class which contains word and associated count the occurrence of the word.
 */
public class WordWithCount {

    public String word;
    public long count;

    public WordWithCount() {}

    public WordWithCount(String word, long count) {
        this.word = word;
        this.count = count;
    }


    @Override
    public String toString() {
        return word + " : " + count;
    }
}
