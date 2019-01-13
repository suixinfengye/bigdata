package utils;

import org.ansj.app.keyword.KeyWordComputer;
import org.ansj.app.keyword.Keyword;
import org.ansj.recognition.impl.StopRecognition;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * feng
 * 19-1-12
 */
public class AnsjUtils implements Serializable {

    Logger logger = LoggerFactory.getLogger(AnsjUtils.class);
    private int keywordNum = 20;
    private KeyWordComputer kwc;
    private List<String> stopWords;
    private StopRecognition stop;

    public AnsjUtils() {
        kwc = new KeyWordComputer(keywordNum);
        stopWords = readStopLines();
        stop = new StopRecognition();
        stop.insertStopWords(stopWords);
    }

    private List<String> readStopLines() {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("stopword");
        List<String> results = null;
        try {
            results = IOUtils.readLines(inputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return results;
    }

    public List<Keyword> getKeywords(String contents) {
        String newContents = DicAnalysis.parse(contents)
                .recognition(stop)
                .toStringWithOutNature("|");
        return kwc.computeArticleTfidf(newContents);
    }

    public int getKeywordNum() {
        return keywordNum;
    }

    public void setKeywordNum(int keywordNum) {
        this.keywordNum = keywordNum;
    }

}
