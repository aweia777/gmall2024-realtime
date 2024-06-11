package com.atguigu.gmall.realtime.common.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class IkUtil {
    public static List<String> IkSplit(String keyword) {
        StringReader stringReader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        Lexeme next = null;
        ArrayList<String> strings = new ArrayList<>();
        try {
            next = ikSegmenter.next();
            while (next != null){
                strings.add(next.getLexemeText());
                next  = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return strings;
    }
    public static void main(String[] args) throws IOException {
        String s = "Apple 苹果15 双卡双待";
        StringReader stringReader = new StringReader(s);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        Lexeme next = ikSegmenter.next();
        while (next != null){
            System.out.println(next.getLexemeText());
            next  = ikSegmenter.next();
        }
    }
}
