package com.example.kafkatwitterconsumer;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.util.Properties;

public class SentimentAnalyzer {

    public static void main(String[] args) {
        analyze();
    }

    public static void analyze() {
        Properties pipelineProps = new Properties();
        Properties tokenizerProps = new Properties();
        pipelineProps.setProperty("annotators", "parse, sentiment");
        pipelineProps.setProperty("parse.binaryTrees", "true");
        pipelineProps.setProperty("enforceRequirements", "false");
        tokenizerProps.setProperty("annotators", "tokenize ssplit");
        StanfordCoreNLP tokenizer = new StanfordCoreNLP(tokenizerProps);
        StanfordCoreNLP pipeline = new StanfordCoreNLP(pipelineProps);
        //String line = "Amazingly grateful beautiful friends are fulfilling an incredibly joyful accomplishment. What an truly terrible idea.";
        String line = "Amazingly grateful beautiful friends are fulfilling an incredibly joyful accomplishment. What an truly incredible idea.";

        Annotation annotation = tokenizer.process(line);
        pipeline.annotate(annotation);
        // normal output
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            String output = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
            System.out.println(output);
        }
    }
}
