package de.hsma.tagcloud.service;

import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.RectangleBackground;
import com.kennycason.kumo.font.scale.SqrtFontScalar;
import com.kennycason.kumo.nlp.FrequencyAnalyzer;
import com.kennycason.kumo.palette.LinearGradientColorPalette;
import de.hsma.tagcloud.controller.LambdaController;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.springframework.stereotype.Service;

import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class BatchLaneService {

    public void calculateCorpus() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        conf.set("numDocuments", String.valueOf(new File("upload/").list().length));
        final long currentTime = System.currentTimeMillis();

        // job 1
        Job job1 = Job.getInstance(conf, "Normalized Corpus");
        job1.setJarByClass(BatchLaneService.class);
        job1.setMapperClass(FilenameMapper.class);
        job1.setReducerClass(CorpusReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path("upload/*.txt"));
        FileOutputFormat.setOutputPath(job1, new Path("hadoop-out/nc-output_" + currentTime));

        job1.waitForCompletion(true);

        // job 2
        Job job2 = Job.getInstance(conf, "Descending sort");
        job2.setJarByClass(BatchLaneService.class);
        job2.setMapperClass(SwapMapper.class);
        job2.setReducerClass(Reducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setNumReduceTasks(1);

        job2.setSortComparatorClass(DescendingComparator.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job2, new Path("hadoop-out/nc-output_" + currentTime));
        FileOutputFormat.setOutputPath(job2, new Path("hadoop-out/cs-output_" + currentTime));

        job2.waitForCompletion(true);
        this.generateTagCloud("hadoop-out/cs-output_" + currentTime, "norm_corpus");
    }

    public void calculateDocument(String filename) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        final long currentTime = System.currentTimeMillis();

        // job 1
        Job job1 = Job.getInstance(conf, "Word count");
        job1.setJarByClass(BatchLaneService.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path("upload/" + filename));
        FileOutputFormat.setOutputPath(job1, new Path("hadoop-out/wc-output_" + currentTime));

        // job 2
        Job job2 = Job.getInstance(conf, "Document frequency");
        job2.setJarByClass(BatchLaneService.class);
        job2.setMapperClass(FilenameMapper.class);
        job2.setReducerClass(DocumentFrequencyReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path("upload/*.txt"));
        FileOutputFormat.setOutputPath(job2, new Path("hadoop-out/df-output_" + currentTime));

        job1.waitForCompletion(true);
        job2.waitForCompletion(true);

        // job 3
        Job job3 = Job.getInstance(conf, "Normalized Document");
        job3.setJarByClass(BatchLaneService.class);
        MultipleInputs.addInputPath(job3, new Path("hadoop-out/wc-output_" + currentTime), SequenceFileInputFormat.class, MapperA.class);
        MultipleInputs.addInputPath(job3, new Path("hadoop-out/df-output_" + currentTime), SequenceFileInputFormat.class, MapperB.class);
        job3.setReducerClass(DocumentReducer.class);
        FileOutputFormat.setOutputPath(job3, new Path("hadoop-out/dn-output_" + currentTime));
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        job3.setOutputFormatClass(SequenceFileOutputFormat.class);

        job3.waitForCompletion(true);

        // job 4
        Job job4 = Job.getInstance(conf, "Descending sort");
        job4.setJarByClass(BatchLaneService.class);
        job4.setMapperClass(SwapMapper.class);
        job4.setReducerClass(Reducer.class);
        job4.setMapOutputKeyClass(IntWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);

        job4.setNumReduceTasks(1);

        job4.setSortComparatorClass(DescendingComparator.class);
        job4.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job4, new Path("hadoop-out/dn-output_" + currentTime));
        FileOutputFormat.setOutputPath(job4, new Path("hadoop-out/ds-output_" + currentTime));

        job4.waitForCompletion(true);
        this.generateTagCloud("hadoop-out/ds-output_" + currentTime, "norm_" + filename);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Pattern pattern = Pattern.compile("(\\b[^\\s]+\\b)");
            Matcher matcher = pattern.matcher(value.toString());
            while (matcher.find()) {
                word.set(value.toString().substring(matcher.start(), matcher.end()).toLowerCase());
                context.write(word, one);
            }
        }
    }

    public static class FilenameMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Pattern pattern = Pattern.compile("(\\b[^\\s]+\\b)");
            Matcher matcher = pattern.matcher(value.toString());
            while (matcher.find()) {
                FileSplit fileSplit = (FileSplit) context.getInputSplit();
                String filename = fileSplit.getPath().getName();

                word.set(value.toString().substring(matcher.start(), matcher.end()).toLowerCase());
                context.write(word, new Text(filename));
            }
        }
    }

    public static class MapperA extends Mapper<Text, IntWritable, Text, Text> {
        private Text word = new Text();

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            word.set("A" + value);
            context.write(key, word);
        }
    }

    public static class MapperB extends Mapper<Text, IntWritable, Text, Text> {
        private Text word = new Text();

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            word.set("B" + value);
            context.write(key, word);
        }
    }

    public static class CorpusReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> uniqueValues = new ArrayList<>();
            int counter = 0;
            int numDocuments = Integer.parseInt(context.getConfiguration().get("numDocuments"));

            for (Text val : values) {
                counter++;
                String stringVal = val.toString();
                if (!uniqueValues.contains(stringVal)) {
                    uniqueValues.add(stringVal);
                }
            }
            result.set((int) Math.round((double) counter / ((double) uniqueValues.size() / (double) numDocuments)));
            context.write(key, result);
        }
    }

    public static class DocumentReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int valueA = 0;
            int valueB = 0;

            for (Text textValue : values) {
                String stringValue = textValue.toString();
                if (stringValue.startsWith("A")) {
                    valueA = Integer.parseInt(stringValue.substring(1));
                } else if (stringValue.startsWith("B")) {
                    valueB = Integer.parseInt(stringValue.substring(1));
                }
            }
            result.set((int) Math.round((double) valueA / (double) valueB));
            context.write(key, result);
        }

    }

    public static class DocumentFrequencyReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> uniqueValues = new ArrayList<>();

            for (Text val : values) {
                String stringVal = val.toString();
                if (!uniqueValues.contains(stringVal)) {
                    uniqueValues.add(stringVal);
                }
            }
            result.set(uniqueValues.size());
            context.write(key, result);
        }
    }

    public static class SwapMapper extends Mapper<Text, IntWritable, IntWritable, Text> {

        @Override
        public void map(Text word, IntWritable count, Context context) throws IOException, InterruptedException {
            context.write(count, word);
        }

    }

    public static class DescendingComparator extends WritableComparator {
        public DescendingComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return super.compare(a, b) * (-1);
        }
    }

    public static class SwapReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(), key);
        }
    }


    private void generateTagCloud(String folder, String filename) throws IOException {
        List<WordFrequency> resultFreq = this.parseResult(folder);
        List<WordFrequency> resultFilter = this.filterResult(resultFreq);

        final FrequencyAnalyzer frequencyAnalyzer = new FrequencyAnalyzer();
        frequencyAnalyzer.setWordFrequenciesToReturn(300);  // not applied
        frequencyAnalyzer.setMinWordLength(4);

        final List<WordFrequency> wordFrequencies = frequencyAnalyzer.loadWordFrequencies(resultFilter);

        final Dimension dimension = new Dimension(600, 600);
        final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.RECTANGLE);
        wordCloud.setPadding(0);
        wordCloud.setBackground(new RectangleBackground(dimension));
        wordCloud.setColorPalette(new LinearGradientColorPalette(Color.RED, Color.BLUE, Color.GREEN, 30, 30));
        wordCloud.setFontScalar(new SqrtFontScalar(10, 40));
        wordCloud.build(wordFrequencies);
        wordCloud.writeToFile(LambdaController.CLOUD_PATH + filename + ".png");
    }

    private List<WordFrequency> parseResult(String folder) {
        final int linesToRead = 800;
        int lineCounter = 0;

        List<WordFrequency> wordFrequencies = new ArrayList<>();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(folder + "/part-r-00000"));
            String line;
            while ((line = reader.readLine()) != null && lineCounter < linesToRead) {
                String[] splitLine = line.split("\t");
                wordFrequencies.add(new WordFrequency(splitLine[1], Integer.parseInt(splitLine[0])));
                lineCounter++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return wordFrequencies;
    }

    private List<WordFrequency> filterResult(List<WordFrequency> wordFrequencies) {
        final int minWordLength = 4;
        List<WordFrequency> filterFrequencies = new ArrayList<>();
        for (WordFrequency wf : wordFrequencies) {
            if (wf.getWord().length() > minWordLength) {
                filterFrequencies.add(wf);
            }
        }
        return filterFrequencies;
    }
}
