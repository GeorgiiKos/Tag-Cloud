package de.hsma.tagcloud.service;

import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.CircleBackground;
import com.kennycason.kumo.font.scale.SqrtFontScalar;
import com.kennycason.kumo.nlp.FrequencyAnalyzer;
import com.kennycason.kumo.palette.ColorPalette;
import de.hsma.tagcloud.conf.TagCloudConf;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Service
public class FastLaneService {

    private final TagCloudConf tagCloudConf;

    public FastLaneService(TagCloudConf tagCloudConf) {
        this.tagCloudConf = tagCloudConf;
    }

    public void generateTagCloud(MultipartFile file) throws IOException {
        final FrequencyAnalyzer frequencyAnalyzer = new FrequencyAnalyzer();
        frequencyAnalyzer.setWordFrequenciesToReturn(300);
        frequencyAnalyzer.setMinWordLength(4);

        final String timestamp = new SimpleDateFormat("yyyMMdd-HHmmssSSS").format(new Date());
        final String imageName = file.getOriginalFilename().substring(0, file.getOriginalFilename().lastIndexOf('.')) + "_" + timestamp + ".png";

        List<String> texts = new ArrayList<>();
        texts.add(new String(file.getBytes()));
        final List<WordFrequency> wordFrequencies = frequencyAnalyzer.load(texts);

        final Dimension dimension = new Dimension(600, 600);
        final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
        wordCloud.setPadding(2);
        wordCloud.setBackground(new CircleBackground(300));
        wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
        wordCloud.setFontScalar(new SqrtFontScalar(8, 50));
        wordCloud.build(wordFrequencies);
        wordCloud.writeToFile(tagCloudConf.getTagcloudPath() + imageName);
        file.transferTo(new File(new File(tagCloudConf.getUploadPath() + file.getOriginalFilename()).getAbsolutePath()));
    }
}
