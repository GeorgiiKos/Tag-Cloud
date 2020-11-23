package de.hsma.tagcloud.controller;

import de.hsma.tagcloud.conf.TagCloudConf;
import de.hsma.tagcloud.service.BatchLaneService;
import de.hsma.tagcloud.service.FastLaneService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Controller
public class LambdaController {

    private final FastLaneService fastLane;
    private final BatchLaneService batchLane;
    private final TagCloudConf tagCloudConf;

    public LambdaController(FastLaneService fastLane, BatchLaneService batchLane, TagCloudConf tagCloudConf) {
        this.fastLane = fastLane;
        this.batchLane = batchLane;
        this.tagCloudConf = tagCloudConf;
    }

    @GetMapping("/upload")
    public String uploadFile() {
        return "upload";
    }

    @GetMapping("/document")
    public String selectDocument(Model model) {
        model.addAttribute("files", new File(tagCloudConf.getUploadPath()).list());
        return "document";
    }

    @PostMapping("/fastLane")
    public String processFile(MultipartFile file) throws IOException {
        fastLane.generateTagCloud(file);
        return "redirect:/overview";
    }

    @GetMapping(value = "/batchLane/document")
    public String executeDocument(@RequestParam String filename) throws InterruptedException, IOException, ClassNotFoundException {
        System.out.println(filename);
        this.batchLane.calculateDocument(filename);
        return "redirect:/overview";
    }

    @GetMapping("/batchLane/corpus")
    public String executeCorpus() throws InterruptedException, IOException, ClassNotFoundException {
        batchLane.calculateCorpus();
        return "redirect:/overview";
    }

    @GetMapping("/overview")
    public String overviewFiles(Model model) {
        String[] images = new File(tagCloudConf.getTagcloudPath()).list();
        if (images != null) {
            Arrays.sort(images, new TimestampDescComparator());
            model.addAttribute("files", images);
        } else {
            model.addAttribute("files", new String[]{});
        }
        return "overview";
    }

    public static class TimestampDescComparator implements Comparator<String> {
        final Pattern pattern = Pattern.compile("\\d{8}-\\d{9}");

        @Override
        public int compare(String o1, String o2) {
            Matcher m1 = pattern.matcher(o1);
            Matcher m2 = pattern.matcher(o2);
            m1.find();
            m2.find();
            return m1.group(0).compareTo(m2.group(0)) * (-1);
        }
    }

}
