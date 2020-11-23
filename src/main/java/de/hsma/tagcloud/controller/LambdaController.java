package de.hsma.tagcloud.controller;

import de.hsma.tagcloud.conf.TagCloudConf;
import de.hsma.tagcloud.service.BatchLaneService;
import de.hsma.tagcloud.service.FastLaneService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;

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
    public String upload() {
        return "upload";
    }

    @GetMapping("/document")
    public String document(Model model) {
        model.addAttribute("files", new File(tagCloudConf.getUploadPath()).list());
        return "document";
    }

    @PostMapping("/fastLane")
    public String process(MultipartFile file) throws IOException {
        fastLane.generateTagCloud(file);
        return "redirect:/overview";
    }

    @GetMapping(value = "/batchLane/document")
    public String executeDoc(@RequestParam String filename) throws InterruptedException, IOException, ClassNotFoundException {
        System.out.println(filename);
        this.batchLane.calculateDocument(filename);
        return "redirect:/overview";
    }

    @GetMapping("/batchLane/corpus")
    public String execute() throws InterruptedException, IOException, ClassNotFoundException {
        batchLane.calculateCorpus();
        return "redirect:/overview";
    }

    @GetMapping("/overview")
    public String overview(Model model) {
        model.addAttribute("files", new File(tagCloudConf.getTagcloudPath()).list());
        return "overview";
    }

}
