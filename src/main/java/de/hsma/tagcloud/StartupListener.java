package de.hsma.tagcloud;

import de.hsma.tagcloud.conf.TagCloudConf;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
public class StartupListener implements ApplicationListener<ContextRefreshedEvent> {

    final TagCloudConf tagCloudConf;

    public StartupListener(TagCloudConf tagCloudConf) {
        this.tagCloudConf = tagCloudConf;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        File uploadPath = new File(tagCloudConf.getUploadPath());
        File tagcloudPath = new File(tagCloudConf.getTagcloudPath());
        File hadoopOutPath = new File(tagCloudConf.getHadoopOutPath());

        if (!uploadPath.exists()) {
            uploadPath.mkdir();
        }
        if (!tagcloudPath.exists()) {
            tagcloudPath.mkdir();
        }
        if (!hadoopOutPath.exists()) {
            hadoopOutPath.mkdir();
        }
    }

}
