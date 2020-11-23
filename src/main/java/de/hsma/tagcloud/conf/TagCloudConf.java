package de.hsma.tagcloud.conf;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotBlank;

@Configuration
@ConfigurationProperties(prefix = "tagcloud")
public class TagCloudConf {

    @NotBlank private String uploadPath;
    @NotBlank private String tagcloudPath;
    @NotBlank private String hadoopOutPath;

    public String getUploadPath() {
        return uploadPath;
    }

    public void setUploadPath(String uploadPath) {
        this.uploadPath = uploadPath;
    }

    public String getTagcloudPath() {
        return tagcloudPath;
    }

    public void setTagcloudPath(String tagcloudPath) {
        this.tagcloudPath = tagcloudPath;
    }

    public String getHadoopOutPath() {
        return hadoopOutPath;
    }

    public void setHadoopOutPath(String hadoopOutPath) {
        this.hadoopOutPath = hadoopOutPath;
    }

}
