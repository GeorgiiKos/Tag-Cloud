package de.hsma.tagcloud.web;

import de.hsma.tagcloud.conf.TagCloudConf;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class AdditionalResourceWebConfiguration implements WebMvcConfigurer {

    final TagCloudConf tagCloudConf;

    public AdditionalResourceWebConfiguration(TagCloudConf tagCloudConf) {
        this.tagCloudConf = tagCloudConf;
    }

    @Override
    public void addResourceHandlers(final ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/" + tagCloudConf.getTagcloudPath() + "**").addResourceLocations("file:" + tagCloudConf.getTagcloudPath());
    }

}