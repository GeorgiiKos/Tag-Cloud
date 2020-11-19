package de.hsma.tagcloud.controller;

import java.io.File;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class AdditionalResourceWebConfiguration implements WebMvcConfigurer {

	@Override
	public void addResourceHandlers(final ResourceHandlerRegistry registry) {
		File path = new File(LambdaController.CLOUD_PATH);

		if (!path.exists())
			path.mkdir();

		registry.addResourceHandler("/" + LambdaController.CLOUD_PATH + "**").addResourceLocations("file:" + LambdaController.CLOUD_PATH);
	}
	
}