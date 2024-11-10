package jaso.log;

import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

public class LoggerSetup {
	public static void setUpLogging() {
        // Build a Log4j configuration programmatically
        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.setStatusLevel(org.apache.logging.log4j.Level.TRACE);
        builder.setConfigurationName("MyConfig");
        
        // Define a console appender
        AppenderComponentBuilder consoleAppender = builder.newAppender("stdout", "CONSOLE")
            .addAttribute("target", "SYSTEM_OUT");
        LayoutComponentBuilder consoleLayout = builder.newLayout("PatternLayout")
            .addAttribute("pattern", "%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n");
        consoleAppender.add(consoleLayout);
        builder.add(consoleAppender);
        
        // Set the console appender as the root appender
        builder.add(builder.newRootLogger(org.apache.logging.log4j.Level.INFO)
            .add(builder.newAppenderRef("stdout")));
        
        // Build and start the Log4j configuration
        Configuration config = builder.build();
        Configurator.initialize(config);
	}

}
