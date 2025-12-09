package com.viettel.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark methods that should be scheduled for specific systems
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SystemScheduled {
    
    /**
     * The key/identifier for this scheduled task
     */
    String key();
    
    /**
     * The datasource key to use for this task
     */
    String datasource() default "";
}
