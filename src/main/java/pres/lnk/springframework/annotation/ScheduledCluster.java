package pres.lnk.springframework.annotation;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import pres.lnk.springframework.ScheduledClusterAnnotationBeanPostProcessor;
import pres.lnk.springframework.ScheduledMethodInvoker;

import java.lang.annotation.*;

/**
 * 集群定时任务控制
 * @author lnk
 * @see EnableScheduling
 * @see ScheduledAnnotationBeanPostProcessor
 * @see ScheduledClusterAnnotationBeanPostProcessor
 * @see ScheduledMethodInvoker
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ScheduledCluster {
    /**
     * 任务id，同一时间只执行同一id的任务
     * 默认为任务方法全名（包名+类名+方法名）
     * @return
     */
    String id() default "";

    /**
     * 是否忽略集群控制
     * 不加入集群定时任务控制，只按自己的工程实例规则执行
     * @return
     */
    boolean ignore() default false;
}
