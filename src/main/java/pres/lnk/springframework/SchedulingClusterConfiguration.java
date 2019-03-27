package pres.lnk.springframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.scheduling.config.TaskManagementConfigUtils;

/**
 * @Author lnk
 * @Date 2019/3/26
 */
@Configuration
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class SchedulingClusterConfiguration {
    private final static Logger logger = LoggerFactory.getLogger(SchedulingClusterConfiguration.class);

    @Bean(name = TaskManagementConfigUtils.SCHEDULED_ANNOTATION_PROCESSOR_BEAN_NAME)
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public ScheduledAnnotationBeanPostProcessor scheduledAnnotationProcessor() {
        logger.debug("启用集群环境的定时任务");
        return new ScheduledClusterAnnotationBeanPostProcessor();
    }

    @Bean
    @ConditionalOnMissingBean(AbstractScheduler.class)
    public AbstractScheduler getScheduler() {
        return new LocalSchedulerImpl();
    }
}
