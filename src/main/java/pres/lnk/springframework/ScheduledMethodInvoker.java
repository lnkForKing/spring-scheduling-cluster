package pres.lnk.springframework;

import org.springframework.aop.support.AopUtils;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;
import pres.lnk.springframework.annotation.ScheduledCluster;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * 定时任务执行器
 * @Author lnk
 * @Date 2018/2/28
 */
public class ScheduledMethodInvoker {

    /** 任务对象 */
    private final Object target;

    /** 定时任务的执行方法 */
    private final Method method;

    /** 任务id，用来做锁 */
    private String taskId;

    /**
     * 是否忽略集群控制
     * @see ScheduledCluster#ignore()
     */
    private boolean ignore = false;


    /** 定时任务调度器 */
    private AbstractScheduler scheduler;

    private StringValueResolver embeddedValueResolver;

    public ScheduledMethodInvoker(Object target, Method method, AbstractScheduler scheduler, StringValueResolver embeddedValueResolver) {
        this.target = target;
        this.method = AopUtils.selectInvocableMethod(method, target.getClass());
        this.scheduler = scheduler != null ? scheduler : new LocalSchedulerImpl();
        this.embeddedValueResolver = embeddedValueResolver;

        init();
    }

    public void invoke() {
        try {
            if (ignore) {
                //设置该任务不受集群调试器控制，直接执行
                ReflectionUtils.makeAccessible(this.method);
                this.method.invoke(this.target);
                return;
            }

            int maxLevel = scheduler.getMaxAliveLevel();
            if (scheduler.getLevel() == 0 || scheduler.getLevel() > maxLevel) {
                //如果当前服务器的级别是0，或比当前运行中最高级别的服务器低，则不执行任务
                return;
            }else if(scheduler.getLevel() < maxLevel){
                //如果当前服务器比最高级别还要高，则修改中间件的最高级别
                scheduler.keepAlive();
            }

            //检测任务是否可执行
            if (scheduler.check(taskId)) {
                Scheduled scheduled = method.getAnnotation(Scheduled.class);
                //获取任务下次执行的间隔时长，然后获取任务锁，如果获取成功则执行任务
                long timeoutMillis = ScheduledUtil.getNextTimeInterval(scheduled, embeddedValueResolver);
                //减500毫秒是为了解决jdk定时任务存在误差问题，防止下次任务执行时间时间还没到而跳过本次任务
                timeoutMillis -= 500;
                if (scheduler.lock(taskId, timeoutMillis)) {
                    ReflectionUtils.makeAccessible(this.method);
                    this.method.invoke(this.target);

                    if (ScheduledUtil.SCHEDULED_FIXED_DELAY.equals(ScheduledUtil.getType(scheduled))) {
                        timeoutMillis = ScheduledUtil.getNextTimeInterval(scheduled, embeddedValueResolver);
                        scheduler.relock(taskId, timeoutMillis);
                    }
                }
            }
        } catch (InvocationTargetException ex) {
            ReflectionUtils.rethrowRuntimeException(ex.getTargetException());
        } catch (IllegalAccessException ex) {
            throw new UndeclaredThrowableException(ex);
        }
    }

    public void init() {
        ScheduledCluster ds = method.getAnnotation(ScheduledCluster.class);
        if (ds != null) {
            if (StringUtils.hasText(ds.id())) {
                taskId = ds.id();
            }
            ignore = ds.ignore();
        }else{
            taskId = method.getDeclaringClass().getCanonicalName() + "." + method.getName();
            taskId = taskId.replaceAll("\\W", "_");
        }
        Scheduled scheduled = method.getAnnotation(Scheduled.class);
        String flag = ScheduledUtil.getFlag(scheduled, embeddedValueResolver);
        taskId += "_" + flag;
    }

    public Object getTarget() {
        return target;
    }

    public Method getMethod() {
        return method;
    }
}
