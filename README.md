# spring-scheduling-cluster
Spring Boot 自带定时器Scheduled增加分布式/集群环境下任务调度控制插件，其原理是对任务加锁实现控制，支持能实现分布锁的中间件。
下面提供了redis缓存和mysql数据库的实现，如果使用到其它中间件暂时麻烦请自行实现，下面会讲如果实现的。

## 环境要求和关键技术
- [JDK1.7](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
- [Spring Boot 1.5.7.RELEASE](https://docs.spring.io/spring-boot/docs/1.5.7.RELEASE/reference/html/)

## 添加插件到项目中并启用

### 添加插件
1. 方式一：下载本项目源码然后加添加到项目中。
2. 方式二：下载本项目的[jar包](https://gitee.com/lnkToKing/spring-scheduling-cluster/attach_files)，添加到项目的libs包库中。
3. 方式三：下载本项目的[jar包](https://gitee.com/lnkToKing/spring-scheduling-cluster/attach_files)，添加到本地maven库中，然后在pom.xml文件添加引用

``` xml
<dependency>
    <groupId>pres.lnk.springframework</groupId>
    <artifactId>spring-scheduling-cluster</artifactId>
    <version>1.0-BATE</version>
</dependency>
```

### 启用插件
在 Spring 配置中添加以下代码将插件注册成Bean（注意，原来用来启动定时器的注解@EnableScheduling还是要添加的）

``` java
@Bean(name = TaskManagementConfigUtils.SCHEDULED_ANNOTATION_PROCESSOR_BEAN_NAME)
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public ScheduledAnnotationBeanPostProcessor scheduledAnnotationProcessor() {
    return new ScheduledClusterAnnotationBeanPostProcessor();
}
```

## 实现调度器中间件对定时任务进行锁操作

### 使用 redis 缓存做中间件
将下面类的代码添加到项目中，并注册成SpringBean限可

``` java
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * redis调度器中间件
 *
 * @Author lnk
 * @Date 2018/2/28
 */
@Component
public class RedisSchedulerImpl extends AbstractScheduler {
    private static final String CACHE_PREFIX = "scheduler_";
    private static final String MAX_LEVEL = "maxLevel";

    @Autowired
    private RedisTemplate redisTemplate;

    @Override
    public boolean check(String id) {
        Long time = getCache(id);
        return time == null || currentTimeMillis() > time;
    }

    @Override
    public boolean lock(String id, long timeoutMillis) {
        String key = prefixKey(id);
        long nextTimeMillis = currentTimeMillis() + timeoutMillis;
        boolean flag = redisTemplate.opsForValue().setIfAbsent(key, nextTimeMillis);
        if(flag){
            redisTemplate.expire(key, timeoutMillis < 0 ? 1 : timeoutMillis, TimeUnit.MILLISECONDS);
        }
        return flag;
    }

    @Override
    public void relock(String id, long timeoutMillis) {
        String key = prefixKey(id);
        redisTemplate.expire(key, timeoutMillis < 0 ? 1 : timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public long currentTimeMillis() {
        RedisConnection connection = redisTemplate.getConnectionFactory().getConnection();
        long time = connection.time();
        connection.close();
        return time;
    }

    /*
     *  ------------- 下面3个重写方法可选 -------------
     *  如果启用了主从服务器则 keepAlive() 和 getMaxAliveLevel() 必须重写
     */

    @Override
    public void keepAlive() {
        int level = getLevel();
        String key = prefixKey(MAX_LEVEL);
        Integer maxLevel = getCache(MAX_LEVEL);
        //如果maxLevel为null，就将当前level写进去
        if (maxLevel == null) {
            //控制只能写一个，防止并发低级别把高级别覆盖了
            boolean result = redisTemplate.opsForValue().setIfAbsent(key, level);
            if (result) {
                //写入成功，添加过期时间，过期时间追加5秒，避免出现误差，在下次刷新时间前就失效了，被低级别的服务器抢先执行任务
                redisTemplate.expire(key, getHeartTime() + 5, TimeUnit.SECONDS);
                return;
            } else {
                //写入失败，则获取最高级别跟当前级别做比较，避免低级别抢先写入成功
                maxLevel = getCache(key);
            }
        }

        //如果当前级别比缓存里级别还要高，则覆盖它
        if (maxLevel > level) {
            redisTemplate.delete(key);
            keepAlive();
            return;
        }

        //如果当前级别跟最高级别同级，则刷新过期时间
        if (maxLevel == level) {
            Long expire = redisTemplate.getExpire(prefixKey(MAX_LEVEL));
            //只有过期时间有还有10秒的时候才刷新时间，避免多个服务器同时刷新
            if (expire < 10) {
                //加5秒理由同上
                redisTemplate.expire(key, getHeartTime() + 5, TimeUnit.SECONDS);
            }
        }

    }

    @Override
    public int getMaxAliveLevel() {
        Integer level = getCache(MAX_LEVEL);
        if (level != null) {
            return level;
        }
        //如果没有最高级别则返回当前服务器级别
        return getLevel();
    }

    /**
     * 任务执行结束后调用的方法，可以写日志，不需要做后续处理可不重写
     */
    @Override
    public void executed(Method method, Object targer, long startTimeMillis, long endTimeMillis, String description) throws Exception {
        //可写任务执行日志
        switch (getStatus()){
            case AbstractScheduler.SUCCESS :
                // 执行成功
            case AbstractScheduler.FAIL_CHECK :
                // 未执行任务，已有服务器执行过
            case AbstractScheduler.FAIL_LOCK :
                // 未执行任务，获取锁失败
            case AbstractScheduler.FAIL_LEVEL :
                // 未执行任务，级别低
            case AbstractScheduler.ERROR :
                // 执行任务出现异常，如果不想处理可以抛回由Spring处理
                throw getException();
        }
    }

    public <T> T getCache(String key) {
        key = prefixKey(key);
        return (T)redisTemplate.opsForValue().get(key);
    }

    private static String prefixKey(String key) {
        return CACHE_PREFIX.concat(key.replaceAll("\\W+", "_"));
    }
}
```

### 使用 mysql数据库 做中间件
由于每个项目使用的持久化框架不一样，下面只提供了Mybatis的实现做参考，请根据自己项目框架做改动

``` java
import com.gzkit.crm.mcs.mapper.TimedTaskMapper;
import com.gzkit.crm.mcs.utils.CacheUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import pres.lnk.springframework.AbstractScheduler;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * mysql调度器中间件
 * 由于mysql 5.6 版本之后才支持查询毫秒，为了兼容低版本mysql，所以将毫秒转成秒再处理业务
 *
 * @Author lnk
 * @Date 2018/2/28
 */
@Component
public class MysqlSchedulerImpl extends AbstractScheduler {
    private static final String MAX_LEVEL = "maxLevel";

    @Autowired
    private TimedTaskMapper timedTaskMapper;

    @Override
    public boolean check(String id) {
        int count = timedTaskMapper.check(id);
        return count == 0;
    }

    @Override
    public boolean lock(String id, long timeoutMillis) {
        long timeout = timeoutMillis / 1000;
        int result = timedTaskMapper.hasId(id);
        if(result > 0){
            //id已有锁，则尝试重新获取锁
            try {
                result = timedTaskMapper.lock(id, timeout);
            } catch (Exception e){ }
        }else{
            //id还没锁，就直接插入锁
            try {
                result = timedTaskMapper.insert(id, timeout, null);
            } catch (Exception e){ }
        }
        return result == 1;
    }

    @Override
    public void relock(String id, long timeoutMillis) {
        long timeout = timeoutMillis / 1000;
        timedTaskMapper.update(id, timeout, null);
    }

    @Override
    public long currentTimeMillis() {
        return timedTaskMapper.time() * 1000;
    }

    /*
     *  ------------- 下面3个重写方法可选 -------------
     *  如果启用了主从服务器则 keepAlive() 和 getMaxAliveLevel() 必须重写
     */
     
    @Override
    public void keepAlive() {
        int timeout = getHeartTime() + 5;

        //获取当前最高level
        String value = timedTaskMapper.getMaxLevel(MAX_LEVEL);
        Integer result = null;
        if(NumberUtils.isDigits(value)){
            result = Integer.parseInt(value);
        }

        if(result == null){
            //如果数据库没有保存level，则保存当前服务器的级别到数据库
            try {
                timedTaskMapper.insert(MAX_LEVEL, timeout, getLevel() + "");
            } catch (Exception e){
                timedTaskMapper.updateLevel(MAX_LEVEL, getLevel() + "", timeout);
            }
        }else{
            //数据库已有level
            if(result == getLevel()){
                //如果数据库level与当前服务器相同，则尝试刷新当前级别服务器的时间
                try {
                    timedTaskMapper.updateLevelTime(MAX_LEVEL, getLevel() + "", timeout);
                } catch (Exception e){ }
            }else if(result > getLevel()){
                //如果当前服务器级别比数据库的高，则刷新数据库级别
                try {
                    timedTaskMapper.updateLevel(MAX_LEVEL, getLevel() + "", timeout);
                } catch (Exception e){ }
            }

        }
    }

    @Override
    public int getMaxAliveLevel() {
        String value = timedTaskMapper.getMaxLevel(MAX_LEVEL);
        if(NumberUtils.isDigits(value)){
            return Integer.parseInt(value);
        }
        return getLevel();
    }

    /**
     * 任务执行结束后调用的方法，可以写日志，不需要做后续处理可不重写
     */
    @Override
    public void executed(Method method, Object targer, long startTimeMillis, long endTimeMillis, String description) throws Exception {
        //可写任务执行日志
        switch (getStatus()){
            case AbstractScheduler.SUCCESS :
                // 执行成功
            case AbstractScheduler.FAIL_CHECK :
                // 未执行任务，已有服务器执行过
            case AbstractScheduler.FAIL_LOCK :
                // 未执行任务，获取锁失败
            case AbstractScheduler.FAIL_LEVEL :
                // 未执行任务，级别低
            case AbstractScheduler.ERROR :
                // 执行任务出现异常，如果不想处理可以抛回由Spring处理
                throw getException();
        }
    }
}

```

``` java
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * @Author lnk
 * @Date 2018/3/6
 */
@Mapper
public interface TimedTaskMapper {
    /**
     * select count(*) from t_timed_task
     * where id = #{id, jdbcType=VARCHAR} and timeout >= UNIX_TIMESTAMP(now())
     *
     * @param id
     * @return
     */
    int check(String id);

    /**
     * select count(*) from t_timed_task where id = #{id, jdbcType=VARCHAR}
     *
     * @param id
     * @return
     */
    int hasId(String id);

    /**
     * update t_timed_task set timeout = UNIX_TIMESTAMP(now()) + #{timeout, jdbcType=BIGINT}
     * where id = #{id, jdbcType=VARCHAR} and timeout &lt; UNIX_TIMESTAMP(now())
     *
     * @param id
     * @param timeout
     * @return
     */
    int lock(@Param("id") String id, @Param("timeout") long timeout);

    /**
     * insert into t_timed_task values (
     * #{id, jdbcType=VARCHAR},
     * UNIX_TIMESTAMP(now()) + #{timeout, jdbcType=BIGINT},
     * #{value, jdbcType=VARCHAR}
     * )
     *
     * @param id
     * @param timeout
     * @param value
     * @return
     */
    int insert(@Param("id") String id, @Param("timeout") long timeout, @Param("value") String value);

    /**
     * update t_timed_task set
     * timeout = UNIX_TIMESTAMP(now()) + #{timeout, jdbcType=BIGINT},
     * value = #{value, jdbcType=VARCHAR}
     * where id = #{id, jdbcType=VARCHAR}
     *
     * @param id
     * @param timeout
     * @param value
     * @return
     */
    int update(@Param("id") String id, @Param("timeout") long timeout, @Param("value") String value);

    /**
     * select UNIX_TIMESTAMP(now())
     *
     * @return
     */
    long time();

    /**
     * update t_timed_task set value = #{value, jdbcType=VARCHAR}, timeout = UNIX_TIMESTAMP(now()) + #{timeout, jdbcType=BIGINT}
     * where id = #{id, jdbcType=VARCHAR} and (value &gt; #{value, jdbcType=VARCHAR} or timeout &lt; UNIX_TIMESTAMP(now()))
     *
     * @param id
     * @return
     */
    int updateLevel(@Param("id") String id, @Param("value") String value, @Param("timeout") long timeout);

    /**
     * update t_timed_task set timeout = UNIX_TIMESTAMP(now()) + #{timeout, jdbcType=BIGINT}
     * where id = #{id, jdbcType=VARCHAR} and value = #{value, jdbcType=VARCHAR}
     *
     * @param id
     * @param value
     * @param timeout
     * @return
     */
    int updateLevelTime(@Param("id") String id, @Param("value") String value, @Param("timeout") long timeout);

    /**
     * select value from t_timed_task where id = #{id, jdbcType=VARCHAR} and timeout &gt; UNIX_TIMESTAMP(now())
     *
     * @param id
     * @return
     */
    String getMaxLevel(String id);
}

```

数据库表结构   
t_timed_task

字段 | 类型 | 说明
 --- | --- | --- 
id | varchar(255) PRIMARY NOT NULL | 任务id
timeout | bigint NOT NULL | 锁的失效时间
value | varchar(255) DEFAULT NULL | 锁对应的值

``` sql
DROP TABLE IF EXISTS `t_timed_task`;

CREATE TABLE `t_timed_task` (
  `id` varchar(255) NOT NULL,
  `timeout` bigint(20) NOT NULL,
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

### 自定义中间件
自定义类继承抽象类`AbstractScheduler`，实现父类的方法并注册成Spring Bean。   
`AbstractScheduler` 方法说明

 方法 | 必须 | 说明
  --- | --- | --- 
 boolean check(String id) | 是 | 检查任务id是否没被锁，如果没被锁则表示可以执行任务，下一步就获取锁 
 boolean lock(String id, long timeoutMillis) | 是 | 对任务id加锁，并在下次执行任务前释放锁，返回加锁是否成功 
 void relock(String id, long timeoutMillis) | 是 | 修改任务id锁的释放时间 
 long currentTimeMillis() | 是 |  获取中间件的服务器时间，做为锁的参考时间。集群所有服务器最好做时间同步，避免cron任务出现误差 
 void keepAlive() | 如果启用优先级功能则必须重写 | 将服务器最高优先级别保存到中间件
 int getMaxAliveLevel() | 如果启用优先级功能则必须重写 | 获取中间件保存的最高级别 
 void executed(Method method, Object targer, long startTimeMillis, long endTimeMillis, String description) | 否 | 定时任务执行结束的后续处理 


## 设置主从服务器
主从服务器是通过设置服务器优先级实现，实现原理是优先级高的服务器定时（心跳时间）告诉中间件我还活着（运行中），
然后优先级低的服务器则不执行任务。如果优先级高级服务器挂了，中间件不再接收到信息了，优先级低的服务器就会接替继续工作。

#### 设置方式
在spring的yml配置文件中添加下面配置
``` yaml
spring:
  scheduling:
    cluster:
      level: 1      #优先级别
      heartTime: 60 #心跳时间（秒）
```

配置说明：   
level ： 优先级别，1 等级最高，数字越大等级越低。其中 0 是该服务器不执行定时任务。 -1 是不参与集群服务调度，不受优先级影响，任务每次都会执行。   
heartTime ： 心跳时间，服务器会以这个时间频率告诉中间件我还活着   

重写中间件的getLevel()方法，可自定义level规则，例如根据ip判断level，这样不用每个服务器单独改配置文件

## 注解@ScheduledCluster
该注解用在`@Scheduled`的方法上，有以下属性

属性 | 必填 | 说明
--- | --- | ---
id | 否 | 自定义任务id
description | 否 | 任务描述
ignore | 否 | 是否忽略集群控制，作用跟level=-1一样，但只针对该任务

``` java
@ScheduledCluster(id="updateData", description = "每小时更新一次数据")
@Scheduled(cron = "0 0 0/1 * * ?")
public void update(){
    // 更新数据
}
```
