package kafka;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

//windows连接 redis-cli.exe -h 127.0.0.1 -p 6379
//单例模式
public class RedisPoolUtil {
    public static org.apache.log4j.Logger logger = Logger.getLogger(RedisPoolUtil.class);

    private static RedisPoolUtil instance = null;

    private static JedisPool jedisPool = null;

    //构造方法private，外界无法new,只能加载一次
    private RedisPoolUtil() throws IOException {
        //加载配置文件
        Properties props = new Properties();
        //获取读取流
        InputStream stream = RedisPoolUtil.class.getClassLoader().getResourceAsStream("application.properties");

        //从配置文件中读取数据
        props.load(stream);

        //关闭流
        stream.close();

        //获取配置
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(Integer.valueOf(props.getProperty("jedis.pool.maxActive")));
        config.setMaxIdle(Integer.valueOf(props.getProperty("jedis.pool.maxIdle")));
        config.setMaxWaitMillis(Long.valueOf(props.getProperty("jedis.pool.maxWait")));
        config.setTestOnBorrow(Boolean.valueOf(props.getProperty("jedis.pool.testOnBorrow")));
        config.setTestOnReturn(Boolean.valueOf(props.getProperty("jedis.pool.testOnReturn")));
        // 根据配置实例化jedis池
        jedisPool = new JedisPool(config, props.getProperty("redis.ip"),
                Integer.valueOf(props.getProperty("redis.port")),
                Integer.valueOf(props.getProperty("redis.timeout")));
        System.out.println("redis连接池被成功初始化");
    }

    //这个会在每个线程中保有一份自己的变量，不会线程共享
    //private static ThreadLocal<Jedis>local;

    public static RedisPoolUtil getInstance() throws IOException {
        if (instance == null) {
            synchronized (RedisPoolUtil.class){
                if (instance == null) {
                    instance = new RedisPoolUtil();
                }
            }
        }
        return instance;
    }

    public static synchronized Jedis getRdis() {
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

    public static void closeRedis(Jedis jedis){
        if (jedis!=null) {
            jedis.close();
        }
    }
}
