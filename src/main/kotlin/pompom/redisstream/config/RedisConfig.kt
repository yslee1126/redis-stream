package pompom.redisstream.config

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.stream.StreamMessageListenerContainer
import redis.embedded.RedisServer
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import java.util.concurrent.ExecutorService
import java.time.Duration
import java.util.concurrent.Executors

@Configuration
class RedisConfig {
    private val log = LoggerFactory.getLogger(RedisConfig::class.java)
    
    private var redisServer: RedisServer? = null

    @PostConstruct
    fun startRedis() {
        try {
            // 실제 redis 클러스터에 연결할때는 lettuce 설정이 필요합니다.
            redisServer = RedisServer(6379)
            redisServer?.start()
            log.info("✅ Embedded Redis started on port 6379")
        } catch (e: Exception) {
            log.error("Failed to start Redis", e)
            throw e
        }
    }

    @PreDestroy
    fun stopRedis() {
        try {
            redisServer?.stop()
            log.info("🛑 Embedded Redis stopped")
        } catch (e: Exception) {
            log.warn("Error stopping Redis", e)
        }
    }

    @Bean(destroyMethod = "shutdown")
    fun streamListenerExecutor(): ExecutorService {
        // 스레드 풀을 Spring이 관리하는 Bean으로 등록합니다.
        // destroyMethod = "shutdown"을 통해 애플리케이션 종료 시 안전하게 스레드 풀이 종료되도록 보장합니다.
        return Executors.newFixedThreadPool(4)
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    fun streamMessageListenerContainer(
        connectionFactory: RedisConnectionFactory,
        streamListenerExecutor: ExecutorService // Spring이 관리하는 스레드 풀 Bean을 주입받습니다.
    ): StreamMessageListenerContainer<String, MapRecord<String, String, String>> {
        val options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions
            .builder()
            .pollTimeout(Duration.ofMillis(500))
            .batchSize(1) // 필요에 따라 배치 크기 조절
            .executor(streamListenerExecutor) // 주입받은 스레드 풀을 사용합니다.
            .errorHandler { e -> log.error("Redis Stream Listener Error", e) }
            .build()

        log.info("✅ StreamMessageListenerContainer created")
        return StreamMessageListenerContainer.create(connectionFactory, options)
    }
}
