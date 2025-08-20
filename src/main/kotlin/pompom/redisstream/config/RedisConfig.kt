package pompom.redisstream.config

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Configuration
import redis.embedded.RedisServer
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy

@Configuration
class RedisConfig {
    private val log = LoggerFactory.getLogger(RedisConfig::class.java)
    
    private var redisServer: RedisServer? = null

    @PostConstruct
    fun startRedis() {
        try {

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
}
