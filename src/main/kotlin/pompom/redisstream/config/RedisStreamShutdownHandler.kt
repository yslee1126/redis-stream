package pompom.redisstream.config

import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationListener
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.context.event.ContextClosedEvent
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.data.redis.stream.StreamMessageListenerContainer
import org.springframework.stereotype.Component
import pompom.redisstream.consumer.BaseRedisStreamListener

@Component
class RedisStreamShutdownHandler(
    // Spring이 애플리케이션에 등록된 모든 BaseRedisStreamListener 빈을 주입해줍니다.
    private val streamListeners: List<BaseRedisStreamListener>,
    // 리스너 컨테이너를 주입받아 종료 시점에 직접 제어합니다.
    private val streamMessageListenerContainer: StreamMessageListenerContainer<String, MapRecord<String, String, String>>
) : ApplicationListener<ContextClosedEvent> {

    private val log = LoggerFactory.getLogger(RedisStreamShutdownHandler::class.java)

    /**
     * Application context가 종료될 때 호출됩니다.
     * 등록된 모든 BaseRedisStreamListener의 stopListener()를 호출하여
     * Redis 컨슈머를 안전하게 정리합니다.
     *
     * @param event 컨텍스트 종료 이벤트
     */
    override fun onApplicationEvent(event: ContextClosedEvent) {
        log.info("Context is closing. Graceful shutdown initiated for Redis Stream components.")

        // 1. 모든 리스너의 구독을 취소하여 폴링 작업을 먼저 중지시킵니다.
        log.info("Unsubscribing all stream listeners...")
        streamListeners.forEach { listener ->
            listener.unsubscribeListeners()
        }

        // 2. 리스너 컨테이너를 중지합니다. 이제 폴링 작업이 없으므로 안전하게 중지됩니다.
        log.info("Stopping StreamMessageListenerContainer...")
        streamMessageListenerContainer.stop()
        log.info("StreamMessageListenerContainer stopped.")

        // 3. 컨테이너가 중지된 후, 각 리스너가 등록한 컨슈머를 Redis 그룹에서 제거합니다.
        streamListeners.forEach { listener ->
            try {
                listener.deleteConsumersFromGroup()
            } catch (e: Exception) {
                log.error("Error while stopping listener for stream '${listener.streamKey}'", e)
            }
        }
        log.info("All Redis stream consumers have been removed from their groups. Graceful shutdown complete.")
    }
}