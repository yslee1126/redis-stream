package pompom.redisstream.consumer

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.stream.StreamMessageListenerContainer
import jakarta.annotation.PostConstruct

abstract class BaseRedisStreamListener(
    private val redisTemplate: StringRedisTemplate,
    private val streamMessageListenerContainer: StreamMessageListenerContainer<String, MapRecord<String, String, String>>
) {

    private val log = org.slf4j.LoggerFactory.getLogger(BaseRedisStreamListener::class.java)

    abstract val streamKey: String
    abstract val group: String
    abstract val consumerName: String

    abstract fun handleMessage(message: MapRecord<String, String, String>)

    @PostConstruct
    fun startListener() {
        try {
            redisTemplate.opsForStream<String, String>().createGroup(streamKey, ReadOffset.from("0-0"), group)
        } catch (e: Exception) {
            log.warn("Group '$group' already exists for stream '$streamKey'")
        }

        streamMessageListenerContainer.receive(
            Consumer.from(group, consumerName),
            StreamOffset.create(streamKey, ReadOffset.lastConsumed())
        ) { message: MapRecord<String, String, String> ->
            log.info("[${this::class.simpleName}] Consumed: ${message.value}")

            try {
                handleMessage(message)

                redisTemplate.opsForStream<String, String>()
                    .acknowledge(streamKey, group, message.id)
                redisTemplate.opsForStream<String, String>()
                    .delete(streamKey, message.id)

                log.info("[${this::class.simpleName}] Ack & Delete: ${message.id}")
            } catch (ex: Exception) {
                log.error("[${this::class.simpleName}] Failed to process message ${message.id}", ex)
            }
        }
    }
    
}