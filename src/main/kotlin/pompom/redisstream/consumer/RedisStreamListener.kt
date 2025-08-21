// import 제거: StreamListener는 존재하지 않음
package pompom.redisstream.consumer

import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.stream.StreamMessageListenerContainer
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.stereotype.Component
import jakarta.annotation.PostConstruct


@Component
class RedisStreamListener(
    private val redisTemplate: StringRedisTemplate 
) {
    private val streamKey = "mystream"
    private val group = "mygroup"
    private val consumerName = "consumer1"

    private val log = org.slf4j.LoggerFactory.getLogger(RedisStreamListener::class.java)

    @PostConstruct
    fun startListener() {
        try {
            redisTemplate.opsForStream<String, String>()
                .createGroup(streamKey, ReadOffset.latest(), group)
        } catch (e: Exception) {
            log.warn("Group '$group' already exists for stream '$streamKey'")
        }

        val connectionFactory = redisTemplate.connectionFactory!!
        val container = StreamMessageListenerContainer.create(connectionFactory)

        container.receive(
            Consumer.from(group, consumerName),
            StreamOffset.create(streamKey, ReadOffset.lastConsumed())
        ) { message: MapRecord<String, String, String> ->
            log.info("Consumed message: ${message.value}")

            redisTemplate.opsForStream<String, String>()
                .acknowledge(streamKey, group, message.id)
            log.info("Acknowledged message: ${message.id}")

            redisTemplate.opsForStream<String, String>()
                .delete(streamKey, message.id)  
            log.info("Deleted message: ${message.id}")    
        }
        container.start()
    }
}