// import 제거: StreamListener는 존재하지 않음
package pompom.redisstream.consumer

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.stream.StreamMessageListenerContainer
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.stereotype.Component


@Component
class MyStreamListener(
    redisTemplate: StringRedisTemplate,
    streamMessageListenerContainer: StreamMessageListenerContainer<String, MapRecord<String, String, String>>
) : BaseRedisStreamListener(redisTemplate, streamMessageListenerContainer) {

    private val log = org.slf4j.LoggerFactory.getLogger(MyStreamListener::class.java)

    override val streamKey = "mystream"
    override val group = "mygroup"
    override val consumerName = "consumer1"

    override fun handleMessage(message: MapRecord<String, String, String>) {
        log.info("Handling message: ${message.value}")
        // 메시지 처리 로직 추가
    }
}