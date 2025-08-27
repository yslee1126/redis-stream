// import 제거: StreamListener는 존재하지 않음
package pompom.redisstream.consumer

import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.stream.StreamMessageListenerContainer
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.stereotype.Component
import org.springframework.scheduling.annotation.Scheduled
import java.time.Duration


@Component
class MyStreamListener(
    redisTemplate: StringRedisTemplate,
    streamMessageListenerContainer: StreamMessageListenerContainer<String, MapRecord<String, String, String>>
) : BaseRedisStreamListener(redisTemplate, streamMessageListenerContainer) {

    private val log = org.slf4j.LoggerFactory.getLogger(MyStreamListener::class.java)

    @Value("\${redis.stream.mystream.key}")
    override val streamKey: String = "mystream"

    @Value("\${redis.stream.mystream.group}")
    override val group: String = "mygroup"
    
    // consumerName 대신 prefix와 count를 사용합니다.
    override val consumerNamePrefix = "consumer"

    // application.yml에서 값을 주입받아 컨슈머 수를 동적으로 설정합니다.
    @Value("\${redis.stream.mystream.consumer.count:4}") // yml에 값이 없으면 기본값 4를 사용
    override val consumerCount: Int = 4

    @Value("\${redis.stream.mystream.scheduler.min-idle-time:1m}")
    private lateinit var minIdleTime: Duration

    @Scheduled(fixedRateString = "\${redis.stream.mystream.scheduler.fixed-rate:60000}", initialDelayString = "10000")
    fun schedulePendingMessageReprocessing() {
        super.reprocessPendingMessages(minIdleTime)
    }

    override fun handleMessage(message: MapRecord<String, String, String>) {
        log.info("Handling message for 'mystream': ${message.value}")
        // 메시지 처리 로직 추가
        // Thread.sleep(1000) // 처리 시간 시뮬레이션
    }
}