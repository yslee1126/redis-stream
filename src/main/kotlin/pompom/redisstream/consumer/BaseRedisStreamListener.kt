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
    
    // 단일 컨슈머 이름 대신, 여러 컨슈머를 생성하기 위한 설정을 추가합니다.
    abstract val consumerNamePrefix: String
    open val consumerCount: Int = 1 // 기본값은 1로 설정합니다.

    abstract fun handleMessage(message: MapRecord<String, String, String>)

    @PostConstruct
    fun startListener() {
        try {
            // 그룹이 없는 경우 생성합니다. 스트림이 없으면 예외가 발생할 수 있습니다.
            // 프로듀서가 먼저 메시지를 보내 스트림을 생성하는 것을 권장합니다.
            redisTemplate.opsForStream<String, String>().createGroup(streamKey, ReadOffset.from("0-0"), group)
        } catch (e: Exception) {
            log.warn("Group '$group' already exists for stream '$streamKey'. This is expected if the application has been run before.")
        }

        // `consumerCount` 만큼 컨슈머를 생성하고 등록합니다.
        for (i in 1..consumerCount) {
            val consumerName = "$consumerNamePrefix-$i"
            streamMessageListenerContainer.receive(
                Consumer.from(group, consumerName),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed())
            ) { message: MapRecord<String, String, String> ->
                val logPrefix = "[${this::class.simpleName}][$consumerName]"
                log.info("$logPrefix Consumed: ${message.value}")

                try {
                    handleMessage(message)

                    redisTemplate.opsForStream<String, String>()
                        .acknowledge(streamKey, group, message.id)
                    redisTemplate.opsForStream<String, String>()
                        .delete(streamKey, message.id)

                    log.info("$logPrefix Ack & Delete: ${message.id}")
                } catch (ex: Exception) {
                    log.error("$logPrefix Failed to process message ${message.id}", ex)
                    // 여기에 실패한 메시지를 별도로 처리하는 로직(e.g., Dead Letter Queue)을 추가할 수 있습니다.
                }
            }
            log.info("Registered consumer '$consumerName' for stream '$streamKey' in group '$group'.")
        }
    }
    
}