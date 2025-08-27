package pompom.redisstream.consumer

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.StreamOperations
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.stream.StreamMessageListenerContainer
import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Range
import java.time.Duration
import java.time.Instant

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

    // 재처리 전용 컨슈머 이름
    open val schedulerConsumerName: String
        get() = "$consumerNamePrefix-scheduler"


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
                // 메시지 처리 로직을 공통 메서드로 추출하여 재사용
                onMessage(message, consumerName)
            }
            log.info("Registered consumer '$consumerName' for stream '$streamKey' in group '$group'.")
        }
    }
    
    /**
     * 메시지 수신 및 재처리 시 공통으로 사용되는 처리 로직
     */
    fun onMessage(message: MapRecord<String, String, String>, consumerName: String) {
        val logPrefix = "[${this::class.simpleName}][$consumerName]"
        log.info("$logPrefix Consumed: ${message.value}")

        try {
            handleMessage(message)

            redisTemplate.opsForStream<String, String>()
                .acknowledge(streamKey, group, message.id) // Not enough information to infer type variable HK
            redisTemplate.opsForStream<String, String>()
                .delete(streamKey, message.id)

            log.info("$logPrefix Ack & Delete: ${message.id}")
        } catch (ex: Exception) {
            log.error("$logPrefix Failed to process message ${message.id}", ex)
            // 실패한 메시지는 ACK되지 않으므로, 나중에 펜딩 메시지 스케줄러에 의해 재처리될 수 있습니다.
        }
    }

    /**
     * 이 리스너의 그룹에 속한 오래된 펜딩 메시지를 재처리합니다.
     * 이 메서드는 스케줄링되지 않았습니다. 하위 클래스에서 @Scheduled를 사용하여 호출해야 합니다.
     * @param minIdleTime 이 시간 이상 pending 상태인 메시지만 재처리 대상이 됩니다.
     */
    fun reprocessPendingMessages(minIdleTime: Duration) {
        val logPrefix = "[${this::class.simpleName}][Scheduler]"
        log.info("$logPrefix Running pending message check for group '$group' on stream '$streamKey'.")

        // opsForStream 을 명시적으로 String 타입으로 맞춤
        val streamOps: StreamOperations<String, String, String> = redisTemplate.opsForStream()

        // 한 번에 최대 100개씩 조회
        val pendingMessages = streamOps.pending(streamKey, group, Range.unbounded<RecordId>(), 100L)

        if (pendingMessages.isEmpty) {
            return
        }

        pendingMessages.forEach { pendingMessage ->
            val idleTime = pendingMessage.elapsedTimeSinceLastDelivery
            if (idleTime.toMillis() > minIdleTime.toMillis()) { // idleTime은 밀리초 단위
                log.warn(
                    "$logPrefix Reclaiming stale message [ID: ${pendingMessage.idAsString}], " +
                        "pending for ${idleTime.toMillis() / 1000}s by consumer [${pendingMessage.consumerName}]"
                )

                val claimedMessages: List<MapRecord<String, String, String>> =
                    streamOps.claim(streamKey, group, schedulerConsumerName, idleTime, pendingMessage.id)

                    claimedMessages.forEach { message ->
                        onMessage(message, schedulerConsumerName)
                    }
                }
            }
        }
}