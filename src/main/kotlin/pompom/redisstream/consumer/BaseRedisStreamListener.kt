package pompom.redisstream.consumer

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.StreamOperations
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.stream.StreamMessageListenerContainer
import org.springframework.data.redis.stream.Subscription
import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Range
import java.time.Duration

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

    // 각 컨슈머에 대한 구독(Subscription)을 저장하여 나중에 취소할 수 있도록 합니다.
    private val subscriptions = mutableListOf<Subscription>()


    abstract fun handleMessage(message: MapRecord<String, String, String>)

    @PostConstruct
    fun startListener() {
        try {
            // 스트림이 존재하지 않을 경우 `createGroup`이 실패할 수 있습니다.
            // 이를 방지하기 위해, 스트림이 없으면 먼저 생성합니다.
            // 가장 간단한 방법은 더미 데이터를 추가하는 것입니다.
            if (redisTemplate.hasKey(streamKey) == false) {
                redisTemplate.opsForStream<String, String>().add(streamKey, mapOf("init" to "true"))
                // 스트림을 생성하기 위해 추가한 더미 데이터는 즉시 제거합니다.
                redisTemplate.opsForStream<String, String>().trim(streamKey, 0)
                log.info("Stream '$streamKey' did not exist. Created a new stream.")
            }

            // 이제 스트림이 존재하므로 그룹을 생성합니다.
            redisTemplate.opsForStream<String, String>().createGroup(streamKey, ReadOffset.from("0-0"), group)
            log.info("Successfully created group '$group' for stream '$streamKey'.")
        } catch (e: org.springframework.data.redis.RedisSystemException) {
            // 그룹이 이미 존재하면 "BUSYGROUP" 오류가 발생합니다. 이는 정상적인 상황이므로 무시합니다.
            if (e.message?.contains("BUSYGROUP") == true) {
                log.info("Group '$group' already exists for stream '$streamKey'. This is an expected condition.")
            } else {
                // 다른 Redis 오류는 심각한 문제일 수 있으므로 애플리케이션 시작을 중단합니다.
                throw IllegalStateException("Failed to initialize Redis Stream group '$group' on stream '$streamKey'", e)
            }
        }

        // `consumerCount` 만큼 컨슈머를 생성하고 등록합니다.
        for (i in 1..consumerCount) {
            val consumerName = "$consumerNamePrefix-$i"
            val subscription = streamMessageListenerContainer.receive(
                Consumer.from(group, consumerName),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed())
            ) { message: MapRecord<String, String, String> ->
                // 메시지 처리 로직을 공통 메서드로 추출하여 재사용
                onMessage(message, consumerName)
            }
            subscriptions.add(subscription)
            log.info("Registered consumer '$consumerName' for stream '$streamKey' in group '$group'.")
        }
    }

    /**
     * Redis 그룹에서 컨슈머를 제거합니다.
     * 이 메서드는 애플리케이션 종료 시 RedisStreamShutdownHandler에 의해 호출됩니다.
     */
    internal fun deleteConsumersFromGroup() {
        log.info("Deleting consumers for stream '$streamKey' and group '$group'.")
        // StreamMessageListenerContainer의 destroy-method='stop'이 리스닝 작업 중지를 처리합니다.
        // 여기서는 컨슈머 그룹에서 컨슈머를 명시적으로 제거합니다.
        for (i in 1..consumerCount) {
            val consumerName = "$consumerNamePrefix-$i"
            try {
                // 컨슈머 그룹에서 컨슈머를 제거합니다.
                // deleteConsumer는 성공 여부를 Boolean으로 반환합니다 (펜딩 메시지 수가 아님).
                val wasRemoved = redisTemplate.opsForStream<String, String>().deleteConsumer(streamKey, Consumer.from(group, consumerName))
                if (wasRemoved == true) {
                    log.info("Successfully removed consumer '$consumerName' from group '$group' on stream '$streamKey'.")
                } else {
                    log.warn("Could not remove consumer '$consumerName' from group '$group' on stream '$streamKey'. It might have already been removed.")
                }
            } catch (e: Exception) {
                log.error("Error removing consumer '$consumerName' from group '$group' on stream '$streamKey'.", e)
            }
        }
    }

    /**
     * 등록된 모든 컨슈머의 구독을 취소합니다.
     * 이렇게 하면 StreamMessageListenerContainer가 더 이상 이 리스너를 위해 폴링하지 않습니다.
     */
    internal fun unsubscribeListeners() {
        subscriptions.forEach { subscription ->
            streamMessageListenerContainer.remove(subscription)
        }
        subscriptions.clear()
        log.info("Unsubscribed all consumers for stream '$streamKey'.")
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