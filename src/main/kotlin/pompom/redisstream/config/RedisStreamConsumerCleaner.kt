package pompom.redisstream.config

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.stream.StreamMessageListenerContainer
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.stereotype.Component
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.domain.Range
import pompom.redisstream.consumer.BaseRedisStreamListener

@Component
class RedisStreamConsumerCleaner(
    private val redisTemplate: StringRedisTemplate,
    private val streamLiseners: List<BaseRedisStreamListener>,
    private val streamMessageListenerContainer: StreamMessageListenerContainer<String, MapRecord<String, String, String>>
) {

    private val log = org.slf4j.LoggerFactory.getLogger(RedisStreamConsumerCleaner::class.java)

    private val CONSUMER_ALIVE_TIME : Long = 180_000

    // 스케줄 간격은 30초정도 그리고 서버 시작후 30초 정도 후에 시작 펜딩 메세지의 처리 시간이 30초 미만이라고 생각한다 그러나 테스트를 위해 3초후 시작
    @Scheduled(fixedDelay = 30000, initialDelay = 3000)
    fun cleanUp() {
        // 리스너 목록을 돌면서 각 리스너의 스트림이름과 그룹 이름을 가져온다
        streamLiseners.forEach { listener ->
            val streamName = listener.streamKey
            val groupName = listener.group
            log.info("Stream Name: $streamName, Group Name: $groupName")

            // 그룹에 해당하는 컨슈머 목록을 가져온다 컨슈머의 idle time 과 pending 메세지 수를 가져온다
            val opsForStream = redisTemplate.opsForStream<String, String>()
            val consumers = opsForStream.consumers(streamName, groupName)
            log.info("Found ${consumers.size()} consumers in group '$groupName' for stream '$streamName'")
            
            consumers.forEach { consumer ->
                val consumerName = consumer.consumerName()
                val idleTimeMs = consumer.idleTimeMs()
                val pendingMessageCount = consumer.pendingCount()
                log.info("Consumer: ${consumerName}, Idle Time: $idleTimeMs, Pending Messages: $pendingMessageCount")

                // idle time 이 3분 이상인 컨슈머는 죽었다고 판단하고 해당 컨슈머의 펜딩 메세지가 있다면 해당 그룹의 다른 컨슈머에게 재처리를 시도한다 펜딩 메세지의 idle time 도 30초 이상이라고 생각해야한다
                if (idleTimeMs > CONSUMER_ALIVE_TIME) {
                    log.warn("Consumer '$consumerName' is considered dead (idle time: $idleTimeMs ms)")
                    if (pendingMessageCount > 0) {
                        log.info("Reprocessing pending messages for consumer '$consumerName'")
                        // 해당 컨슈머의 모든 펜딩 메시지를 가져옴
                        val pendingMessages = opsForStream.pending(
                            streamName,
                            Consumer.from(groupName, consumerName),
                            Range.unbounded<RecordId>(),
                            pendingMessageCount
                        )

                        // 현재 소비자를 제외하고, idle 시간이 3분(180,000ms) 이내인 첫 번째 활성 소비자 선택
                        val otherConsumer = consumers.firstOrNull { it.consumerName() != consumerName && it.idleTimeMs() <= CONSUMER_ALIVE_TIME }
                        if (otherConsumer != null) {
                            log.info("Reprocessing pending messages for consumer '${otherConsumer.consumerName()}'")
                            pendingMessages.forEach { message ->
                                val claimedMessages = opsForStream.claim(
                                    streamName,
                                    groupName,
                                    otherConsumer.consumerName(),
                                    message.elapsedTimeSinceLastDelivery,
                                    message.id
                                )

                                // 메세지 동시 처리 문제 
                                claimedMessages.forEach { claimMessage ->
                                    log.info("Claimed message ${claimMessage.id} for consumer '${otherConsumer.consumerName()}'")
                                    listener.onMessage(claimMessage, otherConsumer.consumerName())
                                }
                            }
                        } else {
                            log.warn("No active consumers (idle time <= 180,000ms) available to reprocess messages for '$consumerName'")
                        }

                    } 

                    // 펜딩 메세지도 처리했으니 컨슈머를 지우자
                    opsForStream.deleteConsumer(streamName, Consumer.from(consumer.groupName(), consumer.consumerName()))
                }

            }
        }

    }
}