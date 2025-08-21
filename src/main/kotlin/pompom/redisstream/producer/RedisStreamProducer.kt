package pompom.redisstream.producer

import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.stereotype.Component

@Component
class RedisStreamProducer(
    private val redisTemplate: StringRedisTemplate 
) {

    val log = org.slf4j.LoggerFactory.getLogger(RedisStreamProducer::class.java)

    fun produce(streamKey: String, values: Map<String, String>): String? {
        val record = MapRecord.create(streamKey, values)

        log.info("Producing message to stream '$streamKey': $values")
        return redisTemplate.opsForStream<String, String>().add(record)?.toString()
    }
}