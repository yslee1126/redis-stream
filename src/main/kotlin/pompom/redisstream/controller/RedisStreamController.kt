package pompom.redisstream.controller

import org.springframework.web.bind.annotation.*
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.connection.stream.MapRecord
import SendMessageRequest
import com.fasterxml.jackson.databind.ObjectMapper
import pompom.redisstream.producer.RedisStreamProducer

@RestController
@RequestMapping("/api/streams")
class RedisStreamController(
    private val redisStreamProducer: RedisStreamProducer
) {
    @PostMapping("/send")
    fun sendMessage(@RequestBody message: SendMessageRequest) {
        
        val messageMap: Map<String, String> = mapOf(
            "message" to message.message
        )

        redisStreamProducer.produce("mystream", messageMap)
    }
}