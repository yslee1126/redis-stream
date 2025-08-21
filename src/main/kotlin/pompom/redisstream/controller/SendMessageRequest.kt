import org.springframework.data.redis.connection.stream.MapRecord

data class SendMessageRequest(
    val message: String
)
