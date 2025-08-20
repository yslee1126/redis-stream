package pompom.redisstream

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class RedisStreamApplication

fun main(args: Array<String>) {
	runApplication<RedisStreamApplication>(*args)
}
