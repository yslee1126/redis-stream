package pompom.redisstream

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.boot.SpringApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class RedisStreamApplication
fun main(args: Array<String>) {
    SpringApplication.run(RedisStreamApplication::class.java, *args)
}