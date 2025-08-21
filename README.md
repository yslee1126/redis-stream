# Redis Stream 예제

Spring Boot와 Redis Stream을 사용한 메시지 스트리밍 예제 프로젝트입니다.

## 실행 방법

### 애플리케이션 실행
```bash
./gradlew bootRun
```
애플리케이션이 시작되면 내장된 Redis 서버가 자동으로 시작되고, 8080 포트에서 서비스가 실행됩니다.

## 테스트 방법

### 메시지 전송 테스트
다음 curl 명령어로 메시지를 전송할 수 있습니다:

```bash
curl -X POST http://localhost:8080/api/streams/send \
     -H "Content-Type: application/json" \
     -d '{"message":"hello world"}'
```

메시지가 성공적으로 전송되면 콘솔에서 Consumer가 메시지를 수신하는 로그를 확인할 수 있습니다.