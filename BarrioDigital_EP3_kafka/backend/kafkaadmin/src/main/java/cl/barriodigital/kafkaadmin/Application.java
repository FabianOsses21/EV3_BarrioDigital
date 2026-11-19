package cl.barriodigital.kafkaadmin;

import cl.barriodigital.common.PlatformConfig;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.*;

@SpringBootApplication
@Import(PlatformConfig.class)
public class Application {
    public static void main(String[] args) { SpringApplication.run(Application.class, args); }

    @Bean(destroyMethod="close")
    Admin admin(@Value("${spring.kafka.bootstrap-servers}") String servers) {
        return Admin.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 4000,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 8000));
    }
}
