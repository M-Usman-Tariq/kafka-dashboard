package com.projects.kafkadash.config;

import com.projects.kafkadash.entity.AppUser;
import com.projects.kafkadash.entity.Client;
import com.projects.kafkadash.repository.ClientRepository;
import com.projects.kafkadash.repository.AppUserRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.password.PasswordEncoder;

@Configuration
public class SeedData {

    @Bean
    CommandLineRunner seed(AppUserRepository users, ClientRepository clients, PasswordEncoder enc) {
        return args -> {
            if (users.count() == 0) {
                AppUser admin = new AppUser();
                admin.setUsername("admin");
                admin.setPassword(enc.encode("admin123"));
                admin.setRole(AppUser.Role.ROLE_ADMIN);
                users.save(admin);

                AppUser appUser = new AppUser();
                appUser.setUsername("user");
                appUser.setPassword(enc.encode("user123"));
                appUser.setRole(AppUser.Role.ROLE_USER);
                users.save(appUser);
            }
            if (clients.count() == 0) {
                Client c1 = new Client();
                c1.setClientId("client-1");
                c1.setClientName("Client One");
                c1.setSubscriptionName("connect-sink-1");
                c1.setContactName("contact-Name-1");
                c1.setContactEmail("contact.email@gmail.com");
                clients.save(c1);
            }
        };
    }
}
