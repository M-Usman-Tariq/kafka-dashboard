// src/main/java/com/projects/kafkadash/service/AdminService.java
package com.projects.kafkadash.service;

import com.projects.kafkadash.entity.AppUser;
import com.projects.kafkadash.entity.Client;
import com.projects.kafkadash.repository.AppUserRepository;
import com.projects.kafkadash.repository.ClientRepository;
import jakarta.transaction.Transactional;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;

@Service
@Validated
public class AdminService {
    private final ClientRepository clientRepo;
    private final AppUserRepository userRepo;
    private final PasswordEncoder encoder;

    public AdminService(ClientRepository clientRepo, AppUserRepository userRepo, PasswordEncoder encoder) {
        this.clientRepo = clientRepo;
        this.userRepo = userRepo;
        this.encoder = encoder;
    }

    // ---------- Clients ----------
    public Page<Client> pageClients(Pageable pageable) {
        return clientRepo.findAll(pageable);
    }

    public Client createClient(@Valid Client client) {
        if (clientRepo.existsByClientId(client.getClientId())) {
            throw new IllegalArgumentException("Client ID already exists.");
        }
        return clientRepo.save(client);
    }

    public Client updateClient(Long id, @Valid Client client) {
        Client existing = getClientById(id);
        // if clientId changed, enforce uniqueness
        if (!existing.getClientId().equals(client.getClientId())
                && clientRepo.existsByClientId(client.getClientId())) {
            throw new IllegalArgumentException("Client ID already exists.");
        }
        client.setId(id);
        return clientRepo.save(client);
    }

    public void deleteClient(Long id) {
        clientRepo.deleteById(id);
    }

    public Client getClientById(Long id) {
        return clientRepo.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Client not found: " + id));
    }

    // ---------- Users ----------
    public Page<AppUser> pageUsers(Pageable pageable) {
        return userRepo.findAll(pageable);
    }

    public AppUser createUser(@Valid AppUser appUser) {
        if (userRepo.existsByUsername(appUser.getUsername())) {
            throw new IllegalArgumentException("Username already exists.");
        }
        if (appUser.getPassword() == null || appUser.getPassword().isBlank()) {
            throw new IllegalArgumentException("Password is required.");
        }
        appUser.setPassword(encoder.encode(appUser.getPassword()));
        return userRepo.save(appUser);
    }

    public AppUser updateUser(Long id, @Valid AppUser appUser) {
        AppUser existing = getUserById(id);
        // if username changed, enforce uniqueness
        if (!existing.getUsername().equals(appUser.getUsername())
                && userRepo.existsByUsername(appUser.getUsername())) {
            throw new IllegalArgumentException("Username already exists.");
        }
        // keep existing password if blank
        if (StringUtils.hasText(appUser.getPassword())) {
            appUser.setPassword(encoder.encode(appUser.getPassword()));
        } else {
            appUser.setPassword(existing.getPassword());
        }
        appUser.setId(id);
        return userRepo.save(appUser);
    }

    public void deleteUser(Long id) { userRepo.deleteById(id); }

    public AppUser getUserById(Long id) {
        return userRepo.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("User not found: " + id));
    }

    @Transactional
    public void resetPassword(Long id) {
        AppUser user = userRepo.findById(id)
                .orElseThrow(() -> new RuntimeException("User not found"));
        String defaultPwd = "changeme";
        user.setPassword(encoder.encode(defaultPwd));
        userRepo.save(user);
    }

}
