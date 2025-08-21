package com.projects.kafkadash.service;



import com.projects.kafkadash.entity.AppUser;
import com.projects.kafkadash.entity.Client;
import com.projects.kafkadash.repository.AppUserRepository;
import com.projects.kafkadash.repository.ClientRepository;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AdminService {
    private final ClientRepository clientRepo;
    private final AppUserRepository userRepo;
    private final PasswordEncoder encoder;

    public AdminService(ClientRepository clientRepo, AppUserRepository userRepo, PasswordEncoder encoder) {
        this.clientRepo = clientRepo;
        this.userRepo = userRepo;
        this.encoder = encoder;
    }

    public List<Client> allClients() { return clientRepo.findAll(); }

    public Client createClient(Client client) { return clientRepo.save(client); }

    public Client updateClient(Long id, Client client) {
        client.setId(id);
        return clientRepo.save(client);
    }

    public void deleteClient(Long id) { clientRepo.deleteById(id); }

    public List<AppUser> allUsers() { return userRepo.findAll(); }

    public AppUser createUser(AppUser appUser) {
        appUser.setPassword(encoder.encode(appUser.getPassword()));
        return userRepo.save(appUser);
    }

    public AppUser updateUser(Long id, AppUser appUser) {
        appUser.setId(id);
        if (appUser.getPassword() != null && !appUser.getPassword().isBlank()) {
            appUser.setPassword(encoder.encode(appUser.getPassword()));
        }
        return userRepo.save(appUser);
    }

    public void deleteUser(Long id) { userRepo.deleteById(id); }
}
