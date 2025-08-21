package com.projects.kafkadash.controller;

import com.projects.kafkadash.entity.AppUser;
import com.projects.kafkadash.entity.Client;
import com.projects.kafkadash.service.AdminService;
import jakarta.validation.Valid;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/admin")
@PreAuthorize("hasRole('ADMIN')")
public class AdminController {

    private final AdminService adminService;

    public AdminController(AdminService adminService) {
        this.adminService = adminService;
    }

    // Clients CRUD
    @GetMapping("/clients")
    public List<Client> allClients() { return adminService.allClients(); }

    @PostMapping("/clients")
    public Client createClient(@Valid @RequestBody Client client) { return adminService.createClient(client); }

    @PutMapping("/clients/{id}")
    public Client updateClient(@PathVariable Long id, @Valid @RequestBody Client client) {
        return adminService.updateClient(id, client);
    }

    @DeleteMapping("/clients/{id}")
    public void deleteClient(@PathVariable Long id) { adminService.deleteClient(id); }

    // Users CRUD
    @GetMapping("/users")
    public List<AppUser> allUsers() { return adminService.allUsers(); }

    @PostMapping("/users")
    public AppUser createUser(@Valid @RequestBody AppUser appUser) {
        return adminService.createUser(appUser);
    }

    @PutMapping("/users/{id}")
    public AppUser updateUser(@PathVariable Long id, @Valid @RequestBody AppUser appUser) {
        return adminService.updateUser(id, appUser);
    }

    @DeleteMapping("/users/{id}")
    public void deleteUser(@PathVariable Long id) { adminService.deleteUser(id); }
}
