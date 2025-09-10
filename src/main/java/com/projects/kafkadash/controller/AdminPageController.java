// src/main/java/com/projects/kafkadash/controller/AdminPageController.java
package com.projects.kafkadash.controller;

import com.projects.kafkadash.entity.AppUser;
import com.projects.kafkadash.entity.Client;
import com.projects.kafkadash.service.AdminService;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
@RequestMapping("/admin")
@PreAuthorize("hasRole('ADMIN')")
public class AdminPageController {

    private final AdminService adminService;

    public AdminPageController(AdminService adminService) {
        this.adminService = adminService;
    }

    // Admin dashboard: two independent pagers (users, clients)
    @GetMapping
    public String adminHome(@RequestParam(name = "upage", defaultValue = "0") int upage,
                            @RequestParam(name = "cpage", defaultValue = "0") int cpage,
                            Model model) {
        Page<AppUser> users = adminService.pageUsers(PageRequest.of(Math.max(upage, 0), 10));
        Page<Client> clients = adminService.pageClients(PageRequest.of(Math.max(cpage, 0), 10));
        model.addAttribute("users", users.getContent());
        model.addAttribute("usersPage", users);
        model.addAttribute("clients", clients.getContent());
        model.addAttribute("clientsPage", clients);
        return "admin";
    }

    // ---------- Users ----------
    // --- show add form ---
    @GetMapping("/users/new")
    public String newUserForm(Model model) {
        if (!model.containsAttribute("user")) {
            model.addAttribute("user", new AppUser());
        }
        model.addAttribute("isEdit", false);
        return "user-form";
    }

    // --- create ---
    @PostMapping("/users")
    public String createUser(@Valid @ModelAttribute("user") AppUser user,
                             BindingResult binding,
                             RedirectAttributes ra) {

        if (binding.hasErrors()) {
            ra.addFlashAttribute("org.springframework.validation.BindingResult.user", binding);
            ra.addFlashAttribute("user", user);
            return "redirect:/admin/users/new";
        }

        try {
            adminService.createUser(user);
            ra.addFlashAttribute("success", "User created.");
            return "redirect:/admin";
        } catch (IllegalArgumentException ex) {
            // duplicate username or other service-level validation
            binding.rejectValue("username", "error.user", ex.getMessage());
            ra.addFlashAttribute("org.springframework.validation.BindingResult.user", binding);
            ra.addFlashAttribute("user", user);
            return "redirect:/admin/users/new";
        }
    }

    // --- show edit form (same template), clear password so it's empty in the form ---
    @GetMapping("/users/{id}/edit")
    public String editUserForm(@PathVariable("id") Long id, Model model, RedirectAttributes ra) {
        try {
            AppUser existing = adminService.getUserById(id);
            // Clear password so hash is not rendered and browsers won't autofill it.
            existing.setPassword("");
            model.addAttribute("user", existing);
            model.addAttribute("isEdit", true);
            return "user-form";
        } catch (IllegalArgumentException ex) {
            ra.addFlashAttribute("error", ex.getMessage());
            return "redirect:/admin";
        }
    }

    // --- update ---
    @PostMapping("/users/{id}")
    public String updateUser(@PathVariable("id") Long id,
                             @Valid @ModelAttribute("user") AppUser user,
                             BindingResult binding,
                             RedirectAttributes ra) {

        if (binding.hasErrors()) {
            ra.addFlashAttribute("org.springframework.validation.BindingResult.user", binding);
            ra.addFlashAttribute("user", user);
            return "redirect:/admin/users/" + id + "/edit";
        }

        try {
            // enforce username not changed: copy stored username from DB
            AppUser existing = adminService.getUserById(id);
            user.setUsername(existing.getUsername());

            adminService.updateUser(id, user);
            ra.addFlashAttribute("success", "User updated.");
            return "redirect:/admin";
        } catch (IllegalArgumentException ex) {
            // unlikely for username (since we enforce it), but may be other validation
            binding.rejectValue("username", "error.user", ex.getMessage());
            ra.addFlashAttribute("org.springframework.validation.BindingResult.user", binding);
            ra.addFlashAttribute("user", user);
            return "redirect:/admin/users/" + id + "/edit";
        }
    }
    @PostMapping("/users/delete/{id}")
    public String deleteUser(@PathVariable("id") Long id, RedirectAttributes ra) {
        adminService.deleteUser(id);
        ra.addFlashAttribute("success", "User deleted.");
        return "redirect:/admin";
    }

    @PostMapping("/users/{id}/reset-password")
    public String resetUserPassword(@PathVariable("id") Long id, RedirectAttributes ra) {
        try {
            adminService.resetPassword(id);
            ra.addFlashAttribute("success", "Password reset to default for user id " + id);
        } catch (Exception e) {
            ra.addFlashAttribute("error", "Could not reset password: " + e.getMessage());
        }
        return "redirect:/admin";
    }

    // ---------- Clients ----------
    @GetMapping("/clients/new")
    public String newClient(Model model) {
        if (!model.containsAttribute("client")) {
            model.addAttribute("client", new Client());
        }
        model.addAttribute("isEdit", false);
        return "client-form";
    }

    @PostMapping("/clients")
    public String createClient(@Valid @ModelAttribute("client") Client client,
                               BindingResult result,
                               RedirectAttributes ra) {
        if (result.hasErrors()) {
            ra.addFlashAttribute("org.springframework.validation.BindingResult.client", result);
            ra.addFlashAttribute("client", client);
            return "redirect:/admin/clients/new";
        }
        try {
            adminService.createClient(client);
            ra.addFlashAttribute("success", "Client created.");
            return "redirect:/admin";
        } catch (IllegalArgumentException e) {

            result.rejectValue("clientId", "error.client", e.getMessage());
            ra.addFlashAttribute("org.springframework.validation.BindingResult.client", result);
            ra.addFlashAttribute("client", client);
            return "redirect:/admin/clients/new";
        }
    }

    @GetMapping("/clients/{id}/edit")
    public String editClient(@PathVariable("id") Long id, Model model, RedirectAttributes ra) {
        try {
            if (!model.containsAttribute("client")) {
                model.addAttribute("client", adminService.getClientById(id));
            }
            model.addAttribute("isEdit", true);
            return "client-form";
        } catch (IllegalArgumentException e) {
            ra.addFlashAttribute("error", e.getMessage());
            return "redirect:/admin";
        }
    }

    @PostMapping("/clients/{id}")
    public String updateClient(@PathVariable("id") Long id,
                               @Valid @ModelAttribute("client") Client client,
                               BindingResult result,
                               RedirectAttributes ra) {
        if (result.hasErrors()) {
            ra.addFlashAttribute("org.springframework.validation.BindingResult.client", result);
            ra.addFlashAttribute("client", client);
            return "redirect:/admin/clients/" + id + "/edit";
        }
        try {
            adminService.updateClient(id, client);
            ra.addFlashAttribute("success", "Client updated.");
            return "redirect:/admin";
        } catch (IllegalArgumentException e) {
            result.rejectValue("clientId", "duplicate", e.getMessage());
            ra.addFlashAttribute("org.springframework.validation.BindingResult.client", result);
            ra.addFlashAttribute("client", client);
            return "redirect:/admin/clients/" + id + "/edit";
        }
    }

    @PostMapping("/clients/delete/{id}")
    public String deleteClient(@PathVariable("id") Long id, RedirectAttributes ra) {
        adminService.deleteClient(id);
        ra.addFlashAttribute("success", "Client deleted.");
        return "redirect:/admin";
    }
}
