package com.projects.kafkadash.controller;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class PageController {

    @GetMapping({"/", "/dashboard"})
    public String dashboard() {
        return "dashboard";
    }

//    @GetMapping("/admin")
//    @PreAuthorize("hasRole('ADMIN')")
//    public String admin() {
//        return "admin";
//    }

    @GetMapping("/login")
    public String login() {
        return "login";
    }
}
