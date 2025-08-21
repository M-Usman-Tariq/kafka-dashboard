package com.projects.kafkadash.repository;

import com.projects.kafkadash.entity.Client;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ClientRepository extends JpaRepository<Client, Long> {
}
