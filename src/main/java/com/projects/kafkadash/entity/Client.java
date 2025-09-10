package com.projects.kafkadash.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CurrentTimestamp;

import java.time.Instant;

@Entity
@Table(name = "client")
@Getter
@Setter
public class Client {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    @NotBlank
    private String clientId;

    @NotBlank
    @Column(nullable = false)
    private String clientName;

    @NotBlank
    @Column(nullable = false)
    @Pattern(regexp = "^[^\\s]+$", message = "Subscription Name cannot contain spaces")
    private String subscriptionName;

    @NotBlank
    @Column(nullable = false)
    private String contactName;

    @NotBlank
    @Column(nullable = false)
    @Email(message = "Invalid email format")
    private String contactEmail;

    @Column(nullable = false)
    @CurrentTimestamp
    private Instant insertionTimestamp;


}
