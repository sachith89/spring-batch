package com.javatechie.spring.batch.repository;

import com.javatechie.spring.batch.entity.Customer;
import com.javatechie.spring.batch.entity.Email;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EmailRepository extends JpaRepository<Email,Integer> {
}
