package com.javatechie.spring.batch.config;

import com.javatechie.spring.batch.entity.Email;
import org.springframework.batch.item.ItemProcessor;

public class EmailProcessor implements ItemProcessor<Email,Email> {

    @Override
    public Email process(Email email) throws Exception {
        // if(customer.getCountry().equals("United States")) {
        return email;
        //  }else{
        //      return null;
        //  }
    }
}