package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;

import java.util.Set;

public interface RateLimitService {

    boolean shouldLimit(Set<RequestDescriptor> requestDescriptors);
}
