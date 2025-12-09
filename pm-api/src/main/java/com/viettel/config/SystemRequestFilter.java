package com.viettel.config;

import java.io.IOException;

import com.viettel.service.SystemValidationService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;


@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@Slf4j
public class SystemRequestFilter extends OncePerRequestFilter {

    private static final String HEADER_SYSTEM = "X-System";
    private static final String HEADER_NE_TYPE = "X-NE-Type";
    @Deprecated // not use for now
    private static final String HEADER_DATASOURCE = "X-DS"; // optional logical datasource key
    private final ConfigManager configManager;
    private final SystemValidationService systemValidationService;

    public SystemRequestFilter(ConfigManager configManager, SystemValidationService systemValidationService) {
        this.configManager = configManager;
        this.systemValidationService = systemValidationService;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    @NonNull HttpServletResponse response,
                                    @NonNull FilterChain filterChain)
            throws ServletException, IOException {
        try {
            String systemHeader = request.getHeader(HEADER_SYSTEM);
            String neTypeHeader = request.getHeader(HEADER_NE_TYPE);
            SystemType system = SystemType.fromCode(systemHeader);

            boolean deployed = configManager.isDeployed(system);
            log.info("Original Client Request Header " + HEADER_SYSTEM + ": {}, " + HEADER_NE_TYPE + ": {}", systemHeader,
                    neTypeHeader);
            if (system == null || neTypeHeader == null || !deployed) {
                log.warn("Client request with invalid system, ne type header or the designated system type is not deployed");
                response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                        "Either the system header is missing or invalid," +
                                " or the designated system type is not operational.");
                return;
            }

            systemHeader = systemHeader.toUpperCase();
            neTypeHeader = neTypeHeader.toUpperCase();

            boolean isValidHeader = systemValidationService.isValid(systemHeader, neTypeHeader);
            if (!isValidHeader) {
                log.warn("Client request with invalid system header: {} or ne type header: {}", systemHeader, neTypeHeader);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                        "Either the system. ne type header is missing or invalid");
                return;
            }

            TenantContextHolder.setCurrentSystem(system);
            String dsHeader = request.getHeader(HEADER_DATASOURCE);
            String resolvedDs = configManager.resolveDatasourceKey(system, dsHeader);
            TenantContextHolder.setCurrentDatasourceKey(resolvedDs);

            log.info("Request context set: System={}, DataSourceKey={}", system.getCode(), resolvedDs);


            filterChain.doFilter(request, response);
        } finally {
            TenantContextHolder.clear();
        }
    }
}
