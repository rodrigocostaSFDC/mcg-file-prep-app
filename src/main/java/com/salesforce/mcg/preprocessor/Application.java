package com.salesforce.mcg.preprocessor;

import com.salesforce.mcg.preprocessor.common.SalesforceBanner;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import picocli.CommandLine;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;

/**
 * Entry point for the mcg-file-preprocessor-app one-off dyno.
 *
 * <p>Parses CLI arguments with picocli, validates using Jakarta Validation,
 * and only then starts Spring Boot.
 */
@SpringBootApplication
@Getter
@Slf4j
public class Application {

    /**
     * Company identifier.
     */
    @NotBlank(message = "--company is required")
    @Pattern(
            regexp = "^(?i)(telmex|telnor)$",
            message = "--company must be telmex or telnor"
    )
    @CommandLine.Option(
            names = "--company",
            required = true,
            description = "Allowed values: telmex, telnor"
    )
    private String company;

    /**
     * File name.
     */
    @NotBlank(message = "--file is required")
    @Pattern(
            regexp = "^.+_S_.+\\.(txt|zip)$",
            message = "--file must match *_S_*.txt or *_S_*.zip"
    )
    @CommandLine.Option(
            names = "--file",
            required = true,
            description = "Format: *_S_*.txt or *_S_*.zip"
    )
    private String file;

    public static void main(String[] args) {

        var appInstance = new Application();

        // 1️⃣ Parse CLI arguments (picocli)
        var cmd = new CommandLine(appInstance);
        try {
            cmd.parseArgs(args);
        } catch (Exception e){
            log.error("❌ {}", e.getMessage());
            System.exit(1);
        }

        // 2️⃣ Validate (Jakarta Validation)
        validate(appInstance, cmd);

        // 3️⃣ Normalize values
        appInstance.company = appInstance.company.toLowerCase();
        appInstance.file = appInstance.file.trim();

        // 4️⃣ Start Spring only after validation
        var app = new SpringApplication(Application.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        app.setBanner(new SalesforceBanner());
        app.setBannerMode(Banner.Mode.CONSOLE);

        app.setDefaultProperties(Map.of(
                "app.company", appInstance.company,
                "app.file", appInstance.file,
                "app.transactionDate", DateTimeFormatter
                        .ofPattern("dd/MM/yyyy HH:mm:ss")
                        .format(LocalDateTime.now().atZone(ZoneOffset.UTC)))//TODO check business rule!!
        );

        System.exit(SpringApplication.exit(app.run(args)));
    }

    /**
     * Validates CLI arguments using Jakarta Validation.
     */
    private static void validate(Application app, CommandLine cmd) {
        try (ValidatorFactory factory = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory()) {

            Validator validator = factory.getValidator();
            Set<ConstraintViolation<Application>> violations = validator.validate(app);

            if (!violations.isEmpty()) {
                StringBuilder message = new StringBuilder("❌ Invalid program arguments:");

                for (ConstraintViolation<Application> v : violations) {
                    message.append(System.lineSeparator())
                            .append(" - ")
                            .append(v.getMessage());
                }
                log.error(message.toString());
                System.exit(1);
            }
        }
    }
}