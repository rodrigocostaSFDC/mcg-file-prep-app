package com.salesforce.mcg.preprocessor.common;

import org.apache.logging.log4j.util.Strings;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class AppConstants {

    public static final String COMPANY_TELMEX = "telmex";
    public static final String COMPANY_TELNOR = "telnor";

    public static final String JDBC_CONN_STRING = "jdbc:postgresql://%s:%s/%s?reWriteBatchedInserts=true%s";

    public static final String SSLMODE_REQUIRED = "sslmode=require";

    public static final String STRICT_HOST_KEY_CHECKING = "StrictHostKeyChecking";
    public static final String STRICT_NO = "no";
    public static final String STRICT_YES = "yes";

    public static final String SFTP_MODE = "sftp.mode";
    public static final String MODE_REMOTE = "remote";
    public static final String MODE_LOCAL = "local";

    public static final String PREFERRED_AUTHENTICATIONS = "PreferredAuthentications";
    public static final String REMOTE_PREFERRED_AUTH = "publickey,password,keyboard-interactive";
    public static final String LOCAL_PREFERRED_AUTH = "password";

    public static final Charset FILE_CHARSET_UTF8 = StandardCharsets.UTF_8;
    public static final String FILENAME_UNSUPPORTED_CHARS_REGEX = "[^a-zA-Z0-9_-]";

    public static final String FILE_EXTENSION_ZIP = ".zip";
    public static final String FILE_EXTENSION_TXT = ".txt";

    public static final char CHAR_PIPE = '|';
    public static final char CHAR_UNDERSCORE = '_';
    public static final char CHAR_DOT = '.';
    public static final char CHAR_COMMA = ',';
    public static final char CHAR_FORWARD_SLASH = '/';
    public static final char CHAR_BACKSLASH = '\\';
    public static final char CR = '\r';

    public static final String EMPTY_STRING = Strings.EMPTY;

    public static final String FILENAME_DATETIME_FORMAT_PATTERN = "yyyyMMdd_HHmm";
    public static final String FILENAME_ERRORS_SUFFIX = ".hasErrors";

    public static final List<String> URL_HEADERS = List.of("URL", "URL2");
    public static final List<String> SUBSCRIBER_KEY_HEADERS = List.of(
            "SUBSCRIBER_KEY",
            "MOBILE_USER_ID");

    public static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    public static final int ZERO = 0;

    public static final String SMS = "S";
    public static final String EMAIL = "E";
    public static final String WHATSAPP = "W";

    public static final String CHANNEL_SFTP = "sftp";
}
