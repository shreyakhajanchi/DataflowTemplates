package com.google.cloud.teleport.v2.templates.spanner.ddl;

import com.google.cloud.spanner.Dialect;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;

/** Cloud Spanner Ddl untility components. */
class DdlUtilityComponents {

    // Private constructor to prevent initializing instance, because this class is only served within
    // ddl directory
    private DdlUtilityComponents() {}

    // Shared at package-level
    static final Escaper OPTION_STRING_ESCAPER =
            Escapers.builder()
                    .addEscape('"', "\\\"")
                    .addEscape('\\', "\\\\")
                    .addEscape('\r', "\\r")
                    .addEscape('\n', "\\n")
                    .build();
    static final String POSTGRESQL_IDENTIFIER_QUOTE = "\"";
    static final String GSQL_IDENTIFIER_QUOTE = "`";

    static String identifierQuote(Dialect dialect) {
        switch (dialect) {
            case POSTGRESQL:
                return POSTGRESQL_IDENTIFIER_QUOTE;
            case GOOGLE_STANDARD_SQL:
                return GSQL_IDENTIFIER_QUOTE;
            default:
                throw new IllegalArgumentException(String.format("Unrecognized dialect: %s", dialect));
        }
    }
}
