package com.salesforce.mcg.preprocessor.helper;

import com.jcraft.jsch.ChannelSftp;
import reactor.util.annotation.NonNull;

import java.io.InputStream;
import java.util.regex.Pattern;

public class SftpClientHelper {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Wraps an {@link InputStream} so that closing it also disconnects the owning
     * {@link ChannelSftp}.  This lets callers use standard try-with-resources without
     * needing to hold a reference to the channel.
     */
    public static final class ChannelClosingInputStream extends InputStream {

        private final InputStream delegate;
        private final ChannelSftp channel;
        private boolean closed = false;

        public ChannelClosingInputStream(InputStream delegate, ChannelSftp channel) {
            this.delegate = delegate;
            this.channel = channel;
        }

        @Override public int read() throws java.io.IOException { return delegate.read(); }
        @Override public int read(@NonNull byte[] b, int off, int len) throws java.io.IOException { return delegate.read(b, off, len); }
        @Override public int available() throws java.io.IOException { return delegate.available(); }

        @Override
        public void close() throws java.io.IOException {
            if (!closed) {
                closed = true;
                try { delegate.close(); } finally { channel.disconnect(); }
            }
        }
    }

    /**
     * Converts a simple glob pattern ({@code *} and {@code ?} wildcards only) to a
     * {@link Pattern} suitable for filename matching.
     */
    private static Pattern toRegex(String glob) {
        StringBuilder sb = new StringBuilder("^");
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*' -> sb.append(".*");
                case '?' -> sb.append(".");
                case '.', '(', ')', '+', '^', '$', '{', '}', '[', ']', '|', '\\' ->
                        sb.append('\\').append(c);
                default -> sb.append(c);
            }
        }
        sb.append("$");
        return Pattern.compile(sb.toString(), Pattern.CASE_INSENSITIVE);
    }
}
